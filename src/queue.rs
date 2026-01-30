use chrono::{Utc, TimeZone};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use tokio::time::{sleep, interval};
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, watch, Mutex, Semaphore};
use tokio::task::JoinHandle;
use futures::future::join_all;
use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};
use tokio::process::Command;
use fred::pool::RedisPool;
use fred::prelude::{RedisConfig, ReconnectPolicy};
use fred::interfaces::{HashesInterface, KeysInterface, SortedSetsInterface, ListInterface, ClientLike, SetsInterface, PubsubInterface};
use fred::interfaces::ClientInterface;
use fred::types::{CustomCommand, ClusterHash, RedisValue};
use uuid::Uuid;
use serde_json::Value;
use cron::Schedule;
use chrono_tz::Tz;
use md5::{Digest, Md5};
use std::str::FromStr;

use crate::commands::Commands;
use crate::job::{Job, JobHandler, JobRecord, JobOpts, RepeatOpts, DEFAULT_JOB_NAME, BackoffStrategy, MoveToFailedResult};
use std::fmt;
use std::path::Path;
use std::process::Stdio;
use base64::{engine::general_purpose, Engine as _};

#[derive(Clone)]
pub struct QueueSettings {
    pub lock_duration: u64,
    pub lock_renew_time: u64,
    pub stalled_interval: u64,
    pub max_stalled_count: u32,
    pub guard_interval: u64,
    pub retry_process_delay: u64,
    pub drain_delay: u64,
    pub child_pool_size: usize,
    pub backoff_strategies: HashMap<String, BackoffStrategy>,
}

#[derive(Debug, Clone)]
pub struct MetricsOptions {
    pub max_data_points: u32,
}

impl fmt::Debug for QueueSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueSettings")
            .field("lock_duration", &self.lock_duration)
            .field("lock_renew_time", &self.lock_renew_time)
            .field("stalled_interval", &self.stalled_interval)
            .field("max_stalled_count", &self.max_stalled_count)
            .field("guard_interval", &self.guard_interval)
            .field("retry_process_delay", &self.retry_process_delay)
            .field("drain_delay", &self.drain_delay)
            .field("child_pool_size", &self.child_pool_size)
            .field("backoff_strategies", &self.backoff_strategies.len())
            .finish()
    }
}

impl Default for QueueSettings {
    fn default() -> Self {
        QueueSettings {
            lock_duration: 30000,
            lock_renew_time: 15000,
            stalled_interval: 30000,
            max_stalled_count: 1,
            guard_interval: 5000,
            retry_process_delay: 5000,
            drain_delay: 5,
            child_pool_size: 1,
            backoff_strategies: HashMap::new(),
        }
    }
}

impl QueueSettings {
    pub fn backoff_strategies(&self) -> &HashMap<String, BackoffStrategy> {
        &self.backoff_strategies
    }
}

#[derive(Debug, Clone)]
pub struct RateLimiter {
    pub max: u32,
    pub duration: u64,
    pub bounce_back: bool,
    pub group_key: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct QueueOptions {
    pub prefix: Option<String>,
    pub limiter: Option<RateLimiter>,
    pub default_job_options: Option<crate::job::JobOpts>,
    pub settings: Option<QueueSettings>,
    pub metrics: Option<MetricsOptions>,
}

#[derive(Debug, Clone)]
pub enum QueueEvent {
    Paused,
    Resumed,
    Drained,
    Active(String),
    Completed(String, String),
    Failed(String, String),
    Removed(String),
    GlobalActive(String),
    GlobalWaiting(String),
    GlobalStalled(String),
    GlobalCompleted(String, String),
    GlobalFailed(String, String),
    GlobalProgress(String, String),
    GlobalPaused,
    GlobalResumed,
    GlobalDrained,
    GlobalDelayed(i64),
    Error(String),
}

#[derive(Debug, Clone)]
pub struct RepeatableJob {
    pub key: String,
    pub name: String,
    pub id: Option<String>,
    pub end_date: Option<i64>,
    pub tz: Option<String>,
    pub cron: Option<String>,
    pub every: Option<i64>,
    pub next: i64,
}

#[derive(Debug, Clone)]
pub struct JobLogs {
    pub logs: Vec<String>,
    pub count: i64,
}

#[derive(Debug, Clone)]
pub struct QueueMetrics {
    pub meta: MetricsMeta,
    pub data: Vec<String>,
    pub count: i64,
}

#[derive(Debug, Clone)]
pub struct MetricsMeta {
    pub count: i64,
    pub prev_ts: i64,
    pub prev_count: i64,
}

#[derive(Clone)]
enum Processor {
    Inline(JobHandler),
    Sandboxed(Arc<SandboxedProcessor>),
}

struct SandboxedProcessor {
    path: String,
    pool: Arc<ChildPool>,
}

#[allow(dead_code)]
pub struct Queue {
    prefix: String,
    queue_name: String,
    queue_prefix: String,
    joiner: String,
    pub token: String,
    handlers: RwLock<HashMap<String, Processor>>,
    event_tx: broadcast::Sender<QueueEvent>,
    commands: Commands,
    redis_client: RedisPool,
    pubsub_client: RedisPool,
    global_event_handle: RwLock<Option<GlobalEventsHandle>>,
    child_pool: RwLock<Option<Arc<ChildPool>>>,
    pub settings: QueueSettings,
    limiter: Option<RateLimiter>,
    default_job_options: Option<crate::job::JobOpts>,
    pub(crate) metrics: Option<MetricsOptions>,
    paused: AtomicBool,
    drained: AtomicBool,
    closing: AtomicBool,
}

struct GlobalEventsHandle {
    shutdown_tx: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

struct ChildPool {
    size: usize,
    available: Mutex<Vec<ChildProcess>>,
    semaphore: Semaphore,
}

struct ChildProcess {
    child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    stdout: BufReader<tokio::process::ChildStdout>,
}

pub struct QueueHandle {
    queue: Arc<Queue>,
    shutdown_tx: watch::Sender<bool>,
    handles: Vec<JoinHandle<()>>,
}

impl QueueHandle {
    pub async fn close(self) -> Result<(), Box<dyn std::error::Error>> {
        self.queue.closing.store(true, Ordering::SeqCst);
        let _ = self.shutdown_tx.send(true);
        for handle in self.handles {
            handle.await.map_err(|err| err.to_string())?;
        }
        self.queue.stop_global_events().await;
        self.queue.stop_child_pool().await;
        Ok(())
    }

    pub async fn stop(self, timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
        self.queue.closing.store(true, Ordering::SeqCst);
        let _ = self.shutdown_tx.send(true);
        let join_future = join_all(self.handles);
        match tokio::time::timeout(timeout, join_future).await {
            Ok(results) => {
                for result in results {
                    result.map_err(|err| err.to_string())?;
                }
                self.queue.stop_global_events().await;
                self.queue.stop_child_pool().await;
                Ok(())
            }
            Err(_) => Err("Timed out waiting for workers to stop".into())
        }
    }
}

impl Queue {
    pub async fn new(queue_name: &str, redis_url: &str) -> Result<Arc<Queue>, Box<dyn std::error::Error>> {
        Queue::new_with_opts(queue_name, redis_url, QueueOptions::default()).await
    }

    pub async fn new_with_opts(queue_name: &str, redis_url: &str, opts: QueueOptions) -> Result<Arc<Queue>, Box<dyn std::error::Error>> {
        let config = RedisConfig::from_url(redis_url)?;
        let policy = ReconnectPolicy::new_linear(0, 5000, 100);
        let redis_client = RedisPool::new(config, None, Some(policy), 5)?;
        let pubsub_client = RedisPool::new(RedisConfig::from_url(redis_url)?, None, Some(ReconnectPolicy::new_linear(0, 5000, 100)), 1)?;

        redis_client.connect();
        redis_client.wait_for_connect().await?;
        pubsub_client.connect();
        pubsub_client.wait_for_connect().await?;

        let commands = Commands::new(&redis_client).await?;
        let prefix = opts.prefix.unwrap_or_else(|| "bull".to_string());
        let joiner = ":".to_string();
        let queue_prefix = vec![prefix.as_str(), queue_name, ""].join(&joiner);

        let token = Uuid::new_v4().to_string();

        let (event_tx, _) = broadcast::channel(128);

        let queue = Arc::new(Queue {
            prefix,
            queue_name: queue_name.to_string(),
            queue_prefix,
            joiner,
            token,
            handlers: RwLock::new(HashMap::new()),
            event_tx,
            commands,
            redis_client,
            pubsub_client,
            global_event_handle: RwLock::new(None),
            child_pool: RwLock::new(None),
            settings: opts.settings.unwrap_or_default(),
            limiter: opts.limiter,
            default_job_options: opts.default_job_options,
            metrics: opts.metrics,
            paused: AtomicBool::new(false),
            drained: AtomicBool::new(true),
            closing: AtomicBool::new(false),
        });

        queue.start_global_events().await?;

        Ok(queue)
    }

    pub async fn add_job(&self, mut job: Job) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let opts = job.opts.clone().unwrap_or_else(|| {
            self.default_job_options.clone().unwrap_or_default()
        });

        if opts.repeat.is_some() {
            return Ok(self.next_repeatable_job(&job.name, &job.data, opts, true).await?.unwrap_or_default());
        }

        self.add_job_raw(&mut job, opts).await
    }

    async fn add_job_raw(&self, job: &mut Job, opts: JobOpts) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let opts_json = serde_json::to_value(&opts).unwrap_or(Value::Null);

        let custom_job_id = job_id_for_group(self.limiter.as_ref(), &opts.custom_job_id, &job.data);

        let delay_timestamp = if job.delay > 0 {
            job.timestamp + job.delay as i64
        } else {
            0
        };

        let lifo = if opts.lifo {
            "RPUSH"
        } else {
            "LPUSH"
        };

        let args = vec!(
            self.get_key(""),
            custom_job_id,
            job.name.clone(),
            job.data.clone(),
            opts_json.to_string(),
            job.timestamp.to_string(),
            job.delay.to_string(),
            delay_timestamp.to_string(),
            opts.priority.to_string(),
            lifo.to_string(),
            self.token.to_string(),
        );

        let result = self.commands.add_job(self, args).await?;
        Ok(result)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<QueueEvent> {
        self.event_tx.subscribe()
    }

    pub(crate) fn redis_client(&self) -> &RedisPool {
        &self.redis_client
    }

    pub(crate) fn commands(&self) -> &Commands {
        &self.commands
    }

    async fn child_pool(&self) -> Arc<ChildPool> {
        if let Some(pool) = self.child_pool.read().await.as_ref() {
            return Arc::clone(pool);
        }

        let mut write = self.child_pool.write().await;
        if let Some(pool) = write.as_ref() {
            return Arc::clone(pool);
        }

        let pool = Arc::new(ChildPool::new(self.settings.child_pool_size));
        *write = Some(Arc::clone(&pool));
        pool
    }

    pub(crate) fn emit_event(&self, event: QueueEvent) {
        self.emit(event);
    }

    fn emit(&self, event: QueueEvent) {
        let _ = self.event_tx.send(event);
    }

    async fn start_global_events(self: &Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        let channels = vec![
            self.to_key("paused"),
            self.to_key("resumed"),
            self.to_key("delayed"),
            self.to_key("completed"),
            self.to_key("failed"),
            self.to_key("drained"),
            self.to_key("progress"),
        ];
        let patterns = vec![
            format!("{}*", self.to_key("active")),
            format!("{}*", self.to_key("waiting")),
            format!("{}*", self.to_key("stalled")),
        ];

        let _: () = self.pubsub_client.subscribe(channels).await?;
        let _: () = self.pubsub_client.psubscribe(patterns).await?;

        let mut receiver = self.pubsub_client.on_message();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let queue = Arc::clone(self);
        let handle = tokio::spawn(async move {
            loop {
                if queue.closing.load(Ordering::SeqCst) {
                    break;
                }
                let message = tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    message = receiver.recv() => match message {
                        Ok(value) => value,
                        Err(_) => continue,
                    }
                };

                let channel = message.channel.to_string();
                let payload = redis_value_to_string(&message.value).unwrap_or_default();

                if channel.starts_with(queue.to_key("active").as_str()) {
                    queue.emit(QueueEvent::GlobalActive(payload));
                    continue;
                }
                if channel.starts_with(queue.to_key("waiting").as_str()) {
                    queue.emit(QueueEvent::GlobalWaiting(payload));
                    continue;
                }
                if channel.starts_with(queue.to_key("stalled").as_str()) {
                    queue.emit(QueueEvent::GlobalStalled(payload));
                    continue;
                }

                if channel == queue.to_key("paused") {
                    queue.emit(QueueEvent::GlobalPaused);
                    continue;
                }
                if channel == queue.to_key("resumed") {
                    queue.emit(QueueEvent::GlobalResumed);
                    continue;
                }
                if channel == queue.to_key("drained") {
                    queue.emit(QueueEvent::GlobalDrained);
                    continue;
                }
                if channel == queue.to_key("delayed") {
                    if let Ok(value) = payload.parse::<i64>() {
                        queue.emit(QueueEvent::GlobalDelayed(value));
                    }
                    if let Err(err) = queue.update_delay_timer().await {
                        queue.emit(QueueEvent::Error(err.to_string()));
                    }
                    continue;
                }
                if channel == queue.to_key("completed") {
                    if let Some((job_id, value)) = parse_event_data(&payload) {
                        queue.emit(QueueEvent::GlobalCompleted(job_id, value));
                    }
                    continue;
                }
                if channel == queue.to_key("failed") {
                    if let Some((job_id, value)) = parse_event_data(&payload) {
                        queue.emit(QueueEvent::GlobalFailed(job_id, value));
                    }
                    continue;
                }
                if channel == queue.to_key("progress") {
                    if let Some((job_id, progress)) = parse_progress_data(&payload) {
                        queue.emit(QueueEvent::GlobalProgress(job_id, progress));
                    }
                    continue;
                }
            }
        });

        let mut handle_slot = self.global_event_handle.write().await;
        *handle_slot = Some(GlobalEventsHandle {
            shutdown_tx,
            handle,
        });
        Ok(())
    }

    async fn stop_global_events(&self) {
        let mut handle_slot = self.global_event_handle.write().await;
        if let Some(handle) = handle_slot.take() {
            let _ = handle.shutdown_tx.send(true);
            let _ = handle.handle.await;
            let _ = self.pubsub_client.unsubscribe(vec![
                self.to_key("paused"),
                self.to_key("resumed"),
                self.to_key("delayed"),
                self.to_key("completed"),
                self.to_key("failed"),
                self.to_key("drained"),
                self.to_key("progress"),
            ]).await;
            let _ = self.pubsub_client.punsubscribe(vec![
                format!("{}*", self.to_key("active")),
                format!("{}*", self.to_key("waiting")),
                format!("{}*", self.to_key("stalled")),
            ]).await;
        }
    }

    async fn stop_child_pool(&self) {
        let mut pool_slot = self.child_pool.write().await;
        if let Some(pool) = pool_slot.take() {
            pool.close().await;
        }
    }

    pub async fn set_handler<F, R>(&self, name: &str, handler: F) -> Result<(), Box<dyn std::error::Error>>
        where
            F: Fn(JobRecord) -> R + Send + Sync + 'static,
            R: Future<Output = Result<String, String>> + Send + 'static,
    {
        self.set_worker_name().await?;
        let mut handlers = self.handlers.write().await;
        if handlers.contains_key(name) {
            return Err(format!("Cannot define the same handler twice {}", name).into());
        }

        let handler = move |job: JobRecord| -> Pin<Box<dyn Future<Output = Result<String, String>> + Send>> {
            Box::pin(handler(job))
        };

        handlers.insert(name.to_string(), Processor::Inline(std::sync::Arc::new(handler)));
        Ok(())
    }

    pub async fn set_handler_file(&self, name: &str, handler_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        if !Path::new(handler_path).exists() {
            return Err(format!("File {} does not exist", handler_path).into());
        }

        self.set_worker_name().await?;

        let mut handlers = self.handlers.write().await;
        if handlers.contains_key(name) {
            return Err(format!("Cannot define the same handler twice {}", name).into());
        }

        let pool = self.child_pool().await;
        let processor = SandboxedProcessor {
            path: handler_path.to_string(),
            pool,
        };
        handlers.insert(name.to_string(), Processor::Sandboxed(Arc::new(processor)));
        Ok(())
    }

    pub fn base64_name(&self) -> String {
        general_purpose::STANDARD.encode(self.queue_name.as_bytes())
    }

    pub fn client_name(&self) -> String {
        format!("{}:{}", self.prefix, self.base64_name())
    }

    pub async fn set_worker_name(&self) -> Result<(), Box<dyn std::error::Error>> {
        match self.redis_client.client_setname(self.client_name()).await {
            Ok(()) => Ok(()),
            Err(err) => {
                let message = err.to_string();
                if message.contains("unknown command") || message.contains("ERR unknown command") {
                    Ok(())
                } else {
                    Err(err.into())
                }
            }
        }
    }

    pub async fn get_workers(&self) -> Result<Vec<HashMap<String, String>>, Box<dyn std::error::Error>> {
        match self.redis_client.client_list::<String, Vec<String>>(None, None).await {
            Ok(list) => Ok(self.parse_client_list(list.as_str())),
            Err(err) => {
                let message = err.to_string();
                if message.contains("unknown command") || message.contains("ERR unknown command") {
                    Ok(Vec::new())
                } else {
                    Err(err.into())
                }
            }
        }
    }

    pub fn parse_client_list(&self, list: &str) -> Vec<HashMap<String, String>> {
        let mut clients = Vec::new();
        for line in list.split('\n') {
            if line.trim().is_empty() {
                continue;
            }
            let mut client = HashMap::new();
            for pair in line.split(' ') {
                if pair.is_empty() {
                    continue;
                }
                if let Some(idx) = pair.find('=') {
                    let key = pair[..idx].to_string();
                    let value = pair[idx + 1..].to_string();
                    client.insert(key, value);
                }
            }
            if let Some(name) = client.get("name").cloned() {
                if name.starts_with(self.client_name().as_str()) {
                    client.insert("name".to_string(), self.queue_name.clone());
                    clients.push(client);
                }
            }
        }
        clients
    }

    pub async fn add(&self, data: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let job = Job::new(data);
        self.add_job(job).await
    }

    pub async fn add_with_opts(&self, data: &str, opts: JobOpts) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut job = Job::new(data);
        job.opts = Some(opts);
        self.add_job(job).await
    }

    pub async fn add_named_with_opts(&self, name: &str, data: &str, opts: JobOpts) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut job = Job::new(data);
        job.name = name.to_string();
        job.opts = Some(opts);
        self.add_job(job).await
    }

    pub async fn add_bulk(&self, jobs: Vec<Job>) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
        let mut results = Vec::with_capacity(jobs.len());
        for job in jobs {
            results.push(self.add_job(job).await?);
        }
        Ok(results)
    }

    pub async fn remove_repeatable(&self, name: Option<&str>, repeat: RepeatOpts) -> Result<(), Box<dyn std::error::Error>> {
        let name = name.unwrap_or(DEFAULT_JOB_NAME);
        let job_id = repeat.job_id.clone().unwrap_or_default();
        let job_id_prefix = if job_id.is_empty() { ":".to_string() } else { format!("{}:", job_id) };
        let repeat_job_key = get_repeat_key(name, &repeat, &job_id_prefix);
        let repeat_job_id = get_repeat_job_id(name, &job_id_prefix, None, &md5_hex(&repeat_job_key));
        self.commands.remove_repeatable(self, &repeat_job_id, &repeat_job_key).await
    }

    pub async fn remove_repeatable_by_key(&self, repeat_job_key: &str) -> Result<(), Box<dyn std::error::Error>> {
        let repeat_meta = key_to_data(repeat_job_key);
        let job_id_prefix = match repeat_meta.id.as_deref() {
            Some(id) if !id.is_empty() => format!("{}:", id),
            _ => ":".to_string(),
        };
        let repeat_job_id = get_repeat_job_id(
            repeat_meta.name.as_str(),
            &job_id_prefix,
            None,
            &md5_hex(repeat_job_key)
        );
        self.commands.remove_repeatable(self, &repeat_job_id, repeat_job_key).await
    }

    pub async fn get_repeatable_jobs(&self, start: i64, end: i64, asc: bool) -> Result<Vec<RepeatableJob>, Box<dyn std::error::Error>> {
        let key = self.to_key("repeat");
        let keys: Vec<String> = if asc {
            self.redis_client.zrange(key, start, end, None, false, None, false).await?
        } else {
            self.redis_client.zrevrange(key, start, end, false).await?
        };

        let mut jobs = Vec::with_capacity(keys.len());
        for repeat_key in keys {
            let score: Option<f64> = self.redis_client.zscore(self.to_key("repeat"), repeat_key.as_str()).await?;
            let next = score.unwrap_or(0.0) as i64;
            let data = key_to_data(&repeat_key);
            let (tz, cron, every) = if data.cron.is_some() {
                (data.tz.clone(), data.cron.clone(), None)
            } else {
                let every = data.tz.as_deref().and_then(|val| val.parse::<i64>().ok());
                (None, None, every)
            };
            jobs.push(RepeatableJob {
                key: data.key,
                name: data.name,
                id: data.id,
                end_date: data.end_date,
                tz,
                cron,
                every,
                next,
            });
        }
        Ok(jobs)
    }

    pub async fn get_repeatable_count(&self) -> Result<i64, Box<dyn std::error::Error>> {
        let key = self.to_key("repeat");
        let count: i64 = self.redis_client.zcard(key).await?;
        Ok(count)
    }

    pub async fn pause(&self, is_local: bool) -> Result<(), Box<dyn std::error::Error>> {
        if is_local {
            self.paused.store(true, Ordering::SeqCst);
            self.emit(QueueEvent::Paused);
            return Ok(());
        }
        self.commands.pause(self, true).await?;
        self.emit(QueueEvent::Paused);
        Ok(())
    }

    pub async fn resume(&self, is_local: bool) -> Result<(), Box<dyn std::error::Error>> {
        if is_local {
            self.paused.store(false, Ordering::SeqCst);
            self.emit(QueueEvent::Resumed);
            return Ok(());
        }
        self.commands.pause(self, false).await?;
        self.emit(QueueEvent::Resumed);
        Ok(())
    }

    pub async fn is_paused(&self, is_local: bool) -> Result<bool, Box<dyn std::error::Error>> {
        if is_local {
            return Ok(self.paused.load(Ordering::SeqCst));
        }

        let is_paused: i64 = self.redis_client.exists(self.get_key("meta-paused")).await?;
        if is_paused > 0 {
            return Ok(true);
        }

        let is_paused_meta: i64 = self.redis_client.hexists(self.get_key("meta"), "paused").await?;
        Ok(is_paused_meta > 0)
    }

    pub async fn retry_jobs(&self, count: usize) -> Result<(), Box<dyn std::error::Error>> {
        let mut cursor = 1i64;
        while cursor != 0 {
            cursor = self.commands.retry_jobs(self, count).await?;
        }
        Ok(())
    }

    pub async fn clean(&self, grace: i64, set_type: &str, limit: usize) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        if grace <= 0 {
            return Err("grace must be greater than 0".into());
        }
        let allowed = ["completed", "wait", "active", "paused", "delayed", "failed"];
        if !allowed.contains(&set_type) {
            return Err(format!("Cannot clean unknown queue type {}", set_type).into());
        }
        let max_timestamp = Utc::now().timestamp_millis() - grace;
        self.commands.clean_jobs_in_set(self, set_type, max_timestamp, limit).await
    }

    pub async fn get_job_counts(&self, types: &[&str]) -> Result<HashMap<String, i64>, Box<dyn std::error::Error>> {
        let types = parse_type_arg(types);
        let mut counts = HashMap::new();
        for entry in types.iter() {
            let normalized = normalize_type(entry);
            let count = match normalized.as_str() {
                "completed" | "failed" | "delayed" | "repeat" => {
                    self.redis_client.zcard(self.to_key(normalized.as_str())).await?
                }
                "active" | "wait" | "paused" => {
                    self.redis_client.llen(self.to_key(normalized.as_str())).await?
                }
                "stalled" => {
                    self.redis_client.scard(self.to_key(normalized.as_str())).await?
                }
                _ => 0,
            };
            counts.insert(entry.clone(), count);
        }
        Ok(counts)
    }

    pub async fn get_job_count_by_types(&self, types: &[&str]) -> Result<i64, Box<dyn std::error::Error>> {
        let counts = self.get_job_counts(types).await?;
        Ok(counts.values().sum())
    }

    pub async fn count(&self) -> Result<i64, Box<dyn std::error::Error>> {
        self.get_job_count_by_types(&["wait", "paused", "delayed"]).await
    }

    pub async fn get_completed_count(&self) -> Result<i64, Box<dyn std::error::Error>> {
        self.get_job_count_by_types(&["completed"]).await
    }

    pub async fn get_failed_count(&self) -> Result<i64, Box<dyn std::error::Error>> {
        self.get_job_count_by_types(&["failed"]).await
    }

    pub async fn get_delayed_count(&self) -> Result<i64, Box<dyn std::error::Error>> {
        self.get_job_count_by_types(&["delayed"]).await
    }

    pub async fn get_active_count(&self) -> Result<i64, Box<dyn std::error::Error>> {
        self.get_job_count_by_types(&["active"]).await
    }

    pub async fn get_waiting_count(&self) -> Result<i64, Box<dyn std::error::Error>> {
        self.get_job_count_by_types(&["wait", "paused"]).await
    }

    pub async fn get_stalled_count(&self) -> Result<i64, Box<dyn std::error::Error>> {
        Ok(self.redis_client.scard(self.to_key("stalled")).await?)
    }

    pub async fn get_paused_count(&self) -> Result<i64, Box<dyn std::error::Error>> {
        self.get_job_count_by_types(&["paused"]).await
    }

    pub async fn get_job_logs(&self, job_id: &str, start: Option<i64>, end: Option<i64>, asc: bool) -> Result<JobLogs, Box<dyn std::error::Error>> {
        let (start, end) = normalize_range(start, end);
        let logs_key = self.to_key(&format!("{}:logs", job_id));
        let logs: Vec<String> = if asc {
            self.redis_client.lrange(logs_key.clone(), start, end).await?
        } else {
            let range_start = -(end + 1);
            let range_end = -(start + 1);
            let mut values: Vec<String> = self.redis_client.lrange(logs_key.clone(), range_start, range_end).await?;
            values.reverse();
            values
        };
        let count: i64 = self.redis_client.llen(logs_key).await?;
        Ok(JobLogs { logs, count })
    }

    pub async fn get_metrics(&self, metric_type: &str, start: Option<i64>, end: Option<i64>) -> Result<QueueMetrics, Box<dyn std::error::Error>> {
        let (start, end) = normalize_range(start, end);
        let metrics_key = self.to_key(&format!("metrics:{}", metric_type));
        let data_key = format!("{}:data", metrics_key);

        let values: Vec<Option<String>> = self.redis_client.hmget(metrics_key.clone(), vec!["count", "prevTS", "prevCount"]).await?;
        let count = values.get(0).and_then(|val| val.as_ref()).and_then(|val| val.parse::<i64>().ok()).unwrap_or(0);
        let prev_ts = values.get(1).and_then(|val| val.as_ref()).and_then(|val| val.parse::<i64>().ok()).unwrap_or(0);
        let prev_count = values.get(2).and_then(|val| val.as_ref()).and_then(|val| val.parse::<i64>().ok()).unwrap_or(0);

        let data: Vec<String> = self.redis_client.lrange(data_key.clone(), start, end).await?;
        let length: i64 = self.redis_client.llen(data_key).await?;

        Ok(QueueMetrics {
            meta: MetricsMeta {
                count,
                prev_ts,
                prev_count,
            },
            data,
            count: length,
        })
    }

    pub async fn get_ranges(&self, types: &[&str], start: Option<i64>, end: Option<i64>, asc: bool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let types = parse_type_arg(types);
        let (start, end) = normalize_range(start, end);
        let mut results = Vec::new();
        for entry in types.iter() {
            let normalized = normalize_type(entry);
            match normalized.as_str() {
                "completed" | "failed" | "delayed" | "repeat" => {
                    if asc {
                        let mut values: Vec<String> = self.redis_client.zrange(self.to_key(normalized.as_str()), start, end, None, false, None, false).await?;
                        results.append(&mut values);
                    } else {
                        let mut values: Vec<String> = self.redis_client.zrevrange(self.to_key(normalized.as_str()), start, end, false).await?;
                        results.append(&mut values);
                    }
                }
                "active" | "wait" | "paused" => {
                    if asc {
                        let range_start = -(end + 1);
                        let range_end = -(start + 1);
                        let mut values: Vec<String> = self.redis_client.lrange(self.to_key(normalized.as_str()), range_start, range_end).await?;
                        values.reverse();
                        results.append(&mut values);
                    } else {
                        let mut values: Vec<String> = self.redis_client.lrange(self.to_key(normalized.as_str()), start, end).await?;
                        results.append(&mut values);
                    }
                }
                _ => {}
            }
        }
        Ok(results)
    }

    pub async fn get_jobs(self: &Arc<Self>, types: &[&str], start: Option<i64>, end: Option<i64>, asc: bool) -> Result<Vec<Option<Job>>, Box<dyn std::error::Error>> {
        let job_ids = self.get_ranges(types, start, end, asc).await?;
        let mut futures = Vec::with_capacity(job_ids.len());
        for job_id in job_ids {
            let queue = Arc::clone(self);
            futures.push(async move { Job::from_id(queue, job_id.as_str()).await });
        }
        let results = join_all(futures).await;
        let mut jobs = Vec::with_capacity(results.len());
        for result in results {
            jobs.push(result?);
        }
        Ok(jobs)
    }

    pub async fn get_waiting(self: &Arc<Self>, start: Option<i64>, end: Option<i64>) -> Result<Vec<Option<Job>>, Box<dyn std::error::Error>> {
        self.get_jobs(&["wait", "paused"], start, end, true).await
    }

    pub async fn get_active(self: &Arc<Self>, start: Option<i64>, end: Option<i64>) -> Result<Vec<Option<Job>>, Box<dyn std::error::Error>> {
        self.get_jobs(&["active"], start, end, true).await
    }

    pub async fn get_delayed(self: &Arc<Self>, start: Option<i64>, end: Option<i64>) -> Result<Vec<Option<Job>>, Box<dyn std::error::Error>> {
        self.get_jobs(&["delayed"], start, end, true).await
    }

    pub async fn get_completed(self: &Arc<Self>, start: Option<i64>, end: Option<i64>) -> Result<Vec<Option<Job>>, Box<dyn std::error::Error>> {
        self.get_jobs(&["completed"], start, end, false).await
    }

    pub async fn get_failed(self: &Arc<Self>, start: Option<i64>, end: Option<i64>) -> Result<Vec<Option<Job>>, Box<dyn std::error::Error>> {
        self.get_jobs(&["failed"], start, end, false).await
    }

    pub async fn get_job(self: &Arc<Self>, job_id: &str) -> Result<Option<Job>, Box<dyn std::error::Error>> {
        Job::from_id(Arc::clone(self), job_id).await
    }

    pub async fn remove_jobs(&self, pattern: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut cursor = 0i64;
        let mut removed = Vec::new();
        loop {
            let (next, mut batch) = self.commands.remove_jobs(self, pattern, cursor).await?;
            for job_id in batch.iter() {
                self.emit(QueueEvent::Removed(job_id.to_string()));
            }
            removed.append(&mut batch);
            if next == 0 {
                break;
            }
            cursor = next;
        }
        Ok(removed)
    }

    pub async fn empty(&self) -> Result<(), Box<dyn std::error::Error>> {
        let wait_key = self.get_key("wait");
        let paused_key = self.get_key("paused");
        let waiting: Vec<String> = self.redis_client.lrange(wait_key.clone(), 0, -1).await?;
        let paused: Vec<String> = self.redis_client.lrange(paused_key.clone(), 0, -1).await?;

        let limiter_pattern = format!("{}*:limited", self.get_key(""));
        let limited_keys: Vec<String> = self
            .redis_client
            .custom(CustomCommand::new("KEYS", ClusterHash::FirstKey, false), vec![limiter_pattern])
            .await?;

        let limiter_key = self.get_key("limiter");
        let delete_keys = vec![
            wait_key,
            paused_key,
            self.get_key("meta-paused"),
            self.get_key("delayed"),
            self.get_key("priority"),
            limiter_key.clone(),
            format!("{}:index", limiter_key),
        ];
        let _: i64 = self.redis_client.del(delete_keys).await?;

        let job_keys = waiting
            .into_iter()
            .chain(paused.into_iter())
            .filter(|value| !value.is_empty())
            .map(|job_id| self.get_key(job_id.as_str()))
            .collect::<Vec<String>>();

        for chunk in job_keys.chunks(10000) {
            let _: i64 = self.redis_client.del(chunk.to_vec()).await?;
        }

        for chunk in limited_keys.chunks(10000) {
            let _: i64 = self.redis_client.del(chunk.to_vec()).await?;
        }

        Ok(())
    }

    pub async fn obliterate(&self, force: bool, count: usize) -> Result<(), Box<dyn std::error::Error>> {
        self.pause(false).await?;
        let mut cursor = 1i64;
        while cursor != 0 {
            let result = self.commands.obliterate(self, count, force).await?;
            if result < 0 {
                return Err(match result {
                    -1 => "Cannot obliterate non-paused queue".into(),
                    -2 => "Cannot obliterate queue with active jobs".into(),
                    _ => "Cannot obliterate queue".into(),
                });
            }
            cursor = result;
        }
        Ok(())
    }

    pub async fn get_next_job(&self) -> Result<Option<String>, Box<dyn std::error::Error>> {
        if self.paused.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let job_id = self.commands.get_job_id(self).await?;
        Ok(job_id)
    }

    pub async fn move_to_active(&self, job_id: Option<String>) -> Result<Option<(Vec<String>, String)>, Box<dyn std::error::Error>> {
        let job_id = match job_id {
            Some(value) => value,
            None => return Ok(None),
        };
        let job = self.commands.move_to_active(self, Some(job_id)).await?;
        Ok(job)
    }

    pub async fn move_unlocked_jobs_to_wait(&self) -> Result<(), Box<dyn std::error::Error>> {
        let (_failed, _stalled) = self.commands.move_stalled_jobs_to_wait(self).await?;
        // dbg!(&failed);
        // dbg!(&stalled);
        Ok(())
    }

    pub async fn process<F, R>(self: Arc<Self>, func: F) -> Result<QueueHandle, Box<dyn std::error::Error>>
        where
            F: Fn(String) -> R + Send + Sync + 'static,
            R: Future<Output = String> + Send + 'static,
    {
        let func = std::sync::Arc::new(func);
        self.set_handler("__default__", move |job: JobRecord| {
            let func = func.clone();
            let data = job.data.clone();
            async move { Ok(func(data).await) }
        }).await?;

        self.run("__default__", 1).await
    }

    pub async fn process_file(self: Arc<Self>, handler_path: &str) -> Result<QueueHandle, Box<dyn std::error::Error>> {
        self.set_handler_file("__default__", handler_path).await?;
        self.run("__default__", 1).await
    }

    pub async fn run(self: Arc<Self>, handler_name: &str, concurrency: usize) -> Result<QueueHandle, Box<dyn std::error::Error>> {
        let concurrency = if concurrency == 0 { 1 } else { concurrency };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut handles = Vec::with_capacity(concurrency + 1);
        let handler_name = handler_name.to_string();

        if let Err(err) = self.move_unlocked_jobs_to_wait().await {
            self.emit(QueueEvent::Error(err.to_string()));
        }

        if self.settings.stalled_interval > 0 {
            let queue = Arc::clone(&self);
            let mut shutdown_rx = shutdown_rx.clone();
            handles.push(tokio::spawn(async move {
                let mut interval = interval(Duration::from_millis(queue.settings.stalled_interval));
                loop {
                    tokio::select! {
                        _ = shutdown_rx.changed() => break,
                        _ = interval.tick() => {
                            if queue.closing.load(Ordering::SeqCst) {
                                break;
                            }
                            if let Err(err) = queue.move_unlocked_jobs_to_wait().await {
                                queue.emit(QueueEvent::Error(err.to_string()));
                            }
                        }
                    }
                }
            }));
        }

        for _ in 0..concurrency {
            let queue = Arc::clone(&self);
            let mut shutdown_rx = shutdown_rx.clone();
            let handler_name = handler_name.clone();
            handles.push(tokio::spawn(async move {
                queue.process_loop(handler_name, &mut shutdown_rx).await;
            }));
        }

        Ok(QueueHandle {
            queue: Arc::clone(&self),
            shutdown_tx,
            handles,
        })
    }

    async fn process_loop(self: Arc<Self>, handler_name: String, shutdown_rx: &mut watch::Receiver<bool>) {
        loop {
            if self.closing.load(Ordering::SeqCst) || *shutdown_rx.borrow() {
                break;
            }

            if self.paused.load(Ordering::SeqCst) {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        break;
                    }
                    _ = sleep(Duration::from_millis(50)) => {}
                }
                continue;
            }

            if let Err(err) = self.update_delay_timer().await {
                self.emit(QueueEvent::Error(err.to_string()));
                continue;
            }
            let job_id = match self.get_next_job().await {
                Ok(value) => value,
                Err(err) => {
                    self.emit(QueueEvent::Error(err.to_string()));
                    continue;
                }
            };
            let job_id = match job_id {
                Some(value) => value,
                None => {
                    if !self.drained.swap(true, Ordering::SeqCst) {
                        self.emit(QueueEvent::Drained);
                    }
                    continue;
                }
            };

            let job_data = match self.move_to_active(Some(job_id)).await {
                Ok(value) => value,
                Err(err) => {
                    self.emit(QueueEvent::Error(err.to_string()));
                    continue;
                }
            };
            let (job_data, job_id) = match job_data {
                Some(value) => value,
                None => continue,
            };

            let job = match JobRecord::from_redis(&job_id, job_data) {
                Some(value) => value,
                None => continue,
            };

            let job_opts = JobOpts::from_value(job.opts.clone());
            if job_opts.repeat.is_some() {
                if let Err(err) = self.next_repeatable_job(&job.name, &job.data, job_opts.clone(), false).await {
                    self.emit(QueueEvent::Error(err.to_string()));
                }
            }

            let timeout_ms = job_opts.timeout;

            self.drained.store(false, Ordering::SeqCst);
            self.emit(QueueEvent::Active(job.id.clone()));

            let handler = {
                let handlers = self.handlers.read().await;
                handlers
                    .get(&job.name)
                    .or_else(|| handlers.get(handler_name.as_str()))
                    .or_else(|| handlers.get("*"))
                    .cloned()
            };

            let handler = match handler {
                Some(value) => value,
                None => {
                    let err = format!("Missing process handler for job type {}", job.name);
                    match self.fail_job(job.id.as_str(), &err).await {
                        Ok(MoveToFailedResult::Failed) => self.emit(QueueEvent::Failed(job.id.clone(), err)),
                        Ok(_) => {}
                        Err(err) => self.emit(QueueEvent::Error(err.to_string())),
                    }
                    continue;
                }
            };

            let lock_extender = start_lock_extender(Arc::clone(&self), job.id.clone());
            let result = match handler {
                Processor::Inline(handler) => {
                    if let Some(timeout_ms) = timeout_ms {
                        match tokio::time::timeout(Duration::from_millis(timeout_ms), handler(job.clone())).await {
                            Ok(result) => result,
                            Err(_) => Err(format!("Promise timed out after {} milliseconds", timeout_ms)),
                        }
                    } else {
                        handler(job.clone()).await
                    }
                }
                Processor::Sandboxed(processor) => {
                    if let Some(timeout_ms) = timeout_ms {
                        match tokio::time::timeout(Duration::from_millis(timeout_ms), processor.process(&job)).await {
                            Ok(result) => result,
                            Err(_) => Err(format!("Promise timed out after {} milliseconds", timeout_ms)),
                        }
                    } else {
                        processor.process(&job).await
                    }
                }
            };

            if let Some((tx, handle)) = lock_extender {
                let _ = tx.send(true);
                let _ = handle.await;
            }

            match result {
                Ok(result) => {
                    let keep_jobs = crate::commands::KeepJobsConfig { count: -1, age: None };
                    if let Err(err) = self.commands.move_to_completed_with_opts(self.as_ref(), &job.id, &result, keep_jobs, false, true).await {
                        self.emit(QueueEvent::Error(err.to_string()));
                    }
                    self.emit(QueueEvent::Completed(job.id.clone(), result));
                }
                Err(err) => {
                    match self.fail_job(job.id.as_str(), &err).await {
                        Ok(MoveToFailedResult::Failed) => self.emit(QueueEvent::Failed(job.id.clone(), err)),
                        Ok(_) => {}
                        Err(err) => self.emit(QueueEvent::Error(err.to_string())),
                    }
                }
            }
        }
    }

    async fn fail_job(self: &Arc<Self>, job_id: &str, err: &str) -> Result<MoveToFailedResult, Box<dyn std::error::Error>> {
        let job = match Job::from_id(Arc::clone(self), job_id).await {
            Ok(value) => value,
            Err(err) => return Err(err),
        };
        if let Some(mut job) = job {
            return job.move_to_failed(err, false).await;
        }
        match self.commands.move_to_failed(self.as_ref(), job_id, err).await {
            Ok(()) => Ok(MoveToFailedResult::Failed),
            Err(err) => Err(err),
        }
    }

    pub fn get_key(&self, pf: &str) -> String {
        self.queue_prefix.to_string() + pf
    }

    pub fn to_key(&self, queue_type: &str) -> String {
        vec![self.prefix.as_str(), self.queue_name.as_str(), queue_type].join(&self.joiner)
    }

    pub fn limiter(&self) -> Option<&RateLimiter> {
        self.limiter.as_ref()
    }

    pub fn get_keys(&self, postfix: Vec<&str>) -> Vec<String> {
        postfix.
            iter().map(|pf| {
                self.get_key(pf)
            })
            .collect()
    }

    pub async fn update_delay_timer(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let result = self.commands.update_delay_set(self, Utc::now().timestamp_millis()).await?;
        // dbg!(&result);
        Ok(result)
    }

    pub(crate) async fn next_repeatable_job(
        &self,
        name: &str,
        data: &str,
        opts: JobOpts,
        skip_check_exists: bool,
    ) -> Result<Option<Vec<String>>, Box<dyn std::error::Error>> {
        let mut repeat = match opts.repeat.clone() {
            Some(value) => value,
            None => return Ok(None),
        };

        let prev_millis = opts.prev_millis.unwrap_or(0);
        if prev_millis == 0 && !opts.custom_job_id.is_empty() {
            repeat.job_id = Some(opts.custom_job_id.clone());
        }

        let current_count = repeat.count.map(|count| count + 1).unwrap_or(1);
        if let Some(limit) = repeat.limit {
            if current_count > limit {
                return Ok(None);
            }
        }

        let now = Utc::now().timestamp_millis();
        if let Some(end_date) = repeat.end_date {
            if now > end_date {
                return Ok(None);
            }
        }

        let now = if prev_millis > now { prev_millis } else { now };
        let next_millis = match get_next_millis(now, &repeat) {
            Ok(value) => value,
            Err(err) => return Err(err),
        };

        let next_millis = match next_millis {
            Some(value) => value,
            None => return Ok(None),
        };

        let job_id = repeat.job_id.clone().unwrap_or_default();
        let job_id_prefix = if job_id.is_empty() { ":".to_string() } else { format!("{}:", job_id) };
        let repeat_key = get_repeat_key(name, &repeat, &job_id_prefix);

        let create_next_job = |repeat_key: String, mut repeat: RepeatOpts, mut opts: JobOpts| async move {
            let _: i64 = self.redis_client
                .zadd(self.to_key("repeat"), None, None, false, false, vec![(next_millis as f64, repeat_key.clone())])
                .await?;

            let custom_id = get_repeat_job_id(name, &job_id_prefix, Some(next_millis), &md5_hex(&repeat_key));
            let now = Utc::now().timestamp_millis();
            let delay = (next_millis - now).max(0) as u64;

            repeat.count = Some(current_count);
            repeat.key = Some(repeat_key.clone());
            opts.repeat = Some(repeat);
            opts.custom_job_id = custom_id;
            opts.prev_millis = Some(next_millis);

            let mut job = Job::new(data);
            job.name = name.to_string();
            job.delay = delay;
            job.timestamp = now;
            job.opts = Some(opts.clone());

            let result = self.add_job_raw(&mut job, opts).await?;
            Ok::<Vec<String>, Box<dyn std::error::Error>>(result)
        };

        if skip_check_exists {
            return Ok(Some(create_next_job(repeat_key, repeat, opts).await?));
        }

        let exists: Option<f64> = self.redis_client.zscore(self.to_key("repeat"), repeat_key.as_str()).await?;
        if exists.is_some() {
            return Ok(Some(create_next_job(repeat_key, repeat, opts).await?));
        }

        Ok(None)
    }

    /*
    pub async fn process<F, R>(&self, func: F) -> Result<bool, Box<dyn std::error::Error>>
        where
            R: Future<Output = String>,
            F: FnOnce(String) -> R,
    {
        //println!("{}", func("yyy".to_string()).await);
        let data = "process data".to_string();
        println!("process data: {}", data);
        println!("result: {}", result);
    }
    */
}

impl SandboxedProcessor {
    async fn process(&self, job: &JobRecord) -> Result<String, String> {
        self.pool.execute(self.path.as_str(), job).await
    }
}

impl ChildPool {
    fn new(size: usize) -> ChildPool {
        let size = if size == 0 { 1 } else { size };
        ChildPool {
            size,
            available: Mutex::new(Vec::new()),
            semaphore: Semaphore::new(size),
        }
    }

    async fn execute(&self, path: &str, job: &JobRecord) -> Result<String, String> {
        let _permit = self.semaphore.acquire().await.map_err(|_| "Child pool closed".to_string())?;
        let mut child = self.take_or_spawn(path).await?;

        let payload = match build_child_payload(job) {
            Ok(value) => value,
            Err(err) => return Err(err),
        };

        if let Err(err) = child.stdin.write_all(payload.as_bytes()).await {
            let _ = child.child.kill().await;
            return Err(err.to_string());
        }
        if let Err(err) = child.stdin.write_all(b"\n").await {
            let _ = child.child.kill().await;
            return Err(err.to_string());
        }

        let mut line = String::new();
        match child.stdout.read_line(&mut line).await {
            Ok(0) => {
                let _ = child.child.kill().await;
                return Err("Child process closed".to_string());
            }
            Ok(_) => {},
            Err(err) => {
                let _ = child.child.kill().await;
                return Err(err.to_string());
            }
        }

        let result = parse_child_result(line.trim());
        self.return_child(child).await;
        result
    }

    async fn take_or_spawn(&self, path: &str) -> Result<ChildProcess, String> {
        if let Some(child) = self.available.lock().await.pop() {
            return Ok(child);
        }
        spawn_child(path).await
    }

    async fn return_child(&self, child: ChildProcess) {
        let mut available = self.available.lock().await;
        if available.len() < self.size {
            available.push(child);
        }
    }

    async fn close(&self) {
        let mut available = self.available.lock().await;
        for mut child in available.drain(..) {
            let _ = child.child.kill().await;
        }
    }
}

#[derive(Debug, Clone)]
struct RepeatableKeyData {
    key: String,
    name: String,
    id: Option<String>,
    end_date: Option<i64>,
    tz: Option<String>,
    cron: Option<String>,
}

fn key_to_data(key: &str) -> RepeatableKeyData {
    let parts: Vec<&str> = key.split(':').collect();
    let name = parts.get(0).unwrap_or(&"").to_string();
    let id = parts.get(1).and_then(|val| if val.is_empty() { None } else { Some(val.to_string()) });
    let end_date = parts.get(2).and_then(|val| val.parse::<i64>().ok());
    let tz = parts.get(3).and_then(|val| if val.is_empty() { None } else { Some(val.to_string()) });
    let cron = parts.get(4).and_then(|val| if val.is_empty() { None } else { Some(val.to_string()) });

    RepeatableKeyData {
        key: key.to_string(),
        name,
        id,
        end_date,
        tz,
        cron,
    }
}

fn get_repeat_job_id(name: &str, job_id: &str, next_millis: Option<i64>, namespace: &str) -> String {
    let next = next_millis.map(|value| value.to_string()).unwrap_or_default();
    format!("repeat:{}:{}", md5_hex(&format!("{}{}{}", name, job_id, namespace)), next)
}

fn get_repeat_key(name: &str, repeat: &RepeatOpts, job_id: &str) -> String {
    let end_date = repeat.end_date.map(|value| format!("{}:", value)).unwrap_or_else(|| ":".to_string());
    let tz = repeat.tz.as_deref().map(|value| format!("{}:", value)).unwrap_or_else(|| ":".to_string());
    let suffix = match repeat.cron.as_deref() {
        Some(cron) => format!("{}{}", tz, cron),
        None => repeat.every.map(|every| every.to_string()).unwrap_or_default(),
    };
    format!("{}:{}{}{}", name, job_id, end_date, suffix)
}

fn get_next_millis(millis: i64, repeat: &RepeatOpts) -> Result<Option<i64>, Box<dyn std::error::Error>> {
    if repeat.cron.is_some() && repeat.every.is_some() {
        return Err("Both .cron and .every options are defined for this repeatable job".into());
    }

    if let Some(every) = repeat.every {
        if every <= 0 {
            return Ok(None);
        }
        let next = (millis / every) * every + every;
        return Ok(Some(next));
    }

    let cron = match repeat.cron.as_deref() {
        Some(value) => value,
        None => return Ok(None),
    };

    let schedule = Schedule::from_str(cron)?;
    let tz: Tz = repeat
        .tz
        .as_deref()
        .unwrap_or("UTC")
        .parse()
        .unwrap_or(chrono_tz::UTC);

    let start_millis = repeat.start_date.unwrap_or(millis);
    let current_millis = if start_millis > millis { start_millis } else { millis };
    let current_date = tz
        .timestamp_millis_opt(current_millis)
        .single()
        .unwrap_or_else(|| tz.timestamp_millis_opt(Utc::now().timestamp_millis()).single().unwrap());

    let mut iter = schedule.after(&current_date);
    if let Some(next) = iter.next() {
        return Ok(Some(next.with_timezone(&Utc).timestamp_millis()));
    }

    Ok(None)
}

fn md5_hex(value: &str) -> String {
    let mut hasher = Md5::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn job_id_for_group(limiter: Option<&RateLimiter>, job_id: &str, data: &str) -> String {
    let limiter = match limiter {
        Some(value) => value,
        None => return job_id.to_string(),
    };

    let group_key = match limiter.group_key.as_deref() {
        Some(value) => value,
        None => return job_id.to_string(),
    };

    let data_value: Value = match serde_json::from_str(data) {
        Ok(value) => value,
        Err(_) => return job_id.to_string(),
    };

    let group_value = data_value.get(group_key).and_then(|val| val.as_str()).unwrap_or("");

    if job_id.is_empty() {
        format!("{}:{}", Uuid::new_v4(), group_value)
    } else {
        format!("{}:{}", job_id, group_value)
    }
}

fn normalize_range(start: Option<i64>, end: Option<i64>) -> (i64, i64) {
    (start.unwrap_or(0), end.unwrap_or(-1))
}

fn normalize_type(value: &str) -> String {
    if value == "waiting" {
        "wait".to_string()
    } else {
        value.to_string()
    }
}

fn parse_type_arg(types: &[&str]) -> Vec<String> {
    let defaults = vec![
        "waiting".to_string(),
        "active".to_string(),
        "completed".to_string(),
        "failed".to_string(),
        "delayed".to_string(),
        "paused".to_string(),
    ];

    if types.is_empty() {
        return defaults;
    }

    let joined = types.join(",");
    let mut result = Vec::new();
    for part in joined.split(',') {
        let trimmed = part.trim();
        if !trimmed.is_empty() {
            result.push(trimmed.to_string());
        }
    }

    if result.is_empty() {
        defaults
    } else {
        result
    }
}

fn redis_value_to_string(value: &RedisValue) -> Option<String> {
    match value {
        RedisValue::String(value) => Some(value.to_string()),
        RedisValue::Bytes(value) => String::from_utf8(value.to_vec()).ok(),
        RedisValue::Integer(value) => Some(value.to_string()),
        RedisValue::Double(value) => Some(value.to_string()),
        RedisValue::Boolean(value) => Some(if *value { "1".to_string() } else { "0".to_string() }),
        _ => None,
    }
}

fn parse_event_data(payload: &str) -> Option<(String, String)> {
    let value: Value = serde_json::from_str(payload).ok()?;
    let obj = value.as_object()?;
    let job_id = obj.get("jobId")?.as_str()?.to_string();
    let val = obj.get("val");
    let val = match val {
        Some(Value::String(value)) => value.clone(),
        Some(other) => other.to_string(),
        None => "".to_string(),
    };
    Some((job_id, val))
}

fn parse_progress_data(payload: &str) -> Option<(String, String)> {
    if let Ok(value) = serde_json::from_str::<Value>(payload) {
        if let Some(obj) = value.as_object() {
            let job_id = obj.get("jobId")?.as_str()?.to_string();
            let progress = obj.get("progress");
            let progress = match progress {
                Some(Value::String(value)) => value.clone(),
                Some(other) => other.to_string(),
                None => "".to_string(),
            };
            return Some((job_id, progress));
        }
    }

    let mut parts = payload.splitn(2, ',');
    let job_id = parts.next()?.to_string();
    let progress = parts.next()?.to_string();
    Some((job_id, progress))
}

async fn spawn_child(path: &str) -> Result<ChildProcess, String> {
    let mut command = Command::new(path);
    command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit());
    let mut child = command.spawn().map_err(|err| err.to_string())?;
    let stdin = child.stdin.take().ok_or("Failed to open child stdin".to_string())?;
    let stdout = child.stdout.take().ok_or("Failed to open child stdout".to_string())?;
    Ok(ChildProcess {
        child,
        stdin,
        stdout: BufReader::new(stdout),
    })
}

fn build_child_payload(job: &JobRecord) -> Result<String, String> {
    let opts = job.opts.clone();
    let payload = serde_json::json!({
        "id": job.id,
        "name": job.name,
        "data": job.data,
        "opts": opts,
        "timestamp": job.timestamp,
        "delay": job.delay,
        "priority": job.priority,
    });
    serde_json::to_string(&payload).map_err(|err| err.to_string())
}

fn parse_child_result(payload: &str) -> Result<String, String> {
    if payload.is_empty() {
        return Err("Child process returned empty response".to_string());
    }
    if let Ok(value) = serde_json::from_str::<Value>(payload) {
        if let Some(obj) = value.as_object() {
            let status = obj.get("status").and_then(|val| val.as_str());
            if status == Some("ok") {
                let result = obj.get("result");
                let result = match result {
                    Some(Value::String(value)) => value.clone(),
                    Some(other) => other.to_string(),
                    None => "".to_string(),
                };
                return Ok(result);
            }
            if status == Some("error") {
                let err = obj.get("error");
                let err = match err {
                    Some(Value::String(value)) => value.clone(),
                    Some(other) => other.to_string(),
                    None => "Child process error".to_string(),
                };
                return Err(err);
            }
        }
        if let Some(value) = value.as_str() {
            return Ok(value.to_string());
        }
        return Ok(value.to_string());
    }
    Ok(payload.to_string())
}

fn start_lock_extender(queue: Arc<Queue>, job_id: String) -> Option<(watch::Sender<bool>, JoinHandle<()>)> {
    if queue.settings.lock_renew_time == 0 {
        return None;
    }

    let lock_renew_time = queue.settings.lock_renew_time;
    let lock_duration = queue.settings.lock_duration;
    let (tx, mut rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(lock_renew_time));
        loop {
            tokio::select! {
                _ = rx.changed() => break,
                _ = interval.tick() => {
                    match queue.commands().extend_lock(queue.as_ref(), &job_id, lock_duration).await {
                        Ok(true) => {}
                        Ok(false) => break,
                        Err(err) => {
                            queue.emit(QueueEvent::Error(err.to_string()));
                            break;
                        }
                    }
                }
            }
        }
    });
    Some((tx, handle))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;
    use tokio::time::timeout;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    const REDIS_URL: &str = "redis://0.0.0.0:6379";

    async fn build_queue() -> Result<Arc<Queue>, Box<dyn std::error::Error>> {
        Queue::new_with_opts(&format!("test-queue-{}", Uuid::new_v4()), REDIS_URL, QueueOptions::default()).await
    }

    #[tokio::test]
    async fn add_job_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let job = Job::new(r#"{"a": 1}"#);
        let result = queue.add_job(job).await?;
        dbg!(result);
        Ok(())
    }

    #[tokio::test]
    async fn update_delay_timer_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let x = queue.update_delay_timer().await?;
        dbg!(x);
        Ok(())
    }

    #[tokio::test]
    async fn get_next_job_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let x = queue.get_next_job().await?;
        dbg!(x);
        Ok(())
    }

    #[tokio::test]
    async fn move_to_active_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let job = Job::new(r#"{"a": 1}"#);
        let result = queue.add_job(job).await?;
        dbg!(result);
        let job_id = queue.get_next_job().await?;
        let job = queue.move_to_active(job_id).await?;
        dbg!(job);
        Ok(())
    }

    #[tokio::test]
    async fn move_unlocked_jobs_to_wait_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let data = queue.move_unlocked_jobs_to_wait().await?;
        dbg!(data);
        Ok(())
    }

    #[tokio::test]
    async fn stop_timeout_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let settings = QueueSettings {
            drain_delay: 1,
            ..QueueSettings::default()
        };
        let opts = QueueOptions {
            settings: Some(settings),
            ..QueueOptions::default()
        };
        let queue = Queue::new_with_opts(&format!("test-queue-{}", Uuid::new_v4()), REDIS_URL, opts).await?;
        let handle = queue.clone().process(|data: String| async move {
            sleep(Duration::from_millis(200)).await;
            data
        }).await?;

        let job = Job::new(r#"{"a": 1}"#);
        let _ = queue.add_job(job).await?;
        sleep(Duration::from_millis(10)).await;

        let result = handle.stop(Duration::from_millis(10)).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn stop_success_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let settings = QueueSettings {
            drain_delay: 1,
            ..QueueSettings::default()
        };
        let opts = QueueOptions {
            settings: Some(settings),
            ..QueueOptions::default()
        };
        let queue = Queue::new_with_opts(&format!("test-queue-{}", Uuid::new_v4()), REDIS_URL, opts).await?;
        let handle = queue.clone().process(|data: String| async move {
            sleep(Duration::from_millis(10)).await;
            data
        }).await?;

        let job = Job::new(r#"{"a": 1}"#);
        let _ = queue.add_job(job).await?;
        sleep(Duration::from_millis(50)).await;

        handle.stop(Duration::from_secs(2)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn new_with_opts_prefix_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let opts = QueueOptions {
            prefix: Some("custom".to_string()),
            ..QueueOptions::default()
        };
        let queue = Queue::new_with_opts(&format!("test-queue-{}", Uuid::new_v4()), REDIS_URL, opts).await?;
        assert_eq!(queue.to_key("wait"), format!("custom:{}:wait", queue.queue_name));
        Ok(())
    }

    #[tokio::test]
    async fn subscribe_pause_resume_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let mut receiver = queue.subscribe();
        queue.pause(true).await?;
        let paused = timeout(Duration::from_secs(1), receiver.recv()).await??;
        assert!(matches!(paused, QueueEvent::Paused));
        queue.resume(true).await?;
        let resumed = timeout(Duration::from_secs(1), receiver.recv()).await??;
        assert!(matches!(resumed, QueueEvent::Resumed));
        Ok(())
    }

    #[tokio::test]
    async fn set_handler_duplicate_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        queue.set_handler("dup", |_job: JobRecord| async { Ok("ok".to_string()) }).await?;
        let result = queue.set_handler("dup", |_job: JobRecord| async { Ok("ok".to_string()) }).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn add_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let result = queue.add(r#"{"a": 1}"#).await?;
        assert!(!result.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn add_named_with_opts_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let mut opts = JobOpts::default();
        opts.priority = 1;
        let result = queue.add_named_with_opts("named", r#"{"a": 2}"#, opts).await?;
        assert!(!result.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn add_bulk_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let jobs = vec![Job::new(r#"{"a": 1}"#), Job::new(r#"{"b": 2}"#)];
        let result = queue.add_bulk(jobs).await?;
        assert_eq!(result.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn repeatable_add_every_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let mut opts = JobOpts::default();
        opts.repeat = Some(RepeatOpts {
            every: Some(1000),
            cron: None,
            tz: None,
            end_date: None,
            start_date: None,
            limit: None,
            count: None,
            key: None,
            job_id: None,
        });
        let result = queue.add_with_opts(r#"{"a": 1}"#, opts).await?;
        assert!(!result.is_empty());

        let count = queue.get_repeatable_count().await?;
        assert_eq!(count, 1);

        let jobs = queue.get_repeatable_jobs(0, -1, true).await?;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].every, Some(1000));
        Ok(())
    }

    #[tokio::test]
    async fn repeatable_remove_by_key_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let mut opts = JobOpts::default();
        opts.repeat = Some(RepeatOpts {
            every: Some(5000),
            cron: None,
            tz: None,
            end_date: None,
            start_date: None,
            limit: None,
            count: None,
            key: None,
            job_id: None,
        });
        let _ = queue.add_with_opts(r#"{"a": 1}"#, opts).await?;

        let jobs = queue.get_repeatable_jobs(0, -1, true).await?;
        assert_eq!(jobs.len(), 1);

        queue.remove_repeatable_by_key(jobs[0].key.as_str()).await?;
        let count = queue.get_repeatable_count().await?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn repeatable_remove_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let repeat = RepeatOpts {
            every: Some(2000),
            cron: None,
            tz: None,
            end_date: None,
            start_date: None,
            limit: None,
            count: None,
            key: None,
            job_id: None,
        };
        let mut opts = JobOpts::default();
        opts.repeat = Some(repeat.clone());
        let _ = queue.add_with_opts(r#"{"a": 1}"#, opts).await?;

        queue.remove_repeatable(None, repeat).await?;
        let count = queue.get_repeatable_count().await?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn pause_resume_local_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        queue.pause(true).await?;
        assert!(queue.is_paused(true).await?);
        queue.resume(true).await?;
        assert!(!queue.is_paused(true).await?);
        Ok(())
    }

    #[tokio::test]
    async fn pause_resume_global_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        queue.pause(false).await?;
        assert!(queue.is_paused(false).await?);
        queue.resume(false).await?;
        assert!(!queue.is_paused(false).await?);
        Ok(())
    }

    #[tokio::test]
    async fn retry_jobs_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        queue.retry_jobs(100).await?;
        Ok(())
    }

    #[tokio::test]
    async fn clean_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let result = queue.clean(1000, "completed", 10).await?;
        assert!(result.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn clean_invalid_type_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let result = queue.clean(1000, "unknown", 10).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn get_job_counts_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let _ = queue.add(r#"{"a": 1}"#).await?;
        let _ = queue.add(r#"{"b": 2}"#).await?;

        let counts = queue.get_job_counts(&["wait", "paused"]).await?;
        assert_eq!(counts.get("wait").cloned().unwrap_or(0), 2);
        assert_eq!(counts.get("paused").cloned().unwrap_or(0), 0);

        let total = queue.get_job_count_by_types(&["wait", "paused"]).await?;
        assert_eq!(total, 2);
        Ok(())
    }

    #[tokio::test]
    async fn get_jobs_waiting_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let result_1 = queue.add(r#"{"a": 1}"#).await?;
        let result_2 = queue.add(r#"{"b": 2}"#).await?;
        let id_1 = result_1.first().cloned().unwrap_or_default();
        let id_2 = result_2.first().cloned().unwrap_or_default();

        let jobs = queue.get_waiting(Some(0), Some(-1)).await?;
        assert_eq!(jobs.len(), 2);
        let ids = jobs.into_iter().filter_map(|job| job.and_then(|entry| entry.id)).collect::<Vec<String>>();
        assert!(ids.contains(&id_1));
        assert!(ids.contains(&id_2));
        Ok(())
    }

    #[tokio::test]
    async fn get_job_logs_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let job = Job::create(queue.clone(), "test", "{}", None).await?;
        job.log("log-1").await?;
        job.log("log-2").await?;

        let job_id = job.id.clone().unwrap();
        let logs = queue.get_job_logs(job_id.as_str(), None, None, true).await?;
        assert_eq!(logs.count, 2);
        assert_eq!(logs.logs.len(), 2);
        assert_eq!(logs.logs[0], "log-1");
        assert_eq!(logs.logs[1], "log-2");
        Ok(())
    }

    #[tokio::test]
    async fn global_completed_event_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let mut receiver = queue.subscribe();
        let handle = queue.clone().process(|data: String| async move { data }).await?;

        let result = queue.add("ok").await?;
        let job_id = result.first().cloned().unwrap_or_default();

        let value = timeout(Duration::from_secs(2), async {
            loop {
                match receiver.recv().await {
                    Ok(QueueEvent::GlobalCompleted(id, val)) if id == job_id => return Ok::<String, Box<dyn std::error::Error>>(val),
                    Ok(_) => {}
                    Err(_) => {}
                }
            }
        }).await??;

        assert_eq!(value, "ok");
        handle.close().await?;
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sandboxed_processor_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let dir = std::env::temp_dir();
        let script_path = dir.join(format!("bullrs-processor-{}.sh", Uuid::new_v4()));
        let script = r#"#!/bin/sh
while IFS= read -r line; do
  echo '{"status":"ok","result":"ok"}'
done
"#;
        fs::write(&script_path, script)?;
        let mut perms = fs::metadata(&script_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script_path, perms)?;

        let handle = queue.clone().process_file(script_path.to_str().unwrap()).await?;
        let result = queue.add("{}") .await?;
        let job_id = result.first().cloned().unwrap_or_default();
        let job = Job::from_id(queue.clone(), &job_id).await?.expect("job should exist");
        let finished = timeout(Duration::from_secs(2), job.finished()).await??;
        assert_eq!(finished, "ok");

        handle.close().await?;
        let _ = fs::remove_file(&script_path);
        Ok(())
    }

    #[tokio::test]
    async fn metrics_completed_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let opts = QueueOptions {
            metrics: Some(MetricsOptions { max_data_points: 10 }),
            ..QueueOptions::default()
        };
        let queue = Queue::new_with_opts(&format!("test-queue-{}", Uuid::new_v4()), REDIS_URL, opts).await?;
        let handle = queue.clone().process(|data: String| async move { data }).await?;

        let _ = queue.add("ok").await?;
        let _ = queue.add("ok2").await?;

        let _ = timeout(Duration::from_secs(3), async {
            loop {
                let completed = queue.get_completed_count().await?;
                if completed >= 2 {
                    return Ok::<(), Box<dyn std::error::Error>>(());
                }
                sleep(Duration::from_millis(50)).await;
            }
        }).await??;

        let metrics = timeout(Duration::from_secs(2), async {
            loop {
                let metrics = queue.get_metrics("completed", Some(0), Some(-1)).await?;
                if metrics.meta.count >= 2 {
                    return Ok::<QueueMetrics, Box<dyn std::error::Error>>(metrics);
                }
                sleep(Duration::from_millis(50)).await;
            }
        }).await??;

        assert!(metrics.meta.count >= 2);
        handle.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn parse_client_list_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let name = queue.client_name();
        let sample = format!(
            "id=10 addr=127.0.0.1:6379 fd=5 name={}\n\
id=11 addr=127.0.0.1:6379 fd=6 name=other",
            name
        );
        let clients = queue.parse_client_list(sample.as_str());
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].get("name").cloned().unwrap_or_default(), queue.queue_name);
        Ok(())
    }

    #[tokio::test]
    async fn process_timeout_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let handle = queue.clone().process(|_data: String| async move {
            sleep(Duration::from_millis(50)).await;
            "ok".to_string()
        }).await?;

        let mut opts = JobOpts::default();
        opts.timeout = Some(10);
        let result = queue.add_with_opts("{}", opts).await?;
        let job_id = result.first().cloned().unwrap_or_default();
        let job = Job::from_id(queue.clone(), &job_id).await?.expect("job should exist");

        let finished = timeout(Duration::from_secs(2), job.finished()).await?;
        assert!(finished.is_err());

        handle.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn lock_renewal_allows_completion_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let settings = QueueSettings {
            lock_duration: 50,
            lock_renew_time: 10,
            stalled_interval: 0,
            ..QueueSettings::default()
        };
        let opts = QueueOptions {
            settings: Some(settings),
            ..QueueOptions::default()
        };
        let queue = Queue::new_with_opts(&format!("test-queue-{}", Uuid::new_v4()), REDIS_URL, opts).await?;
        let handle = queue.clone().process(|data: String| async move {
            sleep(Duration::from_millis(120)).await;
            data
        }).await?;

        let result = queue.add("ok").await?;
        let job_id = result.first().cloned().unwrap_or_default();
        let job = Job::from_id(queue.clone(), &job_id).await?.expect("job should exist");

        let finished = timeout(Duration::from_secs(2), job.finished()).await??;
        assert_eq!(finished, "ok");

        handle.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn remove_jobs_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let job_1 = Job::new(r#"{"a": 1}"#);
        let job_2 = Job::new(r#"{"b": 2}"#);
        let result_1 = queue.add_job(job_1).await?;
        let result_2 = queue.add_job(job_2).await?;
        let id_1 = result_1.first().cloned().unwrap_or_default();
        let id_2 = result_2.first().cloned().unwrap_or_default();

        let _ = queue.remove_jobs("*").await?;

        let fetched_1 = Job::from_id(queue.clone(), &id_1).await?;
        let fetched_2 = Job::from_id(queue.clone(), &id_2).await?;
        assert!(fetched_1.is_none());
        assert!(fetched_2.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn empty_queue_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let settings = QueueSettings {
            drain_delay: 1,
            ..QueueSettings::default()
        };
        let opts = QueueOptions {
            settings: Some(settings),
            ..QueueOptions::default()
        };
        let queue = Queue::new_with_opts(&format!("test-queue-{}", Uuid::new_v4()), REDIS_URL, opts).await?;
        let job = Job::new(r#"{"a": 1}"#);
        let result = queue.add_job(job).await?;
        let job_id = result.first().cloned().unwrap_or_default();

        queue.empty().await?;
        let next = queue.get_next_job().await?;
        assert!(next.is_none());
        let fetched = Job::from_id(queue.clone(), &job_id).await?;
        assert!(fetched.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn obliterate_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        queue.pause(false).await?;
        queue.obliterate(true, 1000).await?;
        Ok(())
    }

    #[tokio::test]
    async fn key_helpers_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        assert_eq!(queue.get_key("wait"), queue.to_key("wait"));
        let keys = queue.get_keys(vec!["wait", "active"]);
        assert_eq!(keys.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn limiter_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        assert!(queue.limiter().is_none());

        let opts = QueueOptions {
            limiter: Some(RateLimiter {
                max: 1,
                duration: 1000,
                bounce_back: false,
                group_key: None,
            }),
            ..QueueOptions::default()
        };
        let queue = Queue::new_with_opts(&format!("test-queue-{}", Uuid::new_v4()), REDIS_URL, opts).await?;
        assert!(queue.limiter().is_some());
        Ok(())
    }

    #[tokio::test]
    async fn run_close_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let settings = QueueSettings {
            drain_delay: 1,
            ..QueueSettings::default()
        };
        let opts = QueueOptions {
            settings: Some(settings),
            ..QueueOptions::default()
        };
        let queue = Queue::new_with_opts(&format!("test-queue-{}", Uuid::new_v4()), REDIS_URL, opts).await?;
        queue.set_handler("runner", |_job: JobRecord| async { Ok("ok".to_string()) }).await?;
        let handle = queue.clone().run("runner", 2).await?;
        handle.close().await?;
        Ok(())
    }

    #[test]
    fn job_id_for_group_case_1() {
        let limiter = RateLimiter {
            max: 1,
            duration: 1000,
            bounce_back: false,
            group_key: Some("group".to_string()),
        };

        let job_id = job_id_for_group(Some(&limiter), "abc", r#"{"group": "g1"}"#);
        assert_eq!(job_id, "abc:g1");

        let job_id = job_id_for_group(Some(&limiter), "", r#"{"group": "g2"}"#);
        assert!(job_id.ends_with(":g2"));

        let job_id = job_id_for_group(Some(&limiter), "abc", "invalid");
        assert_eq!(job_id, "abc");
    }
}
