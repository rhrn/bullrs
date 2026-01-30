use std::pin::Pin;
use std::future::Future;
use std::sync::Arc;
use std::collections::HashMap;
use chrono::{Utc, DateTime, FixedOffset};
use serde::Serialize;
use serde_json::Value;
use fred::interfaces::{HashesInterface, ListInterface, SortedSetsInterface};
use crate::queue::{Queue, QueueEvent};
use tokio::time::{interval, Duration};

pub type JobFn = Box<dyn Fn(String) -> Pin<Box<dyn Future<Output = ()>>>>;

pub type JobHandler = Arc<dyn Fn(JobRecord) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send>> + Send + Sync>;

pub type BackoffStrategy = Arc<dyn Fn(u32, &str, Option<&Value>) -> i64 + Send + Sync>;

pub const DEFAULT_JOB_NAME: &str = "__default__";
const FINISHED_WATCHDOG: u64 = 5000;

#[derive(Debug, Serialize, Clone)]
pub struct JobOpts {
    #[serde(rename = "jobId", skip_serializing_if = "String::is_empty")]
    pub custom_job_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repeat: Option<RepeatOpts>,
    pub priority: u32,
    pub lifo: bool,
    pub attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backoff: Option<BackoffConfig>,
    #[serde(rename = "removeOnComplete", skip_serializing_if = "Option::is_none")]
    pub remove_on_complete: Option<RemoveOn>,
    #[serde(rename = "removeOnFail", skip_serializing_if = "Option::is_none")]
    pub remove_on_fail: Option<RemoveOn>,
    #[serde(rename = "stackTraceLimit", skip_serializing_if = "Option::is_none")]
    pub stack_trace_limit: Option<usize>,
    #[serde(rename = "prevMillis", skip_serializing_if = "Option::is_none")]
    pub prev_millis: Option<i64>,
}

impl Default for JobOpts {
    fn default() -> JobOpts {
        JobOpts {
            custom_job_id: "".to_string(),
            repeat: None,
            priority: 0,
            lifo: false,
            attempts: 1,
            timeout: None,
            backoff: None,
            remove_on_complete: None,
            remove_on_fail: None,
            stack_trace_limit: None,
            prev_millis: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(untagged)]
pub enum RemoveOn {
    Count(i64),
    Enabled(bool),
}

#[derive(Debug, Serialize, Clone)]
pub struct RepeatOpts {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub every: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tz: Option<String>,
    #[serde(rename = "endDate", skip_serializing_if = "Option::is_none")]
    pub end_date: Option<i64>,
    #[serde(rename = "startDate", skip_serializing_if = "Option::is_none")]
    pub start_date: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(rename = "jobId", skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct BackoffConfig {
    #[serde(rename = "type")]
    pub backoff_type: String,
    pub delay: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Value>,
}

impl BackoffConfig {
    pub fn fixed(delay: i64) -> BackoffConfig {
        BackoffConfig {
            backoff_type: "fixed".to_string(),
            delay,
            options: None,
        }
    }

    pub fn exponential(delay: i64) -> BackoffConfig {
        BackoffConfig {
            backoff_type: "exponential".to_string(),
            delay,
            options: None,
        }
    }
}

#[derive(Serialize, Clone)]
pub struct Job {
    #[serde(skip)]
    pub queue: Option<Arc<Queue>>,
    pub id: Option<String>,
    pub name: String,
    pub data: String,
    pub attempts: Option<u32>,
    pub delay: u64,
    pub timestamp: i64,
    pub opts: Option<JobOpts>,
    pub progress: Option<String>,
    pub finished_on: Option<i64>,
    pub processed_on: Option<i64>,
    pub retried_on: Option<i64>,
    pub failed_reason: Option<String>,
    pub attempts_made: u32,
    pub stacktrace: Vec<String>,
    pub returnvalue: Option<String>,
    pub discarded: bool,
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("data", &self.data)
            .field("attempts", &self.attempts)
            .field("delay", &self.delay)
            .field("timestamp", &self.timestamp)
            .field("opts", &self.opts)
            .field("progress", &self.progress)
            .field("finished_on", &self.finished_on)
            .field("processed_on", &self.processed_on)
            .field("retried_on", &self.retried_on)
            .field("failed_reason", &self.failed_reason)
            .field("attempts_made", &self.attempts_made)
            .field("stacktrace", &self.stacktrace)
            .field("returnvalue", &self.returnvalue)
            .field("discarded", &self.discarded)
            .finish()
    }
}

impl Default for Job {
    fn default() -> Job {
        Job {
            queue: None,
            id: None,
            data: "".to_string(),
            name: DEFAULT_JOB_NAME.to_string(),
            attempts: Some(1),
            delay: 0,
            timestamp: Utc::now().timestamp_millis(),
            opts: Some(JobOpts::default()),
            progress: None,
            finished_on: None,
            processed_on: None,
            retried_on: None,
            failed_reason: None,
            attempts_made: 0,
            stacktrace: Vec::new(),
            returnvalue: None,
            discarded: false,
        }
    }
}

impl Job {
    pub fn new(data: &str) -> Job {
        Job {
            data: data.to_string(),
            ..Job::default()
        }
    }

    pub async fn create(queue: Arc<Queue>, name: &str, data: &str, opts: Option<JobOpts>) -> Result<Job, Box<dyn std::error::Error>> {
        let mut job = Job::new(data);
        job.name = name.to_string();
        job.opts = Some(opts.unwrap_or_default());
        job.queue = Some(queue.clone());

        let result = queue.add_job(job.clone()).await?;
        job.id = result.first().cloned();
        Ok(job)
    }

    pub async fn create_bulk(queue: Arc<Queue>, jobs: Vec<Job>) -> Result<Vec<Job>, Box<dyn std::error::Error>> {
        let mut results = Vec::with_capacity(jobs.len());
        for mut job in jobs {
            job.queue = Some(queue.clone());
            let result = queue.add_job(job.clone()).await?;
            job.id = result.first().cloned();
            results.push(job);
        }
        Ok(results)
    }

    pub async fn from_id(queue: Arc<Queue>, job_id: &str) -> Result<Option<Job>, Box<dyn std::error::Error>> {
        let job_key = queue.to_key(job_id);
        let raw: HashMap<String, String> = queue.redis_client().hgetall(job_key).await?;
        if raw.is_empty() {
            return Ok(None);
        }
        Ok(Some(Job::from_json(queue, raw, job_id)))
    }

    pub fn from_json(queue: Arc<Queue>, json: HashMap<String, String>, job_id: &str) -> Job {
        let opts_value = json.get("opts").and_then(|value| serde_json::from_str::<Value>(value).ok()).unwrap_or(Value::Null);
        let mut job = Job::new(json.get("data").map(String::as_str).unwrap_or(""));
        job.queue = Some(queue);
        job.id = Some(json.get("id").cloned().unwrap_or_else(|| job_id.to_string()));
        job.name = json.get("name").cloned().unwrap_or_else(|| DEFAULT_JOB_NAME.to_string());
        job.progress = json.get("progress").cloned();
        job.delay = json.get("delay").and_then(|value| value.parse::<u64>().ok()).unwrap_or(0);
        job.timestamp = json.get("timestamp").and_then(|value| value.parse::<i64>().ok()).unwrap_or(0);
        job.finished_on = json.get("finishedOn").and_then(|value| value.parse::<i64>().ok());
        job.processed_on = json.get("processedOn").and_then(|value| value.parse::<i64>().ok());
        job.retried_on = json.get("retriedOn").and_then(|value| value.parse::<i64>().ok());
        job.failed_reason = json.get("failedReason").cloned();
        job.attempts_made = json.get("attemptsMade").and_then(|value| value.parse::<u32>().ok()).unwrap_or(0);
        job.returnvalue = json.get("returnvalue").cloned();
        job.opts = Some(JobOpts::from_value(opts_value));
        job
    }

    pub fn to_json(&self) -> Value {
        let opts = self.opts.clone().unwrap_or_default();
        serde_json::to_value(opts).unwrap_or(Value::Null)
    }

    pub fn lock_key(&self) -> Result<String, Box<dyn std::error::Error>> {
        let queue = self.queue()?;
        let job_id = self.id.as_ref().ok_or("job id is missing")?;
        Ok(format!("{}:lock", queue.to_key(job_id)))
    }

    pub async fn take_lock(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let queue = self.queue()?;
        let job_id = self.id.as_ref().ok_or("job id is missing")?;
        queue.commands().take_lock(queue, job_id).await
    }

    pub async fn release_lock(&self) -> Result<(), Box<dyn std::error::Error>> {
        let queue = self.queue()?;
        let job_id = self.id.as_ref().ok_or("job id is missing")?;
        let unlocked = queue.commands().release_lock(queue, job_id).await?;
        if !unlocked {
            return Err(format!("Could not release lock for job {}", job_id).into());
        }
        Ok(())
    }

    pub async fn extend_lock(&self, duration: u64) -> Result<bool, Box<dyn std::error::Error>> {
        let queue = self.queue()?;
        let job_id = self.id.as_ref().ok_or("job id is missing")?;
        queue.commands().extend_lock(queue, job_id, duration).await
    }

    pub async fn progress(&mut self, progress: Option<&str>) -> Result<Option<String>, Box<dyn std::error::Error>> {
        if progress.is_none() {
            return Ok(self.progress.clone());
        }
        let queue = self.queue()?.clone();
        let job_id = self.id.clone().ok_or("job id is missing")?;
        let value = progress.unwrap().to_string();
        self.progress = Some(value.clone());
        queue.commands().update_progress(queue.as_ref(), &job_id, &value).await?;
        Ok(self.progress.clone())
    }

    pub async fn update(&mut self, data: &str) -> Result<(), Box<dyn std::error::Error>> {
        let queue = self.queue()?.clone();
        let job_id = self.id.clone().ok_or("job id is missing")?;
        self.data = data.to_string();
        let mut values = HashMap::new();
        values.insert("data".to_string(), data.to_string());
        queue.redis_client().hset::<(), _, _>(queue.to_key(&job_id), values).await?;
        Ok(())
    }

    pub async fn move_to_completed(&mut self, return_value: &str, ignore_lock: bool, not_fetch: bool) -> Result<(), Box<dyn std::error::Error>> {
        let queue = self.queue()?.clone();
        let job_id = self.id.clone().ok_or("job id is missing")?;
        self.returnvalue = Some(return_value.to_string());
        self.finished_on = Some(Utc::now().timestamp_millis());
        let keep_jobs = keep_jobs_from_option(self.opts.as_ref().and_then(|opts| opts.remove_on_complete));
        queue.commands().move_to_completed_with_opts(queue.as_ref(), &job_id, return_value, keep_jobs, ignore_lock, not_fetch).await
    }

    pub fn discard(&mut self) {
        self.discarded = true;
    }

    pub async fn move_to_failed(&mut self, err: &str, ignore_lock: bool) -> Result<MoveToFailedResult, Box<dyn std::error::Error>> {
        let queue = self.queue()?.clone();
        let job_id = self.id.clone().ok_or("job id is missing")?;

        self.failed_reason = Some(err.to_string());
        self.attempts_made += 1;
        self.save_attempt(&queue, &job_id, err).await?;

        let opts = self.opts.clone().unwrap_or_default();
        let should_retry = self.attempts_made < opts.attempts && !self.discarded;

        if should_retry {
            let delay = calculate_backoff_delay(opts.backoff.as_ref(), self.attempts_made, queue.settings.backoff_strategies(), err)?;
            match delay {
                Some(delay) if delay < 0 => {
                    // fall through to move to failed
                }
                Some(delay) if delay > 0 => {
                    let result = queue.commands().move_to_delayed(queue.as_ref(), &job_id, Utc::now().timestamp_millis() + delay, ignore_lock).await?;
                    match result {
                        0 => return Ok(MoveToFailedResult::Delayed),
                        -1 => return Err(format!("Missing Job {} when trying to move from active to delayed", job_id).into()),
                        -2 => return Err(format!("Job {} was locked when trying to move from active to delayed", job_id).into()),
                        _ => return Err(format!("Failed to move Job {} to delayed", job_id).into()),
                    }
                }
                _ => {
                    let lifo = opts.lifo;
                    let result = queue.commands().retry_job(queue.as_ref(), &job_id, lifo, ignore_lock).await?;
                    match result {
                        0 => return Ok(MoveToFailedResult::Retried),
                        -1 => return Err(format!("Missing key for job {} retry", job_id).into()),
                        -2 => return Err(format!("Job {} was locked when trying to retry", job_id).into()),
                        _ => return Err(format!("Failed to retry job {}", job_id).into()),
                    }
                }
            }
        }

        self.finished_on = Some(Utc::now().timestamp_millis());
        let keep_jobs = keep_jobs_from_option(self.opts.as_ref().and_then(|opts| opts.remove_on_fail));
        queue.commands().move_to_failed_with_opts(queue.as_ref(), &job_id, err, keep_jobs, ignore_lock).await?;
        Ok(MoveToFailedResult::Failed)
    }

    pub async fn move_to_delayed(&self, timestamp: i64, ignore_lock: bool) -> Result<i64, Box<dyn std::error::Error>> {
        let queue = self.queue()?.clone();
        let job_id = self.id.clone().ok_or("job id is missing")?;
        queue.commands().move_to_delayed(queue.as_ref(), &job_id, timestamp, ignore_lock).await
    }

    pub async fn promote(&self) -> Result<(), Box<dyn std::error::Error>> {
        let queue = self.queue()?.clone();
        let job_id = self.id.clone().ok_or("job id is missing")?;
        let result = queue.commands().promote(queue.as_ref(), &job_id).await?;
        if result == -1 {
            return Err(format!("Job {} is not in a delayed state", job_id).into());
        }
        Ok(())
    }

    pub async fn retry(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let queue = self.queue()?.clone();
        let job_id = self.id.clone().ok_or("job id is missing")?;
        self.failed_reason = None;
        self.finished_on = None;
        self.processed_on = None;
        self.retried_on = Some(Utc::now().timestamp_millis());

        let lifo = self.opts.as_ref().map(|opts| opts.lifo).unwrap_or(false);
        let result = queue.commands().reprocess_job(queue.as_ref(), &job_id, "failed", lifo).await?;
        match result {
            1 => Ok(()),
            0 => Err("Retry job does not exist".into()),
            -1 => Err("Retry job is locked".into()),
            -2 => Err("Retry job not failed".into()),
            _ => Err("Retry job failed".into()),
        }
    }

    pub async fn log(&self, log_row: &str) -> Result<(), Box<dyn std::error::Error>> {
        let queue = self.queue()?.clone();
        let job_id = self.id.clone().ok_or("job id is missing")?;
        let logs_key = format!("{}:logs", queue.to_key(&job_id));
        queue.redis_client().rpush::<(), _, _>(logs_key, log_row).await?;
        Ok(())
    }

    pub async fn is_completed(&self) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(self.is_done("completed").await?)
    }

    pub async fn is_failed(&self) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(self.is_done("failed").await?)
    }

    pub async fn is_delayed(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let queue = self.queue()?.clone();
        let job_id = self.id.clone().ok_or("job id is missing")?;
        let score: Option<f64> = queue.redis_client().zscore(queue.to_key("delayed"), job_id).await?;
        Ok(score.is_some())
    }

    pub async fn is_active(&self) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(self.is_in_list("active").await?)
    }

    pub async fn is_waiting(&self) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(self.is_in_list("wait").await?)
    }

    pub async fn is_paused(&self) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(self.is_in_list("paused").await?)
    }

    pub async fn is_stuck(&self) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(self.get_state().await? == "stuck")
    }

    pub fn is_discarded(&self) -> bool {
        self.discarded
    }

    pub async fn get_state(&self) -> Result<String, Box<dyn std::error::Error>> {
        if self.is_completed().await? {
            return Ok("completed".to_string());
        }
        if self.is_failed().await? {
            return Ok("failed".to_string());
        }
        if self.is_delayed().await? {
            return Ok("delayed".to_string());
        }
        if self.is_active().await? {
            return Ok("active".to_string());
        }
        if self.is_waiting().await? {
            return Ok("waiting".to_string());
        }
        if self.is_paused().await? {
            return Ok("paused".to_string());
        }
        Ok("stuck".to_string())
    }

    pub async fn remove(&self) -> Result<(), Box<dyn std::error::Error>> {
        let queue = self.queue()?.clone();
        let job_id = self.id.clone().ok_or("job id is missing")?;
        let removed = queue.commands().remove_job(queue.as_ref(), &job_id).await?;
        if removed == 1 {
            queue.emit_event(QueueEvent::Removed(job_id.to_string()));
            Ok(())
        } else {
            Err(format!("Could not remove job {}", job_id).into())
        }
    }

    pub async fn finished(&self) -> Result<String, Box<dyn std::error::Error>> {
        let queue = self.queue()?.clone();
        let job_id = self.id.clone().ok_or("job id is missing")?;
        let mut receiver = queue.subscribe();

        if let Some(result) = check_finished(&queue, &job_id).await? {
            return result;
        }

        let mut watchdog = interval(Duration::from_millis(FINISHED_WATCHDOG));
        loop {
            tokio::select! {
                _ = watchdog.tick() => {
                    if let Some(result) = check_finished(&queue, &job_id).await? {
                        return result;
                    }
                }
                event = receiver.recv() => {
                    match event {
                        Ok(QueueEvent::Completed(id, value)) if id == job_id => return Ok(value),
                        Ok(QueueEvent::Failed(id, reason)) if id == job_id => return Err(reason.into()),
                        Ok(QueueEvent::GlobalCompleted(id, value)) if id == job_id => return Ok(value),
                        Ok(QueueEvent::GlobalFailed(id, reason)) if id == job_id => return Err(reason.into()),
                        Ok(_) => {}
                        Err(_) => {}
                    }
                }
            }
        }
    }

    fn queue(&self) -> Result<&Arc<Queue>, Box<dyn std::error::Error>> {
        self.queue.as_ref().ok_or_else(|| "Job queue is not set".into())
    }

    async fn is_done(&self, list: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let queue = self.queue()?.clone();
        let job_id = self.id.clone().ok_or("job id is missing")?;
        let score: Option<f64> = queue.redis_client().zscore(queue.to_key(list), job_id).await?;
        Ok(score.is_some())
    }

    async fn is_in_list(&self, list: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let queue = self.queue()?.clone();
        let job_id = self.id.clone().ok_or("job id is missing")?;
        let result = queue.commands().is_job_in_list(queue.as_ref(), list, &job_id).await?;
        Ok(result == 1)
    }
}

#[derive(Debug, Clone)]
pub struct JobRecord {
    pub id: String,
    pub name: String,
    pub data: String,
    pub opts: Value,
    pub timestamp: Option<i64>,
    pub delay: Option<i64>,
    pub priority: Option<i64>,
}

impl JobOpts {
    pub fn from_value(value: Value) -> JobOpts {
        let mut opts = JobOpts::default();
        if let Value::Object(map) = value {
            if let Some(value) = map.get("priority").and_then(|val| val.as_u64()) {
                opts.priority = value as u32;
            }
            if let Some(value) = map.get("lifo").and_then(|val| val.as_bool()) {
                opts.lifo = value;
            }
            if let Some(value) = map.get("attempts").and_then(|val| val.as_u64()) {
                opts.attempts = value as u32;
            }
            if let Some(value) = map.get("timeout").and_then(|val| val.as_u64()) {
                opts.timeout = Some(value as u64);
            }
            if let Some(value) = map.get("jobId").and_then(|val| val.as_str()) {
                opts.custom_job_id = value.to_string();
            }
            if let Some(value) = map.get("removeOnComplete") {
                opts.remove_on_complete = remove_on_from_value(value);
            }
            if let Some(value) = map.get("removeOnFail") {
                opts.remove_on_fail = remove_on_from_value(value);
            }
            if let Some(value) = map.get("backoff") {
                opts.backoff = backoff_from_value(value);
            }
            if let Some(value) = map.get("stackTraceLimit").and_then(|val| val.as_u64()) {
                opts.stack_trace_limit = Some(value as usize);
            }
            if let Some(value) = map.get("prevMillis").and_then(|val| val.as_i64()) {
                opts.prev_millis = Some(value);
            }
            if let Some(value) = map.get("repeat") {
                opts.repeat = RepeatOpts::from_value(value.clone());
            }
        }
        opts
    }
}

impl RepeatOpts {
    pub fn from_value(value: Value) -> Option<RepeatOpts> {
        let Value::Object(map) = value else {
            return None;
        };

        let every = map.get("every").and_then(|val| val.as_i64());
        let cron = map.get("cron").and_then(|val| val.as_str()).map(|val| val.to_string());
        let tz = map.get("tz").and_then(|val| val.as_str()).map(|val| val.to_string());
        let limit = map.get("limit").and_then(|val| val.as_u64()).map(|val| val as u32);
        let count = map.get("count").and_then(|val| val.as_u64()).map(|val| val as u32);
        let key = map.get("key").and_then(|val| val.as_str()).map(|val| val.to_string());
        let job_id = map.get("jobId").and_then(|val| val.as_str()).map(|val| val.to_string());

        let end_date = parse_repeat_date(map.get("endDate"));
        let start_date = parse_repeat_date(map.get("startDate"));

        Some(RepeatOpts {
            every,
            cron,
            tz,
            end_date,
            start_date,
            limit,
            count,
            key,
            job_id,
        })
    }
}

fn parse_repeat_date(value: Option<&Value>) -> Option<i64> {
    let value = value?;
    if let Some(number) = value.as_i64() {
        return Some(number);
    }
    if let Some(value) = value.as_str() {
        if let Ok(number) = value.parse::<i64>() {
            return Some(number);
        }
        if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
            return Some(parsed.timestamp_millis());
        }
        if let Ok(parsed) = DateTime::<FixedOffset>::parse_from_str(value, "%Y-%m-%d %H:%M:%S%z") {
            return Some(parsed.timestamp_millis());
        }
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveToFailedResult {
    Failed,
    Retried,
    Delayed,
}

impl Job {
    async fn save_attempt(&mut self, queue: &Arc<Queue>, job_id: &str, err: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut params: HashMap<String, String> = HashMap::new();
        params.insert("attemptsMade".to_string(), self.attempts_made.to_string());

        if let Some(limit) = self.opts.as_ref().and_then(|opts| opts.stack_trace_limit) {
            if limit > 0 {
                let keep = limit.saturating_sub(1);
                if keep == 0 {
                    self.stacktrace.clear();
                } else if self.stacktrace.len() > keep {
                    self.stacktrace = self.stacktrace.split_off(self.stacktrace.len() - keep);
                }
            }
        }

        self.stacktrace.push(err.to_string());
        params.insert("stacktrace".to_string(), serde_json::to_string(&self.stacktrace).unwrap_or_else(|_| "[]".to_string()));
        params.insert("failedReason".to_string(), err.to_string());

        queue.redis_client().hset::<(), _, _>(queue.to_key(job_id), params).await?;
        Ok(())
    }
}

fn backoff_from_value(value: &Value) -> Option<BackoffConfig> {
    match value {
        Value::Number(number) => number.as_i64().map(BackoffConfig::fixed),
        Value::Object(map) => {
            let backoff_type = map.get("type").and_then(|val| val.as_str())?.to_string();
            let delay = map.get("delay").and_then(|val| val.as_i64()).unwrap_or(0);
            let options = map.get("options").cloned();
            Some(BackoffConfig { backoff_type, delay, options })
        }
        _ => None,
    }
}

fn calculate_backoff_delay(
    backoff: Option<&BackoffConfig>,
    attempts_made: u32,
    strategies: &HashMap<String, BackoffStrategy>,
    err: &str,
) -> Result<Option<i64>, Box<dyn std::error::Error>> {
    let Some(backoff) = backoff else {
        return Ok(None);
    };

    if let Some(strategy) = strategies.get(backoff.backoff_type.as_str()) {
        let delay = (strategy)(attempts_made, err, backoff.options.as_ref());
        return Ok(Some(delay));
    }

    match backoff.backoff_type.as_str() {
        "fixed" => Ok(Some(backoff.delay)),
        "exponential" => {
            let delay = ((2_i64.pow(attempts_made) - 1) * backoff.delay).max(0);
            Ok(Some(delay))
        }
        _ => Err(format!(
            "Unknown backoff strategy {}. If a custom backoff strategy is used, specify it when the queue is created.",
            backoff.backoff_type
        ).into()),
    }
}

fn remove_on_from_value(value: &Value) -> Option<RemoveOn> {
    match value {
        Value::Bool(flag) => Some(RemoveOn::Enabled(*flag)),
        Value::Number(number) => number.as_i64().map(RemoveOn::Count),
        _ => None,
    }
}

fn keep_jobs_from_option(value: Option<RemoveOn>) -> crate::commands::KeepJobsConfig {
    match value {
        Some(RemoveOn::Count(count)) => crate::commands::KeepJobsConfig { count, age: None },
        Some(RemoveOn::Enabled(true)) => crate::commands::KeepJobsConfig { count: 0, age: None },
        Some(RemoveOn::Enabled(false)) => crate::commands::KeepJobsConfig { count: -1, age: None },
        None => crate::commands::KeepJobsConfig { count: -1, age: None },
    }
}

async fn check_finished(queue: &Arc<Queue>, job_id: &str) -> Result<Option<Result<String, Box<dyn std::error::Error>>>, Box<dyn std::error::Error>> {
    let status = queue.commands().is_finished(queue.as_ref(), job_id).await?;
    if status == 1 {
        if let Some(job) = Job::from_id(queue.clone(), job_id).await? {
            return Ok(Some(Ok(job.returnvalue.unwrap_or_default())));
        }
        return Ok(Some(Ok(String::new())));
    }
    if status == 2 {
        if let Some(job) = Job::from_id(queue.clone(), job_id).await? {
            let reason = job.failed_reason.unwrap_or_else(|| "Job failed".to_string());
            return Ok(Some(Err(reason.into())));
        }
        return Ok(Some(Err("Job failed".into())));
    }
    Ok(None)
}

impl JobRecord {
    pub fn from_redis(job_id: &str, raw: Vec<String>) -> Option<JobRecord> {
        if raw.is_empty() {
            return None;
        }

        let mut map = HashMap::new();
        for chunk in raw.chunks(2) {
            if let [key, value] = chunk {
                map.insert(key.to_string(), value.to_string());
            }
        }

        let name = map.get("name")?.to_string();
        let data = map.get("data").cloned().unwrap_or_default();
        let opts = map
            .get("opts")
            .and_then(|value| serde_json::from_str::<Value>(value).ok())
            .unwrap_or(Value::Null);

        let timestamp = map.get("timestamp").and_then(|value| value.parse::<i64>().ok());
        let delay = map.get("delay").and_then(|value| value.parse::<i64>().ok());
        let priority = map.get("priority").and_then(|value| value.parse::<i64>().ok());

        Some(JobRecord {
            id: job_id.to_string(),
            name,
            data,
            opts,
            timestamp,
            delay,
            priority,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, timeout, Duration};

    const REDIS_URL: &str = "redis://0.0.0.0:6379";

    async fn build_queue() -> Result<Arc<Queue>, Box<dyn std::error::Error>> {
        Queue::new_with_opts(&format!("test-queue-{}", uuid::Uuid::new_v4()), REDIS_URL, crate::queue::QueueOptions::default()).await
    }

    async fn activate_job(queue: &Arc<Queue>, job_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let _ = queue.get_next_job().await?;
        let _ = queue.move_to_active(Some(job_id.to_string())).await?;
        Ok(())
    }

    #[tokio::test]
    async fn new_job() -> Result<(), Box<dyn std::error::Error>> {
        let job = Job::new("new data");
        dbg!(job);
        Ok(())
    }

    #[tokio::test]
    async fn create_and_from_id_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let job = Job::create(queue.clone(), "test", r#"{"a": 1}"#, None).await?;
        let job_id = job.id.clone().unwrap();
        let fetched = Job::from_id(queue.clone(), &job_id).await?.unwrap();
        assert_eq!(fetched.id.as_deref(), Some(job_id.as_str()));
        Ok(())
    }

    #[tokio::test]
    async fn create_bulk_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let jobs = vec![Job::new("a"), Job::new("b")];
        let results = Job::create_bulk(queue, jobs).await?;
        assert_eq!(results.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn lock_methods_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let job = Job::create(queue.clone(), "test", "{}", None).await?;
        let job_id = job.id.clone().unwrap();
        let locked = job.take_lock().await?;
        assert!(locked);
        let extended = job.extend_lock(5000).await?;
        assert!(extended);
        job.release_lock().await?;
        let _ = queue.commands().release_lock(queue.as_ref(), &job_id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn progress_update_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let mut job = Job::create(queue.clone(), "test", "{}", None).await?;
        let _ = job.progress(Some("50")).await?;
        let progress = job.progress(None).await?;
        assert_eq!(progress.as_deref(), Some("50"));
        job.update("{\"b\":2}").await?;
        let fetched = Job::from_id(queue, job.id.as_ref().unwrap()).await?.unwrap();
        assert_eq!(fetched.data, "{\"b\":2}");
        Ok(())
    }

    #[tokio::test]
    async fn move_to_completed_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let mut job = Job::create(queue.clone(), "test", "{}", None).await?;
        let job_id = job.id.clone().unwrap();
        activate_job(&queue, &job_id).await?;
        job.move_to_completed("ok", false, true).await?;
        assert!(job.is_completed().await?);
        Ok(())
    }

    #[tokio::test]
    async fn move_to_failed_and_retry_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let mut job = Job::create(queue.clone(), "test", "{}", None).await?;
        let job_id = job.id.clone().unwrap();
        activate_job(&queue, &job_id).await?;
        let result = job.move_to_failed("boom", false).await?;
        assert_eq!(result, MoveToFailedResult::Failed);
        assert!(job.is_failed().await?);
        job.retry().await?;
        Ok(())
    }

    #[tokio::test]
    async fn delayed_promote_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let job = Job::create(queue.clone(), "test", "{}", None).await?;
        let job_id = job.id.clone().unwrap();
        activate_job(&queue, &job_id).await?;
        job.move_to_delayed(Utc::now().timestamp_millis() + 1000, false).await?;
        assert!(job.is_delayed().await?);
        job.promote().await?;
        Ok(())
    }

    #[tokio::test]
    async fn backoff_retry_immediate_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let mut opts = JobOpts::default();
        opts.attempts = 2;
        opts.backoff = Some(BackoffConfig::fixed(0));
        let mut job = Job::create(queue.clone(), "test", "{}", Some(opts)).await?;
        let job_id = job.id.clone().unwrap();
        activate_job(&queue, &job_id).await?;

        let result = job.move_to_failed("boom", false).await?;
        assert_eq!(result, MoveToFailedResult::Retried);
        assert!(!job.is_failed().await?);
        assert!(job.is_waiting().await?);
        Ok(())
    }

    #[tokio::test]
    async fn backoff_retry_delayed_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let mut opts = JobOpts::default();
        opts.attempts = 2;
        opts.backoff = Some(BackoffConfig::fixed(1000));
        let mut job = Job::create(queue.clone(), "test", "{}", Some(opts)).await?;
        let job_id = job.id.clone().unwrap();
        activate_job(&queue, &job_id).await?;

        let result = job.move_to_failed("boom", false).await?;
        assert_eq!(result, MoveToFailedResult::Delayed);
        assert!(job.is_delayed().await?);
        Ok(())
    }

    #[tokio::test]
    async fn log_and_state_checks_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let job = Job::create(queue.clone(), "test", "{}", None).await?;
        job.log("log-row").await?;
        assert!(job.is_waiting().await?);
        queue.pause(false).await?;
        let paused_job = Job::create(queue.clone(), "test", "{}", None).await?;
        assert!(paused_job.is_paused().await?);
        queue.resume(false).await?;
        Ok(())
    }

    #[tokio::test]
    async fn finished_remove_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let mut job = Job::create(queue.clone(), "test", "{}", None).await?;
        let job_id = job.id.clone().unwrap();
        activate_job(&queue, &job_id).await?;
        job.move_to_completed("done", false, true).await?;
        let result = job.finished().await?;
        assert_eq!(result, "done");

        let removable = Job::create(queue.clone(), "test", "{}", None).await?;
        removable.remove().await?;
        let state = removable.get_state().await?;
        assert_eq!(state, "stuck");
        Ok(())
    }

    #[tokio::test]
    async fn finished_waits_for_completion_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = build_queue().await?;
        let handle = queue.clone().process(|data: String| async move {
            sleep(Duration::from_millis(50)).await;
            data
        }).await?;

        let job = Job::create(queue.clone(), "test", "ok", None).await?;
        let result = timeout(Duration::from_secs(2), job.finished()).await??;
        assert_eq!(result, "ok");

        handle.close().await?;
        Ok(())
    }

    #[test]
    fn job_opts_remove_on_parsing_case_1() {
        let value = serde_json::json!({
            "removeOnComplete": true,
            "removeOnFail": 5
        });
        let opts = JobOpts::from_value(value);
        match opts.remove_on_complete {
            Some(RemoveOn::Enabled(true)) => {}
            _ => panic!("expected remove_on_complete true"),
        }
        match opts.remove_on_fail {
            Some(RemoveOn::Count(5)) => {}
            _ => panic!("expected remove_on_fail count"),
        }
    }

    #[test]
    fn discard_case_1() {
        let mut job = Job::new("data");
        job.discard();
        assert!(job.is_discarded());
    }
}
