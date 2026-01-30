use bullrs::queue::{Queue, QueueOptions, QueueSettings, RateLimiter, MetricsOptions, QueueEvent};
use bullrs::job::{Job, JobOpts, RepeatOpts, BackoffConfig, RemoveOn};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("bullrs main: complex");

    let mut backoff_strategies: HashMap<String, Arc<dyn Fn(u32, &str, Option<&serde_json::Value>) -> i64 + Send + Sync>> = HashMap::new();
    backoff_strategies.insert("custom".to_string(), Arc::new(|attempts, _err, options| {
        let base = options
            .and_then(|value| value.get("base"))
            .and_then(|value| value.as_i64())
            .unwrap_or(250);
        base * attempts as i64
    }));

    let settings = QueueSettings {
        lock_duration: 20000,
        lock_renew_time: 10000,
        stalled_interval: 5000,
        max_stalled_count: 2,
        guard_interval: 8000,
        retry_process_delay: 2000,
        drain_delay: 2,
        child_pool_size: 2,
        backoff_strategies,
    };

    let default_job_options = JobOpts {
        attempts: 2,
        backoff: Some(BackoffConfig::fixed(0)),
        remove_on_complete: Some(RemoveOn::Count(50)),
        remove_on_fail: Some(RemoveOn::Enabled(true)),
        stack_trace_limit: Some(5),
        ..JobOpts::default()
    };

    let limiter = RateLimiter {
        max: 100,
        duration: 1000,
        bounce_back: true,
        group_key: Some("group".to_string()),
    };

    let opts = QueueOptions {
        prefix: Some("bullrs".to_string()),
        limiter: Some(limiter),
        default_job_options: Some(default_job_options),
        settings: Some(settings),
        metrics: Some(MetricsOptions { max_data_points: 60 }),
    };

    let queue_name = format!("complex-{}", Uuid::new_v4());
    let queue = Queue::new_with_opts(&queue_name, "redis://localhost:26379", opts).await?;

    queue.set_handler("email", |job| async move {
        println!("email job: {}", job.data);
        Ok(format!("email:{}", job.id))
    }).await?;

    queue.set_handler("report", |job| async move {
        println!("report job: {}", job.data);
        Ok("report:ok".to_string())
    }).await?;

    queue.set_handler("*", |job| async move {
        println!("default job: {}", job.data);
        Ok("default:ok".to_string())
    }).await?;

    if let Ok(path) = std::env::var("BULLRS_PROCESS_FILE") {
        let _ = queue.set_handler_file("sandbox", &path).await?;
    }

    let mut events = queue.subscribe();
    let events_handle = tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            match event {
                QueueEvent::Completed(job_id, result) => println!("completed {} => {}", job_id, result),
                QueueEvent::Failed(job_id, reason) => println!("failed {} => {}", job_id, reason),
                QueueEvent::GlobalCompleted(job_id, result) => println!("global completed {} => {}", job_id, result),
                QueueEvent::GlobalFailed(job_id, reason) => println!("global failed {} => {}", job_id, reason),
                QueueEvent::Paused => println!("paused"),
                QueueEvent::Resumed => println!("resumed"),
                QueueEvent::Drained => println!("drained"),
                _ => {}
            }
        }
    });

    let handle = queue.clone().run("*", 2).await?;

    let payload = json!({"group":"alpha","msg":"hello"}).to_string();
    let repeat_opts = RepeatOpts {
        every: Some(3000),
        cron: None,
        tz: None,
        end_date: None,
        start_date: None,
        limit: Some(3),
        count: None,
        key: None,
        job_id: Some("repeat-alpha".to_string()),
    };

    let job_opts = JobOpts {
        custom_job_id: "complex:1".to_string(),
        priority: 1,
        lifo: true,
        attempts: 3,
        timeout: Some(2000),
        backoff: Some(BackoffConfig {
            backoff_type: "custom".to_string(),
            delay: 0,
            options: Some(json!({"base": 250})),
        }),
        remove_on_complete: Some(RemoveOn::Count(20)),
        remove_on_fail: Some(RemoveOn::Enabled(true)),
        stack_trace_limit: Some(10),
        repeat: Some(repeat_opts),
        ..JobOpts::default()
    };

    let repeat_job_ids = queue.add_named_with_opts("email", &payload, job_opts).await?;
    println!("repeat job ids: {:?}", repeat_job_ids);

    queue.add("plain job").await?;

    let mut bulk_one = Job::new(&json!({"group":"alpha","task":"bulk-1"}).to_string());
    bulk_one.name = "report".to_string();
    bulk_one.opts = Some(JobOpts { priority: 2, ..JobOpts::default() });

    let mut bulk_two = Job::new(&json!({"group":"beta","task":"bulk-2"}).to_string());
    bulk_two.name = "report".to_string();
    bulk_two.opts = Some(JobOpts { priority: 3, ..JobOpts::default() });

    let bulk_ids = queue.add_bulk(vec![bulk_one, bulk_two]).await?;
    println!("bulk job ids: {:?}", bulk_ids);

    let mut created = Job::create(queue.clone(), "report", &json!({"group":"beta","msg":"created"}).to_string(), None).await?;
    created.progress(Some("10")).await?;
    created.log("job created").await?;
    created.update(&json!({"group":"beta","msg":"updated"}).to_string()).await?;

    let job_id = created.id.clone().unwrap_or_default();
    let logs = queue.get_job_logs(&job_id, Some(0), Some(50), true).await?;
    println!("logs count: {}", logs.count);

    let _job = queue.get_job(&job_id).await?;
    let _jobs = queue.get_jobs(&["waiting", "active", "completed", "failed", "delayed"], Some(0), Some(50), true).await?;

    let counts = queue.get_job_counts(&["waiting", "active", "completed", "failed", "delayed", "paused", "stalled"]).await?;
    println!("counts: {:?}", counts);

    let _waiting = queue.get_waiting(Some(0), Some(10)).await?;
    let _active = queue.get_active(Some(0), Some(10)).await?;
    let _delayed = queue.get_delayed(Some(0), Some(10)).await?;
    let _completed = queue.get_completed(Some(0), Some(10)).await?;
    let _failed = queue.get_failed(Some(0), Some(10)).await?;

    let _total = queue.count().await?;
    let _completed_count = queue.get_completed_count().await?;
    let _failed_count = queue.get_failed_count().await?;
    let _delayed_count = queue.get_delayed_count().await?;
    let _active_count = queue.get_active_count().await?;
    let _waiting_count = queue.get_waiting_count().await?;
    let _paused_count = queue.get_paused_count().await?;
    let _stalled_count = queue.get_stalled_count().await?;

    let _metrics = queue.get_metrics("completed", Some(0), Some(-1)).await?;
    let _workers = queue.get_workers().await?;

    queue.pause(true).await?;
    queue.resume(true).await?;

    let repeatables = queue.get_repeatable_jobs(0, -1, true).await?;
    if let Some(first) = repeatables.first() {
        queue.remove_repeatable_by_key(&first.key).await?;
    }

    let _ = queue.remove_jobs("complex:*").await?;

    sleep(Duration::from_secs(2)).await;

    let _ = queue.retry_jobs(50).await?;
    let _ = queue.clean(1000, "completed", 100).await?;
    let _ = queue.empty().await?;

    handle.close().await?;
    events_handle.abort();
    Ok(())
}
