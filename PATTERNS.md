# Patterns (Rust)

Common Bull patterns translated for the Rust port.

- [Message Queue](#message-queue)
- [Returning Job Completions](#returning-job-completions)
- [Redis Cluster](#redis-cluster)
- [Debugging](#debugging)
- [Custom backoff strategy](#custom-backoff-strategy)
- [Manually fetching jobs](#manually-fetching-jobs)

## Message Queue

Use `Queue::add` to send and `Queue::process` to receive.

Server A:

```rust
use bullrs::queue::Queue;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let send_queue = Queue::new("server-b", "redis://127.0.0.1:6379").await?;
    let receive_queue = Queue::new("server-a", "redis://127.0.0.1:6379").await?;

    let handle = receive_queue.clone().process(|data: String| async move {
        println!("Received message: {}", data);
        String::new()
    }).await?;

    send_queue.add(r#"{\"msg\":\"Hello\"}"#).await?;

    handle.close().await?;
    Ok(())
}
```

Server B:

```rust
use bullrs::queue::Queue;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let send_queue = Queue::new("server-a", "redis://127.0.0.1:6379").await?;
    let receive_queue = Queue::new("server-b", "redis://127.0.0.1:6379").await?;

    let handle = receive_queue.clone().process(|data: String| async move {
        println!("Received message: {}", data);
        String::new()
    }).await?;

    send_queue.add(r#"{\"msg\":\"World\"}"#).await?;

    handle.close().await?;
    Ok(())
}
```

## Returning Job Completions

Use a results queue (message queue pattern) or listen to completion events.

```rust
use bullrs::queue::Queue;
use bullrs::queue::QueueEvent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let work_queue = Queue::new("work", "redis://127.0.0.1:6379").await?;
    let results_queue = Queue::new("results", "redis://127.0.0.1:6379").await?;

    let worker = work_queue.clone().process(|data: String| async move {
        format!("processed: {}", data)
    }).await?;

    let mut events = work_queue.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            if let QueueEvent::Completed(job_id, result) = event {
                let payload = format!("{{\"job_id\":\"{}\",\"result\":{}}}", job_id, result);
                let _ = results_queue.add(&payload).await;
            }
        }
    });

    work_queue.add("input").await?;
    worker.close().await?;
    Ok(())
}
```

## Redis Cluster

Bull operations require multiple keys to be in the same hash slot. Use a hash tag
in the queue prefix to co-locate keys:

```rust
use bullrs::queue::{Queue, QueueOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut opts = QueueOptions::default();
    opts.prefix = Some("{myprefix}".to_string());

    let _queue = Queue::new_with_opts("cluster", "redis://127.0.0.1:6379", opts).await?;
    Ok(())
}
```

## Debugging

Use Redis MONITOR to watch commands:

```bash
docker-compose -f docker/redis.yaml up -d
docker exec -it bull_rs_redis_1 redis-cli monitor
```

## Custom backoff strategy

Register custom strategies in `QueueSettings::backoff_strategies`. The function
receives `(attempts_made, error, options)` and returns a delay in milliseconds.
Return `0` to retry immediately or a negative value to fail immediately.

```rust
use std::collections::HashMap;
use std::sync::Arc;
use bullrs::queue::{Queue, QueueOptions, QueueSettings};
use bullrs::job::{BackoffConfig, JobOpts};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut strategies: HashMap<String, Arc<dyn Fn(u32, &str, Option<&serde_json::Value>) -> i64 + Send + Sync>> = HashMap::new();
    strategies.insert("jitter".to_string(), Arc::new(|attempts, _err, _opts| {
        5000 + (attempts as i64 * 50)
    }));

    let settings = QueueSettings {
        backoff_strategies: strategies,
        ..QueueSettings::default()
    };

    let mut opts = QueueOptions::default();
    opts.settings = Some(settings);

    let queue = Queue::new_with_opts("backoff", "redis://127.0.0.1:6379", opts).await?;

    let mut job_opts = JobOpts::default();
    job_opts.attempts = 3;
    job_opts.backoff = Some(BackoffConfig { backoff_type: "jitter".to_string(), delay: 0, options: None });

    queue.add_with_opts("payload", job_opts).await?;
    Ok(())
}
```

## Manually fetching jobs

You can fetch jobs manually when you want custom scheduling or a separate worker
service. The flow is:

1. Add a job to the queue.
2. Fetch the next job id with `Queue::get_next_job`.
3. Move it to active with `Queue::move_to_active` to get its data.
4. Use `JobRecord::from_redis` to deserialize the payload.
5. Finish with `Job::move_to_completed` or `Job::move_to_failed`.

```rust
use bullrs::queue::Queue;
use bullrs::job::{Job, JobRecord};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Queue::new("manual", "redis://127.0.0.1:6379").await?;
    queue.add("work").await?;

    if let Some(job_id) = queue.get_next_job().await? {
        if let Some((job_data, active_id)) = queue.move_to_active(Some(job_id)).await? {
            if let Some(record) = JobRecord::from_redis(&active_id, job_data) {
                // Do work with record.data
                let mut job = Job::from_id(queue.clone(), &record.id).await?
                    .ok_or("job missing")?;
                job.move_to_completed("ok", false, true).await?;
            }
        }
    }

    Ok(())
}
```

If your job takes longer than the lock duration, use `Job::extend_lock` to
keep it alive before the lock expires.
