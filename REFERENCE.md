# Reference (Rust)

- [Queue](#queue)
  - [Queue::new](#queuenew)
  - [Queue::new_with_opts](#queuenew_with_opts)
  - [Queue::set_handler](#queueset_handler)
  - [Queue::set_handler_file](#queueset_handler_file)
  - [Queue::process](#queueprocess)
  - [Queue::process_file](#queueprocess_file)
  - [Queue::run](#queuerun)
  - [Queue::add](#queueadd)
  - [Queue::add_with_opts](#queueadd_with_opts)
  - [Queue::add_named_with_opts](#queueadd_named_with_opts)
  - [Queue::add_bulk](#queueadd_bulk)
  - [Queue::pause](#queuepause)
  - [Queue::is_paused](#queueis_paused)
  - [Queue::resume](#queueresume)
  - [Queue::retry_jobs](#queueretry_jobs)
  - [Queue::clean](#queueclean)
  - [Queue::obliterate](#queueobliterate)
  - [Queue::empty](#queueempty)
  - [Queue::remove_jobs](#queueremove_jobs)
  - [Queue::get_job](#queueget_job)
  - [Queue::get_jobs](#queueget_jobs)
  - [Queue::get_waiting](#queueget_waiting)
  - [Queue::get_active](#queueget_active)
  - [Queue::get_delayed](#queueget_delayed)
  - [Queue::get_completed](#queueget_completed)
  - [Queue::get_failed](#queueget_failed)
  - [Queue::get_job_counts](#queueget_job_counts)
  - [Queue::count](#queuecount)
  - [Queue::get_completed_count](#queueget_completed_count)
  - [Queue::get_failed_count](#queueget_failed_count)
  - [Queue::get_delayed_count](#queueget_delayed_count)
  - [Queue::get_active_count](#queueget_active_count)
  - [Queue::get_waiting_count](#queueget_waiting_count)
  - [Queue::get_stalled_count](#queueget_stalled_count)
  - [Queue::get_paused_count](#queueget_paused_count)
  - [Queue::get_job_logs](#queueget_job_logs)
  - [Queue::get_repeatable_jobs](#queueget_repeatable_jobs)
  - [Queue::get_repeatable_count](#queueget_repeatable_count)
  - [Queue::remove_repeatable](#queueremove_repeatable)
  - [Queue::remove_repeatable_by_key](#queueremove_repeatable_by_key)
  - [Queue::get_metrics](#queueget_metrics)
  - [Queue::get_workers](#queueget_workers)
  - [Queue::subscribe](#queuesubscribe)
- [Job](#job)
  - [Job::create](#jobcreate)
  - [Job::create_bulk](#jobcreate_bulk)
  - [Job::from_id](#jobfrom_id)
  - [Job::progress](#jobprogress)
  - [Job::log](#joblog)
  - [Job::get_state](#jobget_state)
  - [Job::update](#jobupdate)
  - [Job::remove](#jobremove)
  - [Job::retry](#jobretry)
  - [Job::discard](#jobdiscard)
  - [Job::promote](#jobpromote)
  - [Job::finished](#jobfinished)
  - [Job::move_to_completed](#jobmove_to_completed)
  - [Job::move_to_failed](#jobmove_to_failed)
  - [Job::move_to_delayed](#jobmove_to_delayed)
- [Events](#events)

## Queue

### Queue::new

```rust
Queue::new(queue_name: &str, redis_url: &str) -> Result<Arc<Queue>, Box<dyn Error>>
```

Creates a new queue backed by Redis. The same queue name will reuse existing
data (waiting, delayed, etc.).

`redis_url` is a Redis connection string, for example:
`redis://mypassword@myredis.server.com:1234`.

### Queue::new_with_opts

```rust
Queue::new_with_opts(queue_name: &str, redis_url: &str, opts: QueueOptions)
  -> Result<Arc<Queue>, Box<dyn Error>>
```

Queue options and settings:

```rust
pub struct QueueOptions {
  pub prefix: Option<String>,
  pub limiter: Option<RateLimiter>,
  pub default_job_options: Option<JobOpts>,
  pub settings: Option<QueueSettings>,
  pub metrics: Option<MetricsOptions>,
}

pub struct RateLimiter {
  pub max: u32,
  pub duration: u64,
  pub bounce_back: bool,
  pub group_key: Option<String>,
}

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

pub struct MetricsOptions {
  pub max_data_points: u32,
}
```

`BackoffStrategy` is a closure:

```rust
type BackoffStrategy = Arc<dyn Fn(u32, &str, Option<&Value>) -> i64 + Send + Sync>;
```

### Queue::set_handler

```rust
Queue::set_handler(name: &str, handler: impl Fn(JobRecord) -> Future<Output = Result<String, String>>)
  -> Result<(), Box<dyn Error>>
```

Registers a named handler for jobs. The handler returns `Ok(result)` for
completed jobs, or `Err(reason)` for failed jobs.

Handlers are matched by job name, then by the run handler name, then by `"*"`.

### Queue::set_handler_file

```rust
Queue::set_handler_file(name: &str, handler_path: &str) -> Result<(), Box<dyn Error>>
```

Registers a handler executed as a separate process. The child process is
invoked with stdin/stdout pipes. It receives one JSON line per job and should
write a single JSON line back.

Input payload:

```json
{"id":"...","name":"...","data":"...","opts":{...},"timestamp":...,"delay":...,"priority":...}
```

Output payload:

```json
{"status":"ok","result":"..."}
```

or

```json
{"status":"error","error":"..."}
```

Any other JSON value is treated as the string result.

### Queue::process

```rust
Queue::process(self: Arc<Queue>, func: impl Fn(String) -> Future<Output = String>)
  -> Result<QueueHandle, Box<dyn Error>>
```

Convenience helper that registers the default handler and starts processing
with concurrency = 1. The handler receives the job `data` string.

### Queue::process_file

```rust
Queue::process_file(self: Arc<Queue>, handler_path: &str)
  -> Result<QueueHandle, Box<dyn Error>>
```

Convenience helper that registers the default handler as a separate process
and starts processing with concurrency = 1.

### Queue::run

```rust
Queue::run(self: Arc<Queue>, handler_name: &str, concurrency: usize)
  -> Result<QueueHandle, Box<dyn Error>>
```

Starts processing jobs using a previously registered handler. If `concurrency`
is 0, it is treated as 1.

`QueueHandle` supports graceful shutdown:

```rust
QueueHandle::close(self) -> Result<(), Box<dyn Error>>
QueueHandle::stop(self, timeout: Duration) -> Result<(), Box<dyn Error>>
```

### Queue::add

```rust
Queue::add(&self, data: &str) -> Result<Vec<String>, Box<dyn Error>>
```

Creates a new job with the default name and default options.

### Queue::add_with_opts

```rust
Queue::add_with_opts(&self, data: &str, opts: JobOpts) -> Result<Vec<String>, Box<dyn Error>>
```

Creates a new job with the default name and provided options.

### Queue::add_named_with_opts

```rust
Queue::add_named_with_opts(&self, name: &str, data: &str, opts: JobOpts)
  -> Result<Vec<String>, Box<dyn Error>>
```

Creates a new job with a specific name. Only matching handlers will process it.

### Queue::add_bulk

```rust
Queue::add_bulk(&self, jobs: Vec<Job>) -> Result<Vec<Vec<String>>, Box<dyn Error>>
```

Creates multiple jobs in order. Each job is a `Job` instance built in memory.

### Queue::pause

```rust
Queue::pause(&self, is_local: bool) -> Result<(), Box<dyn Error>>
```

If `is_local` is true, pauses only this process. Otherwise, pauses the queue
globally for all workers.

### Queue::is_paused

```rust
Queue::is_paused(&self, is_local: bool) -> Result<bool, Box<dyn Error>>
```

Checks if the queue is paused, either locally or globally.

### Queue::resume

```rust
Queue::resume(&self, is_local: bool) -> Result<(), Box<dyn Error>>
```

Resumes the queue, either locally or globally.

### Queue::retry_jobs

```rust
Queue::retry_jobs(&self, count: usize) -> Result<(), Box<dyn Error>>
```

Retries failed jobs in batches of `count`.

### Queue::clean

```rust
Queue::clean(&self, grace: i64, set_type: &str, limit: usize)
  -> Result<Vec<String>, Box<dyn Error>>
```

Removes jobs in a specific state older than `grace` milliseconds. Supported
types: `completed`, `wait`, `active`, `paused`, `delayed`, `failed`.

### Queue::obliterate

```rust
Queue::obliterate(&self, force: bool, count: usize) -> Result<(), Box<dyn Error>>
```

Completely removes a queue with all its data. If `force` is false, the queue
must have no active jobs.

### Queue::empty

```rust
Queue::empty(&self) -> Result<(), Box<dyn Error>>
```

Removes waiting and delayed jobs only. Active, completed, failed, and
repeatable job data remain.

### Queue::remove_jobs

```rust
Queue::remove_jobs(&self, pattern: &str) -> Result<Vec<String>, Box<dyn Error>>
```

Removes jobs whose IDs match a Redis glob pattern (same syntax as `KEYS`).

### Queue::get_job

```rust
Queue::get_job(self: &Arc<Queue>, job_id: &str) -> Result<Option<Job>, Box<dyn Error>>
```

Fetches a single job by ID. Returns `None` if missing.

### Queue::get_jobs

```rust
Queue::get_jobs(self: &Arc<Queue>, types: &[&str], start: Option<i64>, end: Option<i64>, asc: bool)
  -> Result<Vec<Option<Job>>, Box<dyn Error>>
```

Fetches jobs for one or more states. The `start` and `end` bounds apply per
type, matching Bull semantics. Supported types include `wait`, `waiting`,
`active`, `completed`, `failed`, `delayed`, `paused`.

### Queue::get_waiting

```rust
Queue::get_waiting(self: &Arc<Queue>, start: Option<i64>, end: Option<i64>)
  -> Result<Vec<Option<Job>>, Box<dyn Error>>
```

### Queue::get_active

```rust
Queue::get_active(self: &Arc<Queue>, start: Option<i64>, end: Option<i64>)
  -> Result<Vec<Option<Job>>, Box<dyn Error>>
```

### Queue::get_delayed

```rust
Queue::get_delayed(self: &Arc<Queue>, start: Option<i64>, end: Option<i64>)
  -> Result<Vec<Option<Job>>, Box<dyn Error>>
```

### Queue::get_completed

```rust
Queue::get_completed(self: &Arc<Queue>, start: Option<i64>, end: Option<i64>)
  -> Result<Vec<Option<Job>>, Box<dyn Error>>
```

### Queue::get_failed

```rust
Queue::get_failed(self: &Arc<Queue>, start: Option<i64>, end: Option<i64>)
  -> Result<Vec<Option<Job>>, Box<dyn Error>>
```

### Queue::get_job_counts

```rust
Queue::get_job_counts(&self, types: &[&str]) -> Result<HashMap<String, i64>, Box<dyn Error>>
```

Returns counts for each provided type.

### Queue::count

```rust
Queue::count(&self) -> Result<i64, Box<dyn Error>>
```

Returns the number of waiting, paused, and delayed jobs.

### Queue::get_completed_count

```rust
Queue::get_completed_count(&self) -> Result<i64, Box<dyn Error>>
```

### Queue::get_failed_count

```rust
Queue::get_failed_count(&self) -> Result<i64, Box<dyn Error>>
```

### Queue::get_delayed_count

```rust
Queue::get_delayed_count(&self) -> Result<i64, Box<dyn Error>>
```

### Queue::get_active_count

```rust
Queue::get_active_count(&self) -> Result<i64, Box<dyn Error>>
```

### Queue::get_waiting_count

```rust
Queue::get_waiting_count(&self) -> Result<i64, Box<dyn Error>>
```

### Queue::get_stalled_count

```rust
Queue::get_stalled_count(&self) -> Result<i64, Box<dyn Error>>
```

### Queue::get_paused_count

```rust
Queue::get_paused_count(&self) -> Result<i64, Box<dyn Error>>
```

### Queue::get_job_logs

```rust
Queue::get_job_logs(&self, job_id: &str, start: Option<i64>, end: Option<i64>, asc: bool)
  -> Result<JobLogs, Box<dyn Error>>
```

Returns job logs and total count.

### Queue::get_repeatable_jobs

```rust
Queue::get_repeatable_jobs(&self, start: i64, end: i64, asc: bool)
  -> Result<Vec<RepeatableJob>, Box<dyn Error>>
```

Lists repeatable job configurations.

### Queue::get_repeatable_count

```rust
Queue::get_repeatable_count(&self) -> Result<i64, Box<dyn Error>>
```

### Queue::remove_repeatable

```rust
Queue::remove_repeatable(&self, name: Option<&str>, repeat: RepeatOpts)
  -> Result<(), Box<dyn Error>>
```

Removes a repeatable job configuration. `repeat` must match the original
options (and job id if used).

### Queue::remove_repeatable_by_key

```rust
Queue::remove_repeatable_by_key(&self, repeat_job_key: &str) -> Result<(), Box<dyn Error>>
```

Removes a repeatable job configuration by its key.

### Queue::get_metrics

```rust
Queue::get_metrics(&self, metric_type: &str, start: Option<i64>, end: Option<i64>)
  -> Result<QueueMetrics, Box<dyn Error>>
```

Reads metrics for `completed` or `failed`. `QueueMetrics` includes `meta`,
`data`, and `count`.

### Queue::get_workers

```rust
Queue::get_workers(&self) -> Result<Vec<HashMap<String, String>>, Box<dyn Error>>
```

Returns Redis client info for active workers, mapped to queue name.

### Queue::subscribe

```rust
Queue::subscribe(&self) -> broadcast::Receiver<QueueEvent>
```

Subscribes to queue events for both local and global notifications.

## Job

Job data is a string (often JSON). Jobs can be created directly or fetched
by id.

### Job::create

```rust
Job::create(queue: Arc<Queue>, name: &str, data: &str, opts: Option<JobOpts>)
  -> Result<Job, Box<dyn Error>>
```

### Job::create_bulk

```rust
Job::create_bulk(queue: Arc<Queue>, jobs: Vec<Job>)
  -> Result<Vec<Job>, Box<dyn Error>>
```

### Job::from_id

```rust
Job::from_id(queue: Arc<Queue>, job_id: &str)
  -> Result<Option<Job>, Box<dyn Error>>
```

### Job::progress

```rust
Job::progress(&mut self, progress: Option<&str>)
  -> Result<Option<String>, Box<dyn Error>>
```

With a value, updates progress. With `None`, reads current progress.

### Job::log

```rust
Job::log(&self, row: &str) -> Result<(), Box<dyn Error>>
```

### Job::get_state

```rust
Job::get_state(&self) -> Result<String, Box<dyn Error>>
```

Returns: `completed`, `failed`, `delayed`, `active`, `waiting`, `paused`, or
`stuck`.

### Job::update

```rust
Job::update(&mut self, data: &str) -> Result<(), Box<dyn Error>>
```

### Job::remove

```rust
Job::remove(&self) -> Result<(), Box<dyn Error>>
```

### Job::retry

```rust
Job::retry(&mut self) -> Result<(), Box<dyn Error>>
```

### Job::discard

```rust
Job::discard(&mut self)
```

Prevents retry even if attempts remain.

### Job::promote

```rust
Job::promote(&self) -> Result<(), Box<dyn Error>>
```

Moves a delayed job to waiting.

### Job::finished

```rust
Job::finished(&self) -> Result<String, Box<dyn Error>>
```

Resolves when the job completes or fails (via queue events).

### Job::move_to_completed

```rust
Job::move_to_completed(&mut self, return_value: &str, ignore_lock: bool, not_fetch: bool)
  -> Result<(), Box<dyn Error>>
```

### Job::move_to_failed

```rust
Job::move_to_failed(&mut self, err: &str, ignore_lock: bool)
  -> Result<MoveToFailedResult, Box<dyn Error>>
```

### Job::move_to_delayed

```rust
Job::move_to_delayed(&self, timestamp: i64, ignore_lock: bool)
  -> Result<i64, Box<dyn Error>>
```

## Events

Subscribe to events with `Queue::subscribe()`:

```rust
let mut events = queue.subscribe();
while let Ok(event) = events.recv().await {
    match event {
        QueueEvent::Completed(job_id, result) => {}
        QueueEvent::Failed(job_id, reason) => {}
        QueueEvent::GlobalCompleted(job_id, result) => {}
        QueueEvent::GlobalFailed(job_id, reason) => {}
        _ => {}
    }
}
```

Events are represented by the `QueueEvent` enum. Global events are already
decoded and emitted as `QueueEvent::Global*` variants.

## JobOpts

```rust
pub struct JobOpts {
  pub custom_job_id: String,
  pub repeat: Option<RepeatOpts>,
  pub priority: u32,
  pub lifo: bool,
  pub attempts: u32,
  pub timeout: Option<u64>,
  pub backoff: Option<BackoffConfig>,
  pub remove_on_complete: Option<RemoveOn>,
  pub remove_on_fail: Option<RemoveOn>,
  pub stack_trace_limit: Option<usize>,
  pub prev_millis: Option<i64>,
}

pub enum RemoveOn {
  Count(i64),
  Enabled(bool),
}

pub struct BackoffConfig {
  pub backoff_type: String,
  pub delay: i64,
  pub options: Option<Value>,
}

pub struct RepeatOpts {
  pub every: Option<i64>,
  pub cron: Option<String>,
  pub tz: Option<String>,
  pub end_date: Option<i64>,
  pub start_date: Option<i64>,
  pub limit: Option<u32>,
  pub count: Option<u32>,
  pub key: Option<String>,
  pub job_id: Option<String>,
}
```
