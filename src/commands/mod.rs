use chrono::Utc;
use std::env;
use tokio::fs::{read_dir, read_to_string};
use std::path::Path;
use std::ffi::OsStr;
use std::collections::HashMap;
use std::io::ErrorKind;
use fred::pool::RedisPool;
use fred::prelude::*;
use fred::types::RedisValue;
use serde::Serialize;
use crate::queue::Queue;

#[derive(Debug, PartialEq)]
struct File<'a> {
    name: &'a str,
    num_args: u8,
    original_filename: &'a str,
}

#[derive(Debug, Clone)]
struct ScriptDefinition {
    lua: String,
    number_of_keys: u8,
}

#[derive(Debug, Serialize)]
struct KeepJobs {
    count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    age: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub struct KeepJobsConfig {
    pub count: i64,
    pub age: Option<i64>,
}

fn redis_value_to_string(value: &RedisValue) -> Option<String> {
    match value {
        RedisValue::String(value) => Some(value.to_string()),
        RedisValue::Bytes(value) => String::from_utf8(value.to_vec()).ok(),
        RedisValue::Integer(value) => Some(value.to_string()),
        RedisValue::Double(value) => Some(value.to_string()),
        _ => None,
    }
}

fn redis_value_to_i64(value: &RedisValue) -> Option<i64> {
    match value {
        RedisValue::Integer(value) => Some(*value),
        RedisValue::String(value) => value.parse::<i64>().ok(),
        RedisValue::Bytes(value) => String::from_utf8(value.to_vec()).ok().and_then(|val| val.parse::<i64>().ok()),
        RedisValue::Boolean(value) => Some(if *value { 1 } else { 0 }),
        RedisValue::Null => Some(0),
        _ => None,
    }
}

fn encode_delayed_timestamp(job_id: &str, timestamp: i64) -> i64 {
    let mut ts = timestamp;
    if ts < 0 {
        ts = 0;
    }
    if ts == 0 {
        return ts;
    }
    let job_num = job_id.parse::<i64>().unwrap_or(0);
    ts * 0x1000 + (job_num & 0xfff)
}

fn parse_filename<'a>(filename: &'a str, name: &'a mut String) -> Option<File<'a>> {
    let file = Path::new(filename);
    let ext = file.extension().and_then(OsStr::to_str)?;

    if ext != "lua" {
        return None
    }

    let stem = file.file_stem().and_then(OsStr::to_str)?;
    let mut parts = stem.split('-');
    let command_name = parts.next()?;
    let num_args = parts.next()?;
    if parts.next().is_some() {
        return None
    }

    let _ = &command_name
        .chars()
        .for_each(|v| {
            match v.is_uppercase() {
                true => {
                    name.push('_');
                    name.push(v.to_lowercase().next().unwrap());
                },
                false => name.push(v)
            }
        });

    let file = File {
        name,
        num_args: num_args.parse::<u8>().ok()?,
        original_filename: filename,
    };

    Some(file)
}

async fn load_scripts() -> Result<HashMap<String, ScriptDefinition>, Box<dyn std::error::Error>> {
    let cwd = env::current_dir()?;
    let path = Path::new(&cwd);
    let path = path.join("src").join("commands");
    let mut scripts_map: HashMap<String, ScriptDefinition> = HashMap::new();

    let mut files = read_dir(&path).await?;
    let mut lua_files_found = false;
    while let Some(file) = files.next_entry().await? {
        let mut name = String::from("");
        let filename = file.file_name();
        let filename = match filename.to_str() {
            Some(value) => value,
            None => continue,
        };
        if !filename.ends_with(".lua") {
            continue;
        }
        lua_files_found = true;
        let file = parse_filename(filename, &mut name)
            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidData, format!("Invalid lua filename: {}", filename)))?;
        let script_path = path
            .join(file.original_filename)
            .into_os_string()
            .into_string().unwrap();
        let contents = read_to_string(script_path).await?;
        scripts_map.insert(file.name.to_string(), ScriptDefinition {
            lua: contents,
            number_of_keys: file.num_args,
        });
    }

    if !lua_files_found {
        return Err(std::io::Error::new(ErrorKind::NotFound, "No .lua files found!").into());
    }

    Ok(scripts_map)
}

#[derive(Debug, Clone)]
pub struct Commands {
    redis_client: RedisPool,
    scripts: HashMap<String, ScriptDefinition>,
}

impl Commands {
    pub async fn new(redis_client: &RedisPool) -> Result<Commands, Box<dyn std::error::Error>> {
        let scripts = load_scripts().await?;
        Ok(Commands {
            redis_client: redis_client.clone(),
            scripts
        })
    }

    fn get_script(&self, name: &str, keys_len: usize) -> Result<&ScriptDefinition, Box<dyn std::error::Error>> {
        let script = self
            .scripts
            .get(name)
            .ok_or_else(|| std::io::Error::new(ErrorKind::NotFound, format!("Lua script not loaded: {}", name)))?;
        if script.number_of_keys as usize != keys_len {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Lua script {} expects {} keys, got {}",
                    name,
                    script.number_of_keys,
                    keys_len
                ),
            ).into());
        }
        Ok(script)
    }
    
    pub async fn add_job(&self, queue: &Queue, args: Vec<String>) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let keys = queue.get_keys(vec!(
            "wait",
            "paused",
            "meta-paused",
            "id",
            "delayed",
            "priority",
        ));

        let script = self.get_script("add_job", keys.len())?;

        //println!("job added");
        //println!("args: {:?}", args);
        //println!("keys: {:?}", keys);

        let result: Vec<String> = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(result)
    }

    pub async fn update_delay_set(&self, queue: &Queue, timestamp: i64) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let keys = queue.get_keys(vec!(
            "delayed",
            "active",
            "wait",
            "priority",
            "paused",
            "meta-paused"
        ));

        let script = self.get_script("update_delay_set", keys.len())?;

        let args = vec!(
            queue.get_key(""),
            timestamp.to_string(),
            queue.token.to_string(),
        );

        let result: Vec<String> = self.redis_client.eval(&script.lua, keys, args).await?;

        // dbg!(&result);

        Ok(result)
    }

    pub async fn get_job_id(&self, queue: &Queue) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let wait = queue.get_key("wait");
        let active = queue.get_key("active");
        let drain_deleay = queue.settings.drain_delay as f64;
        // dbg!(&wait);
        // dbg!(&active);
        // dbg!(drain_deleay);
        let result: Vec<String> = self.redis_client.brpoplpush(wait, active, drain_deleay).await.unwrap_or(vec![]);

        Ok(result.first().cloned())
    }

    pub async fn move_to_active(&self, queue: &Queue, job_id: Option<String>) -> Result<Option<(Vec<String>, String)>, Box<dyn std::error::Error>> {
        let timestamp = Utc::now().timestamp_millis();
        let lock_duration = queue.settings.lock_duration;
        let keys = queue.get_keys(vec!(
            "wait",
            "active",
            "priority",
            &("active".to_owned() + "@" + &queue.token),
            "stalled",
            "limiter",
            "delayed",
            "drained"
        ));

        let script = self.get_script("move_to_active", keys.len())?;

        let mut args = vec!(
            queue.get_key(""),
            queue.token.to_string(),
            lock_duration.to_string(),
            timestamp.to_string(),
            job_id.unwrap_or("".to_string())
        );

        if let Some(limiter) = queue.limiter() {
            args.push(limiter.max.to_string());
            args.push(limiter.duration.to_string());
            args.push(limiter.bounce_back.to_string());
            if limiter.group_key.is_some() {
                args.push("true".to_string());
            }
        }

        let result: RedisValue = self.redis_client.eval(&script.lua, keys, args).await?;
        match result {
            RedisValue::Array(values) => {
                if values.len() != 2 {
                    return Ok(None);
                }
                let job_data = match &values[0] {
                    RedisValue::Array(data) => data
                        .iter()
                        .filter_map(|item| redis_value_to_string(item))
                        .collect::<Vec<String>>(),
                    _ => Vec::new(),
                };
                let job_id = redis_value_to_string(&values[1]).unwrap_or_default();
                Ok(Some((job_data, job_id)))
            }
            RedisValue::Null => Ok(None),
            _ => Ok(None)
        }
    }

    pub async fn move_stalled_jobs_to_wait(&self, queue: &Queue) -> Result<(Vec<String>, Vec<String>), Box<dyn std::error::Error>> {
        let timestamp = Utc::now().timestamp_millis();
        let max_stalled_count = queue.settings.max_stalled_count;
        let stalled_interval = queue.settings.stalled_interval;

        let keys = queue.get_keys(vec!(
            "stalled",
            "wait",
            "active",
            "failed",
            "stalled-check",
            "meta-paused",
            "paused",
        ));

        let script = self.get_script("move_stalled_jobs_to_wait", keys.len())?;

        let args = vec!(
            max_stalled_count.to_string(),
            queue.get_key(""),
            timestamp.to_string(),
            stalled_interval.to_string(),
        );

        // dbg!(&keys);
        // dbg!(&args);

        let (failed, stalled): (Vec<String>, Vec<String>) = self.redis_client.eval(&script.lua, keys, args).await?;

        // dbg!(&failed);
        // dbg!(&stalled);

        Ok((failed, stalled))
    }

    pub async fn take_lock(&self, queue: &Queue, job_id: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let lock_key = format!("{}:lock", queue.get_key(job_id));
        let keys = vec![lock_key];
        let script = self.get_script("take_lock", keys.len())?;
        let args = vec![queue.token.to_string(), queue.settings.lock_duration.to_string()];
        let result: i64 = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(result == 1)
    }

    pub async fn release_lock(&self, queue: &Queue, job_id: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let lock_key = format!("{}:lock", queue.get_key(job_id));
        let keys = vec![lock_key];
        let script = self.get_script("release_lock", keys.len())?;
        let args = vec![queue.token.to_string(), queue.settings.lock_duration.to_string()];
        let result: i64 = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(result == 1)
    }

    pub async fn extend_lock(&self, queue: &Queue, job_id: &str, duration: u64) -> Result<bool, Box<dyn std::error::Error>> {
        let lock_key = format!("{}:lock", queue.get_key(job_id));
        let stalled_key = queue.get_key("stalled");
        let keys = vec![lock_key, stalled_key];
        let script = self.get_script("extend_lock", keys.len())?;
        let args = vec![queue.token.to_string(), duration.to_string(), job_id.to_string()];
        let result: i64 = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(result == 1)
    }

    pub async fn update_progress(&self, queue: &Queue, job_id: &str, progress: &str) -> Result<(), Box<dyn std::error::Error>> {
        let keys = vec![queue.get_key(job_id), queue.get_key("progress")];
        let script = self.get_script("update_progress", keys.len())?;
        let event = serde_json::json!({"jobId": job_id, "progress": progress}).to_string();
        let args = vec![progress.to_string(), event];
        let _: RedisValue = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(())
    }

    pub async fn retry_job(&self, queue: &Queue, job_id: &str, lifo: bool, ignore_lock: bool) -> Result<i64, Box<dyn std::error::Error>> {
        let keys = queue.get_keys(vec!["active", "wait", job_id, "meta-paused", "paused"]);
        let script = self.get_script("retry_job", keys.len())?;
        let push_cmd = if lifo { "RPUSH" } else { "LPUSH" };
        let token = if ignore_lock { "0".to_string() } else { queue.token.to_string() };
        let args = vec![push_cmd.to_string(), job_id.to_string(), token];
        let result: i64 = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(result)
    }

    pub async fn move_to_delayed(&self, queue: &Queue, job_id: &str, timestamp: i64, ignore_lock: bool) -> Result<i64, Box<dyn std::error::Error>> {
        let keys = queue.get_keys(vec!["active", "delayed", job_id]);
        let script = self.get_script("move_to_delayed", keys.len())?;
        let delayed_timestamp = encode_delayed_timestamp(job_id, timestamp);
        let token = if ignore_lock { "0".to_string() } else { queue.token.to_string() };
        let args = vec![delayed_timestamp.to_string(), job_id.to_string(), token];
        let result: i64 = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(result)
    }

    pub async fn promote(&self, queue: &Queue, job_id: &str) -> Result<i64, Box<dyn std::error::Error>> {
        let keys = queue.get_keys(vec!["delayed", "wait", "paused", "priority"]);
        let script = self.get_script("promote", keys.len())?;
        let args = vec![queue.get_key(""), job_id.to_string(), queue.token.to_string()];
        let result: i64 = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(result)
    }

    pub async fn is_finished(&self, queue: &Queue, job_id: &str) -> Result<i64, Box<dyn std::error::Error>> {
        let keys = queue.get_keys(vec!["completed", "failed"]);
        let script = self.get_script("is_finished", keys.len())?;
        let args = vec![job_id.to_string()];
        let result: RedisValue = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(redis_value_to_i64(&result).unwrap_or(0))
    }

    pub async fn reprocess_job(&self, queue: &Queue, job_id: &str, state: &str, lifo: bool) -> Result<i64, Box<dyn std::error::Error>> {
        let keys = vec![
            queue.get_key(job_id),
            format!("{}:lock", queue.get_key(job_id)),
            queue.get_key(state),
            queue.get_key("wait"),
            queue.get_key("meta-paused"),
            queue.get_key("paused"),
        ];
        let script = self.get_script("reprocess_job", keys.len())?;
        let push_cmd = if lifo { "RPUSH" } else { "LPUSH" };
        let args = vec![job_id.to_string(), push_cmd.to_string(), queue.token.to_string(), Utc::now().timestamp_millis().to_string()];
        let result: i64 = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(result)
    }

    pub async fn remove_job(&self, queue: &Queue, job_id: &str) -> Result<i64, Box<dyn std::error::Error>> {
        let keys = vec![
            queue.get_key("active"),
            queue.get_key("wait"),
            queue.get_key("delayed"),
            queue.get_key("paused"),
            queue.get_key("completed"),
            queue.get_key("failed"),
            queue.get_key("priority"),
            queue.get_key(job_id),
            format!("{}:logs", queue.get_key(job_id)),
            queue.get_key("limiter"),
        ];
        let script = self.get_script("remove_job", keys.len())?;
        let args = vec![job_id.to_string(), queue.token.to_string()];
        let result: i64 = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(result)
    }

    pub async fn remove_jobs(&self, queue: &Queue, pattern: &str, cursor: i64) -> Result<(i64, Vec<String>), Box<dyn std::error::Error>> {
        let keys = queue.get_keys(vec!["active", "wait", "delayed", "paused", "completed", "failed", "priority", "limiter"]);
        let script = self.get_script("remove_jobs", keys.len())?;
        let args = vec![queue.get_key(""), pattern.to_string(), cursor.to_string()];
        let result: RedisValue = self.redis_client.eval(&script.lua, keys, args).await?;
        match result {
            RedisValue::Array(values) if values.len() == 2 => {
                let cursor = redis_value_to_string(&values[0]).and_then(|val| val.parse::<i64>().ok()).unwrap_or(0);
                let removed = match &values[1] {
                    RedisValue::Array(items) => items
                        .iter()
                        .filter_map(redis_value_to_string)
                        .collect::<Vec<String>>(),
                    _ => Vec::new(),
                };
                Ok((cursor, removed))
            }
            _ => Ok((0, Vec::new()))
        }
    }

    pub async fn is_job_in_list(&self, queue: &Queue, list: &str, job_id: &str) -> Result<i64, Box<dyn std::error::Error>> {
        let keys = vec![queue.get_key(list)];
        let script = self.get_script("is_job_in_list", keys.len())?;
        let args = vec![job_id.to_string()];
        let result: RedisValue = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(redis_value_to_i64(&result).unwrap_or(0))
    }

    pub async fn remove_repeatable(&self, queue: &Queue, repeat_job_id: &str, repeat_job_key: &str) -> Result<(), Box<dyn std::error::Error>> {
        let keys = vec![queue.get_key("repeat"), queue.get_key("delayed")];
        let script = self.get_script("remove_repeatable", keys.len())?;
        let args = vec![repeat_job_id.to_string(), repeat_job_key.to_string(), queue.get_key("")];
        let _: RedisValue = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(())
    }

    pub async fn pause(&self, queue: &Queue, pause: bool) -> Result<(), Box<dyn std::error::Error>> {
        let (source, target, event) = if pause {
            ("wait", "paused", "paused")
        } else {
            ("paused", "wait", "resumed")
        };

        let keys = queue.get_keys(vec!(
            source,
            target,
            "meta-paused",
            event,
            "meta",
        ));

        let script = self.get_script("pause", keys.len())?;

        let args = vec!(event.to_string());

        let _: RedisValue = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(())
    }

    pub async fn retry_jobs(&self, queue: &Queue, count: usize) -> Result<i64, Box<dyn std::error::Error>> {
        let keys = queue.get_keys(vec!(
            "",
            "failed",
            "wait",
            "meta-paused",
            "paused",
        ));

        let script = self.get_script("retry_jobs", keys.len())?;
        let args = vec!(count.to_string());

        let result: i64 = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(result)
    }

    pub async fn clean_jobs_in_set(&self, queue: &Queue, set_name: &str, max_timestamp: i64, limit: usize) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let keys = vec!(
            queue.get_key(set_name),
            queue.get_key("priority"),
            queue.get_key("limiter"),
        );

        let script = self.get_script("clean_jobs_in_set", keys.len())?;

        let args = vec!(
            queue.get_key(""),
            max_timestamp.to_string(),
            limit.to_string(),
            set_name.to_string(),
        );

        let result: Vec<String> = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(result)
    }

    pub async fn obliterate(&self, queue: &Queue, count: usize, force: bool) -> Result<i64, Box<dyn std::error::Error>> {
        let keys = queue.get_keys(vec!(
            "meta-paused",
            "",
        ));

        let script = self.get_script("obliterate", keys.len())?;

        let args = vec!(
            count.to_string(),
            if force { "force".to_string() } else { "".to_string() },
        );

        let result: i64 = self.redis_client.eval(&script.lua, keys, args).await?;
        Ok(result)
    }

    pub async fn move_to_completed(&self, queue: &Queue, job_id: &str, return_value: &str) -> Result<(), Box<dyn std::error::Error>> {
        let keep_jobs = KeepJobsConfig { count: -1, age: None };
        self.move_to_completed_with_opts(queue, job_id, return_value, keep_jobs, false, false).await
    }

    pub async fn move_to_failed(&self, queue: &Queue, job_id: &str, failed_reason: &str) -> Result<(), Box<dyn std::error::Error>> {
        let keep_jobs = KeepJobsConfig { count: -1, age: None };
        self.move_to_failed_with_opts(queue, job_id, failed_reason, keep_jobs, false).await
    }

    pub async fn move_to_completed_with_opts(
        &self,
        queue: &Queue,
        job_id: &str,
        return_value: &str,
        keep_jobs: KeepJobsConfig,
        ignore_lock: bool,
        not_fetch: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.move_to_finished(queue, job_id, return_value, "returnvalue", "completed", keep_jobs, ignore_lock, not_fetch).await
    }

    pub async fn move_to_failed_with_opts(
        &self,
        queue: &Queue,
        job_id: &str,
        failed_reason: &str,
        keep_jobs: KeepJobsConfig,
        ignore_lock: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.move_to_finished(queue, job_id, failed_reason, "failedReason", "failed", keep_jobs, ignore_lock, true).await
    }

    async fn move_to_finished(
        &self,
        queue: &Queue,
        job_id: &str,
        value: &str,
        prop_val: &str,
        target: &str,
        keep_jobs: KeepJobsConfig,
        ignore_lock: bool,
        not_fetch: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let keys = queue.get_keys(vec!(
            "active",
            target,
            job_id,
            "wait",
            "priority",
            &format!("active@{}", queue.token),
            "delayed",
            "stalled",
            &format!("metrics:{}", target),
        ));

        let script = self.get_script("move_to_finished", keys.len())?;

        let keep_jobs = KeepJobs { count: keep_jobs.count, age: keep_jobs.age };
        let keep_jobs = rmp_serde::to_vec_named(&keep_jobs)?;
        let event_data = serde_json::json!({"jobId": job_id, "val": value}).to_string();
        let timestamp = Utc::now().timestamp_millis().to_string();
        let not_fetch_arg = if not_fetch { "0" } else { "1" };
        let lock_token = if ignore_lock { "0" } else { &queue.token };

        let metrics_limit = queue.metrics.as_ref().map(|metrics| metrics.max_data_points.to_string()).unwrap_or_default();
        let args: Vec<RedisValue> = vec!(
            job_id.to_string().into(),
            timestamp.into(),
            prop_val.to_string().into(),
            value.to_string().into(),
            lock_token.to_string().into(),
            RedisValue::Bytes(keep_jobs.into()),
            event_data.into(),
            not_fetch_arg.to_string().into(),
            queue.get_key("").into(),
            queue.settings.lock_duration.to_string().into(),
            queue.token.to_string().into(),
            metrics_limit.into(),
        );

        let result: RedisValue = self.redis_client.eval(&script.lua, keys, args).await?;
        let result_code = redis_value_to_i64(&result).unwrap_or(0);
        if result_code < 0 {
            let message = match result_code {
                -1 => format!("Missing key for job {}", job_id),
                -2 => format!("Missing lock for job {}", job_id),
                _ => format!("Failed to move job {} to {}", job_id, target),
            };
            return Err(message.into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::Job;
    use crate::queue::Queue;
    use fred::pool::RedisPool;
    use fred::prelude::{RedisConfig, ReconnectPolicy};
    use uuid::Uuid;

    const REDIS_URL: &str = "redis://0.0.0.0:6379/0";

    async fn build_commands(queue_name: &str) -> Result<(Commands, std::sync::Arc<Queue>), Box<dyn std::error::Error>> {
        let config = RedisConfig::from_url(REDIS_URL)?;
        let policy = ReconnectPolicy::new_linear(0, 5000, 100);
        let redis_client = RedisPool::new(config, None, Some(policy), 5)?;
        redis_client.connect();
        redis_client.wait_for_connect().await?;

        let commands = Commands::new(&redis_client).await?;
        let queue = Queue::new(queue_name, REDIS_URL).await?;

        Ok((commands, queue))
    }

    fn build_add_job_args(queue: &Queue, job: Job) -> Vec<String> {
        let opts_json = job.to_json();
        let opts = job.opts.unwrap_or_default();
        let custom_job_id = opts.custom_job_id;
        let delay_timestamp = if job.delay > 0 {
            job.timestamp + job.delay as i64
        } else {
            0
        };
        let lifo = if opts.lifo { "RPUSH" } else { "LPUSH" };

        vec!(
            queue.get_key(""),
            custom_job_id,
            job.name,
            job.data,
            opts_json.to_string(),
            job.timestamp.to_string(),
            job.delay.to_string(),
            delay_timestamp.to_string(),
            opts.priority.to_string(),
            lifo.to_string(),
            queue.token.to_string(),
        )
    }

    #[tokio::test]
    async fn convert_camel_case_case_1() {
        let file_name = "moveToDelayed-3.lua";
        let mut name = String::from("");
        let parsed = parse_filename(file_name, &mut name);

        assert_eq!(parsed, Some(File {
            name: "move_to_delayed",
            num_args: 3,
            original_filename: "moveToDelayed-3.lua",
        }));
    }

    #[tokio::test]
    async fn load_scripts_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let script_map = load_scripts().await?;
        let script = script_map.get("add_job").unwrap();
        let lua = include_str!("./addJob-6.lua");
        assert_eq!(script.lua.as_str(), lua);
        Ok(())
    }

    #[tokio::test]
    async fn commands_add_job_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue_name = format!("test-queue-{}", Uuid::new_v4());
        let (commands, queue) = build_commands(&queue_name).await?;
        let job = Job::new(r#"{"a": 1}"#);
        let args = build_add_job_args(&queue, job);
        let result = commands.add_job(&queue, args).await?;
        assert!(!result.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn commands_update_delay_set_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue_name = format!("test-queue-{}", Uuid::new_v4());
        let (commands, queue) = build_commands(&queue_name).await?;
        let result = commands.update_delay_set(&queue, Utc::now().timestamp_millis()).await?;
        assert!(result.iter().all(|entry| !entry.is_empty()));
        Ok(())
    }

    #[tokio::test]
    async fn commands_get_job_id_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue_name = format!("test-queue-{}", Uuid::new_v4());
        let (commands, queue) = build_commands(&queue_name).await?;
        let job = Job::new(r#"{"a": 1}"#);
        let args = build_add_job_args(&queue, job);
        let result = commands.add_job(&queue, args).await?;
        let expected_id = result.first().expect("expected job id").to_string();
        let job_id = commands.get_job_id(&queue).await?;
        assert_eq!(job_id.as_deref(), Some(expected_id.as_str()));
        Ok(())
    }

    #[tokio::test]
    async fn commands_move_to_active_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue_name = format!("test-queue-{}", Uuid::new_v4());
        let (commands, queue) = build_commands(&queue_name).await?;
        let job = Job::new(r#"{"a": 1}"#);
        let args = build_add_job_args(&queue, job);
        let _ = commands.add_job(&queue, args).await?;
        let job_id = commands.get_job_id(&queue).await?.expect("expected job id");
        let result = commands.move_to_active(&queue, Some(job_id)).await?;
        let result = result.expect("expected job data");
        assert!(!result.1.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn commands_move_stalled_jobs_to_wait_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue_name = format!("test-queue-{}", Uuid::new_v4());
        let (commands, queue) = build_commands(&queue_name).await?;
        let (failed, stalled) = commands.move_stalled_jobs_to_wait(&queue).await?;
        assert!(failed.iter().all(|entry| !entry.is_empty()));
        assert!(stalled.iter().all(|entry| !entry.is_empty()));
        Ok(())
    }

    #[tokio::test]
    async fn exec_lua_add_job_case_1() -> Result<(), RedisError> {
        let config = RedisConfig::from_url("redis://0.0.0.0:6379/0")?;
        let client = RedisClient::new(config, None, None);
        let _ = client.connect();
        let _ = client.wait_for_connect().await.unwrap();

        let lua = include_str!("./addJob-6.lua");

        //let script = Script::from_lua(lua);
        // let _ = script.load(&client).await.unwrap();
        let keys = vec![
            "bull:test-queue:wait",
            "bull:test-queue:paused",
            "bull:test-queue:meta-paused",
            "bull:test-queue:id",
            "bull:test-queue:delayed",
            "bull:test-queue:priority",
        ];
        let args = vec![
            "bull:test-queue:",
            "",
            "__default__",
            "{\"a\":1}",
            "{\"attempts\":1,\"delay\":0,\"timestamp\":1679947727071}",
            "1679947727071",
            "0",
            "0",
            "0",
            "LPUSH",
            "ebb12588-da63-4b70-b127-c6d06f2ab064"
        ];

        let result: Vec<String> = client.eval(lua, keys, args).await.unwrap();

        dbg!(result);

        Ok(())
    }

    #[tokio::test]
    async fn exec_lua_pause_case_1() -> Result<(), RedisError> {
        let config = RedisConfig::from_url("redis://0.0.0.0:6379/0")?;
        let client = RedisClient::new(config, None, None);
        let _ = client.connect();
        let _ = client.wait_for_connect().await.unwrap();

        let lua = include_str!("./pause-5.lua");

        let keys = vec![
            "bull:test-queue:wait",
            "bull:test-queue:paused",
            "bull:test-queue:meta-paused",
            "bull:test-queue:paused",
            "bull:test-queue:meta",
        ];

        let args = vec![
            "paused"
        ];

        let result: Vec<String> = client.eval(lua, keys, args).await.unwrap();

        dbg!(result);

        Ok(())
    }

    #[tokio::test]
    async fn exec_lua_resume_case_1() -> Result<(), RedisError> {
        let config = RedisConfig::from_url("redis://0.0.0.0:6379/0")?;
        let client = RedisClient::new(config, None, None);
        let _ = client.connect();
        let _ = client.wait_for_connect().await.unwrap();

        let lua = include_str!("./pause-5.lua");

        let keys = vec![
            "bull:test-queue:paused",
            "bull:test-queue:wait",
            "bull:test-queue:meta-paused",
            "bull:test-queue:resumed",
            "bull:test-queue:meta",
        ];

        let args = vec![
            "resumed"
        ];

        let result: Vec<String> = client.eval(lua, keys, args).await.unwrap();

        dbg!(result);

        Ok(())
    }

    #[tokio::test]
    async fn exec_lua_update_delay_set_case_1() -> Result<(), RedisError> {
        let config = RedisConfig::from_url("redis://0.0.0.0:6379/0")?;
        let client = RedisClient::new(config, None, None);
        let _ = client.connect();
        let _ = client.wait_for_connect().await.unwrap();

        let lua = include_str!("./updateDelaySet-6.lua");

        let keys = vec![
            "bull:test-queue:delayed",
            "bull:test-queue:active",
            "bull:test-queue:wait",
            "bull:test-queue:priority",
            "bull:test-queue:paused",
            "bull:test-queue:meta-paused",
        ];

        let ts = Utc::now().timestamp_millis().to_string(); 
        let ts = ts.as_str();
        let args = vec![
            "bull:test-queue:",
            ts,
            "5735bf2b-a90c-4103-8c42-881210110142"
        ];

        let result: Vec<String> = client.eval(lua, keys, args).await.unwrap();

        dbg!(result);

        Ok(())
    }

    #[tokio::test]
    async fn exec_lua_move_unlocked_jobs_to_wait_case_1() -> Result<(), RedisError> {
        let config = RedisConfig::from_url("redis://0.0.0.0:6379/0")?;
        let policy = ReconnectPolicy::new_linear(0, 5000, 100);
        let client = RedisPool::new(config, None, Some(policy), 5)?;

        // let config = RedisConfig::from_url("redis://0.0.0.0:6379/0")?;
        // let client = RedisClient::new(config, None, None);
        let _ = client.connect();
        let _ = client.wait_for_connect().await.unwrap();

        dbg!(&client);

        let lua = include_str!("./moveStalledJobsToWait-7.lua");

        let keys = vec![
            "bull:test-queue:stalled",
            "bull:test-queue:wait",
            "bull:test-queue:active",
            "bull:test-queue:failed",
            "bull:test-queue:stalled-check",
            "bull:test-queue:meta-paused",
            "bull:test-queue:paused",
        ];

        let ts = Utc::now().timestamp_millis().to_string(); 
        let ts = ts.as_str();
        let args = vec![
            "10", // queue.settings.maxStalledCount
            "bull:test-queue:",
            ts,
            "1000" // queue.settings.stalledInterval
        ];

        let result: (Vec<String>, Vec<String>) = client.eval(lua, keys, args).await.unwrap();

        dbg!(result);

        Ok(())
    }

    #[tokio::test]
    async fn get_job_id_case_1() -> Result<(), RedisError> {
        Ok(())
    }

    #[tokio::test]
    async fn move_to_active_case_1() -> Result<(), Box<dyn std::error::Error>> {

        Ok(())
    }
}
