use chrono::Utc;
use std::collections::HashMap;
use std::future::Future;
use tokio::time::sleep;
use std::time::Duration;
use fred::pool::RedisPool;
use fred::prelude::{RedisConfig, ReconnectPolicy};
use uuid::Uuid;
use md5::{Digest, Md5};

use crate::commands::Commands;
use crate::job::{Job, JobFn};

// #[derive(Clone)]
pub struct Queue<'a> {
    prefix: &'a str,
    queue_name: &'a str,
    queue_prefix: String,
    joiner: &'a str,
    pub token: String,
    handlers: HashMap<String, JobFn>,
    commands: Commands,
    redis_client: RedisPool,
}

impl<'a> Queue<'a> {
    pub async fn new(queue_name: &'a str, redis_url: &str) -> Result<Queue<'a>, Box<dyn std::error::Error>> {
        let config = RedisConfig::from_url(redis_url)?;
        let policy = ReconnectPolicy::new_linear(0, 5000, 100);
        let redis_client = RedisPool::new(config, None, Some(policy), 5)?;

        redis_client.connect();
        redis_client.wait_for_connect().await?;

        let commands = Commands::new(&redis_client).await?;
        let prefix = "bull";
        let joiner = ":";
        let queue_prefix = vec!(prefix, queue_name, "").join(joiner);

        let mut hasher = Md5::new();
        hasher.update(&queue_prefix);
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&hasher.finalize()[..16]);
        let token = Uuid::from_bytes(bytes).to_string();

        Ok(Queue {
            prefix,
            queue_name,
            queue_prefix,
            joiner,
            token,
            handlers: HashMap::new(),
            commands,
            redis_client
        })
    }

    pub async fn add_job(&self, job: Job) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let opts_json = job.to_json();
        let opts = job.opts.unwrap();

        let custom_job_id = opts.custom_job_id;

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
            job.name,
            job.data,
            opts_json.to_string(),
            job.timestamp.to_string(),
            job.delay.to_string(),
            delay_timestamp.to_string(),
            opts.priority.to_string(),
            lifo.to_string(),
            self.token.to_string(),
        );

        //dbg!(&keys);
        //dbg!(&args);

        let result = self.commands.add_job(self, args).await?;
        //dbg!(result);
        Ok(result)
    }

    pub async fn get_next_job(&self) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let job_id = self.commands.get_job_id(self).await?;
        dbg!(&job_id);
        Ok(job_id)
    }

    pub async fn move_to_active(&self, job_id: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
        let job = self.commands.move_to_active(self, job_id).await?;
        dbg!(&job);
        Ok(())
    }

    pub async fn move_unlocked_jobs_to_wait(&self) -> Result<(), Box<dyn std::error::Error>> {
        let (failed, stalled) = self.commands.move_stalled_jobs_to_wait(self).await?;
        dbg!(&failed);
        dbg!(&stalled);
        Ok(())
    }

    pub async fn process<F, R>(&self, func: F) -> Result<(), Box<dyn std::error::Error>>
        where
            R: Future<Output = String>,
            F: FnOnce(String) -> R,
    {
        println!("start_process");

        println!("update_delay_timer_start");
        self.update_delay_timer().await?;
        println!("update_delay_timer_finished");

        println!("move_unlocked_job_started");
        self.move_unlocked_jobs_to_wait().await?;
        println!("move_unlocked_job_finished");

        println!("get_next_job_start");
        let job_id = self.get_next_job().await?;
        println!("get_next_job_finished");

        if job_id.is_none() {
            println!("no job available");
            return Ok(());
        }

        println!("move_to_active_started");
        let job_data = self.move_to_active(job_id).await?;
        println!("move_to_active_finished");

        dbg!(job_data);

        sleep(Duration::from_millis(2000)).await;
        let result = func("from process".to_string()).await;
        println!("result: {}", result);
        Ok(())
    }

    pub fn get_key(&self, pf: &str) -> String {
        self.queue_prefix.to_string() + pf
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
        dbg!(&result);
        Ok(result)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn add_job_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = Queue::new("test-queue", "redis://0.0.0.0:6379").await?;
        let job = Job::new(r#"{"a": 1}"#);
        let result = queue.add_job(job).await?;
        dbg!(result);
        Ok(())
    }

    #[tokio::test]
    async fn update_delay_timer_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = Queue::new("test-queue", "redis://0.0.0.0:6379").await?;
        let x = queue.update_delay_timer().await?;
        dbg!(x);
        Ok(())
    }

    #[tokio::test]
    async fn get_next_job_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = Queue::new("test-queue", "redis://0.0.0.0:6379").await?;
        let x = queue.get_next_job().await?;
        dbg!(x);
        Ok(())
    }

    #[tokio::test]
    async fn move_to_active_case_1() -> Result<(), Box<dyn std::error::Error>> {
        let queue = Queue::new("test-queue", "redis://0.0.0.0:6379").await?;
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
        let queue = Queue::new("test-queue", "redis://0.0.0.0:6379").await?;
        let data = queue.move_unlocked_jobs_to_wait().await?;
        dbg!(data);
        Ok(())
    }
}
