use std::future::Future;
use tokio::time::sleep;
use std::time::Duration;
use fred::pool::RedisPool;
use fred::prelude::{RedisConfig, ReconnectPolicy};
use uuid::Uuid;
use md5::{Digest, Md5};

use crate::commands::Commands;
use crate::job::Job;

#[derive(Debug, Clone)]
pub struct Queue<'a> {
    prefix: &'a str,
    queue_name: &'a str,
    queue_prefix: String,
    joiner: &'a str,
    token: String,
    commands: Commands,
}

impl<'a> Queue<'a> {
    pub async fn new(queue_name: &'a str, redis_url: &str) -> Result<Queue<'a>, Box<dyn std::error::Error>> {
        let config = RedisConfig::from_url(redis_url)?;
        let policy = ReconnectPolicy::new_linear(0, 5000, 100);
        let redis_client = RedisPool::new(config, None, Some(policy), 5)?;

        redis_client.connect();
        redis_client.wait_for_connect().await?;

        let commands = Commands::new(redis_client).await?;
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
            commands,
        })
    }

    fn get_key(&self, pf: &str) -> String {
        self.queue_prefix.to_string() + pf
    }

    fn get_keys(&self, postfix: Vec<&str>) -> Vec<String> {
        postfix.
            iter().map(|pf| {
                self.get_key(pf)
            })
            .collect()
    }

    pub async fn add_job(&self, job: Job) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let opts_json = job.to_json();
        let opts = job.opts.unwrap();
        let keys = self.get_keys(vec!(
            "",
            "paused",
            "meta-paused",
            "id",
            "delayed",
            "priority",
        ));

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

        let result = self.commands.add_job(keys, args).await?;
        //dbg!(result);
        Ok(result)
    }

    pub async fn process<F, R>(&self, func: F) -> Result<(), Box<dyn std::error::Error>>
        where
            R: Future<Output = String>,
            F: FnOnce(String) -> R,
    {
        sleep(Duration::from_millis(2000)).await;
        let result = func("from process".to_string()).await;
        println!("result: {}", result);
        Ok(())
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
    async fn new_queue() -> Result<(), Box<dyn std::error::Error>> {
        let queue = Queue::new("queue", "redis://localhost:6379").await?;
        let job = Job::new("add job!!!");
        queue.add_job(job).await?;
        dbg!(queue);
        Ok(())
    }
}
