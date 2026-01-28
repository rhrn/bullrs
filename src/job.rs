use std::pin::Pin;
use std::future::Future;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub type JobFn = Box<dyn Fn(String) -> Pin<Box<dyn Future<Output = ()>>>>;

#[derive(Debug, Serialize, Clone)]
pub struct JobOpts {
    pub custom_job_id: String,
    pub priority: u32,
    pub lifo: bool,
}

impl Default for JobOpts {
    fn default() -> JobOpts {
        JobOpts {
            custom_job_id: "".to_string(),
            priority: 0,
            lifo: false
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct Job {
    pub name: String,
    pub data: String,
    pub attempts: Option<u32>,
    pub delay: u64,
    pub timestamp: i64,
    pub opts: Option<JobOpts>,
}

impl Default for Job {
    fn default() -> Job {
        Job {
            data: "".to_string(),
            name: "__default__".to_string(),
            attempts: Some(1),
            delay: 0,
            timestamp: Utc::now().timestamp_millis(),
            opts: Some(JobOpts::default())
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

    pub fn to_json(&self) -> Value {
        let Job { attempts, delay, timestamp, .. } = self;
        json!({
            "attempts": attempts,
            "timestamp": timestamp,
            "delay": delay
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn new_job() -> Result<(), Box<dyn std::error::Error>> {
        let job = Job::new("new data");
        dbg!(job);
        Ok(())
    }
}
