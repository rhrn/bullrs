use chrono::Utc;
use std::env;
use tokio::fs::{read_dir, read_to_string};
use std::path::Path;
use std::ffi::OsStr;
use std::collections::HashMap;
use fred::pool::RedisPool;
use fred::prelude::*;
use crate::queue::Queue;

#[derive(Debug, PartialEq)]
struct File<'a> {
    name: &'a str,
    num_args: u8,
    original_filename: &'a str,
}

fn parse_filename<'a>(filename: &'a str, name: &'a mut String) -> Option<File<'a>> {
    let file = Path::new(filename);
    let ext = file.extension().and_then(OsStr::to_str)?;

    if ext != "lua" {
        return None
    }

    let stem = file.file_stem().and_then(OsStr::to_str)?;
    let stem = stem.split('-').collect::<Vec<&str>>();

    let _ = &stem[0]
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
        num_args: stem[1].parse::<u8>().ok()?,
        original_filename: filename,
    };

    Some(file)
}

async fn load_scripts() -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let cwd = env::current_dir()?;
    let path = Path::new(&cwd);
    let path = path.join("src").join("commands");
    let mut scripts_map: HashMap<String, String> = HashMap::new();

    let mut files = read_dir(&path).await?;
    while let Some(file) = files.next_entry().await? {
        let mut name = String::from("");
        let filename = file.file_name();
        if let Some(file) = parse_filename(filename.to_str().unwrap(), &mut name) {
            let script_path = path
                .join(file.original_filename)
                .into_os_string()
                .into_string().unwrap();
            let contents = read_to_string(script_path).await?;
            scripts_map.insert(file.name.to_string(), contents);
        }
    }

    Ok(scripts_map)
}

#[derive(Debug, Clone)]
pub struct Commands {
    redis_client: RedisPool,
    scripts: HashMap<String, String>,
}

impl<'a> Commands {
    pub async fn new(redis_client: &RedisPool) -> Result<Commands, Box<dyn std::error::Error>> {
        let scripts = load_scripts().await?;
        Ok(Commands {
            redis_client: redis_client.clone(),
            scripts
        })
    }
    
    pub async fn add_job(&self, queue: &Queue<'a>, args: Vec<String>) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let lua = self.scripts.get("add_job").unwrap();

        let keys = queue.get_keys(vec!(
            "wait",
            "paused",
            "meta-paused",
            "id",
            "delayed",
            "priority",
        ));

        //println!("job added");
        //println!("args: {:?}", args);
        //println!("keys: {:?}", keys);

        let result: Vec<String> = self.redis_client.eval(lua, keys, args).await.unwrap();
        Ok(result)
    }

    pub async fn update_delay_set(&self, queue: &Queue<'a>, timestamp: i64) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let lua = self.scripts.get("update_delay_set").unwrap();

        let keys = queue.get_keys(vec!(
            "delayed",
            "active",
            "wait",
            "priority",
            "paused",
            "meta-paused"
        ));

        let args = vec!(
            queue.get_key(""),
            timestamp.to_string(),
            queue.token.to_string(),
        );

        let result: Vec<String> = self.redis_client.eval(lua, keys, args).await.unwrap();

        // dbg!(&result);

        Ok(result)
    }

    pub async fn get_job_id(&self, queue: &Queue<'a>) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let wait = queue.get_key("wait");
        let active = queue.get_key("active");
        let drain_deleay = 5.0;
        dbg!(&wait);
        dbg!(&active);
        dbg!(drain_deleay);
        let result: Vec<String> = self.redis_client.brpoplpush(wait, active, drain_deleay).await.unwrap_or(vec![]);

        Ok(result.first().cloned())
    }

    pub async fn move_to_active(&self, queue: &Queue<'a>, job_id: Option<String>) -> Result<(Vec<String>, String), Box<dyn std::error::Error>> {
        let lua = self.scripts.get("move_to_active").unwrap();
        let timestamp = Utc::now().timestamp_millis();
        let lock_duration = 30000;
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

        let args = vec!(
            queue.get_key(""),
            queue.token.to_string(),
            lock_duration.to_string(),
            timestamp.to_string(),
            job_id.unwrap_or("".to_string())
        );

        let result: (Vec<String>, String) = self.redis_client.eval(lua, keys, args).await?;

        Ok(result)
    }

    pub async fn move_stalled_jobs_to_wait(&self, queue: &Queue<'a>) -> Result<(Vec<String>, Vec<String>), Box<dyn std::error::Error>> {
        let lua = self.scripts.get("move_stalled_jobs_to_wait").unwrap();
        let timestamp = Utc::now().timestamp_millis();
        let max_stalled_count = 1;
        let stalled_interval = 30000;

        let keys = queue.get_keys(vec!(
            "stalled",
            "wait",
            "active",
            "failed",
            "stalled-check",
            "meta-paused",
            "paused",
        ));

        let args = vec!(
            max_stalled_count.to_string(),
            queue.get_key(""),
            timestamp.to_string(),
            stalled_interval.to_string(),
        );

        dbg!(&keys);
        dbg!(&args);

        let (failed, stalled): (Vec<String>, Vec<String>) = self.redis_client.eval(lua, keys, args).await?;

        dbg!(&failed);
        dbg!(&stalled);

        Ok((failed, stalled))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fred::prelude::*;

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
        assert_eq!(script, lua);
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
