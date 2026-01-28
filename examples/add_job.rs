use bullrs::queue::Queue;
use bullrs::job::Job;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("bullrs main: add job");

    let queue = Queue::new("test-queue", "redis://localhost:6379").await?;

    let uuid = Uuid::new_v4();

    println!("uuid: {}", uuid);

    let job = Job::new(&format!(r#"{{"uuid":"{}"}}"#, uuid));

    queue.add_job(job).await?;

    Ok(())
}
