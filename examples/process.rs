use bullrs::queue::Queue;
use bullrs::job::Job;
use tokio::time::sleep;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("bullrs main: process");

    let queue = Queue::new("test-queue", "redis://localhost:6379").await?;

    let qr = queue.process(|data: String| async move {
        sleep(Duration::from_millis(1000)).await;
        println!("data: {}", data);
        "yyyy".to_string()
    }).await?;

    // dbg!(qr);

    loop {
    }

    Ok(())
}
