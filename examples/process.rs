use bullrs::queue::Queue;
use tokio::time::sleep;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("bullrs main: process");

    let queue = Queue::new("test-queue", "redis://localhost:26379").await?;

    let _handle = queue.clone().process(|data: String| async move {
        sleep(Duration::from_millis(1000)).await;
        println!("data: {}", data);
        "yyyy".to_string()
    }).await?;

    // dbg!(qr);

    // sleep(Duration::from_millis(1000)).await;
    // handle.stop(Duration::from_secs(1)).await?;

    // Ok(())
    loop {}
}
