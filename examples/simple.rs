use bullrs::queue::{Queue, QueueOptions, QueueSettings};
use tokio::time::sleep;
use std::time::Duration;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("bullrs main: simple");

    let settings = QueueSettings {
        drain_delay: 1,
        ..QueueSettings::default()
    };
    let opts = QueueOptions {
        settings: Some(settings),
        ..QueueOptions::default()
    };
    let queue = Queue::new_with_opts("test-queue", "redis://localhost:26379", opts).await?;

    let _handle = queue.clone().process(|data: String| async move {
        sleep(Duration::from_millis(1000)).await;
        println!("received data: {}", data);
        data
    }).await?;

    let uuid = Uuid::new_v4();

    println!("init uuid: {uuid}");

    queue.add(format!("new job: {}", uuid).as_str()).await?;

    sleep(Duration::from_secs(10)).await;
    Ok(())
}
