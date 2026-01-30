### reference
See `REFERENCE.md` for the Rust API surface.

### quick start
```rust
use bullrs::queue::Queue;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Queue::new("example", "redis://127.0.0.1:6379").await?;

    let handle = queue.clone().process(|data: String| async move {
        format!("processed: {}", data)
    }).await?;

    queue.add("hello").await?;

    handle.close().await?;
    Ok(())
}
```

### redis
```
docker-compose -f docker/redis.yaml up -d
```

### watch on commands
```
docker exec -it bull_rs_redis_1 redis-cli monitor
```

### run examples
```
cargo run --example simple --  --show-output
cargo run --example process --  --show-output
cargo run --example add_job --  --show-output
```

### run test
```
cargo test
cargo test commands::main::tests::exec_lua_add_job_case_1 --  --exact --show-output
cargo test job::tests::new_job --  --exact --show-output
```
