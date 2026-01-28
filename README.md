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
