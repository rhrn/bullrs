### redis
```
docker-compose -f docker/redis.yaml up -d
```

### watch on commands
```
docker exec -it bull_rs_redis_1 redis-cli monitor
```

### test
```
cargo test commands::main::tests::exec_lua_add_job_case_1 --  --exact --show-output
```
