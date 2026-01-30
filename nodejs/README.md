#### repl access to test-queue
```
npx bull-repl connect -u redis://0.0.0.0:26379 --prefix bull test-queue
```

#### node commands
```
node add_job.js
node process.js
node --inspect-brk=9292 process.js
```
