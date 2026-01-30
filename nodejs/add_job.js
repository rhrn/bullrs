const Queue = require('bull')
const crypto = require('crypto')

const testQueue = new Queue('test-queue', 'redis://localhost:26379')

;(async () => {
  const uuid = crypto.randomUUID()
  await testQueue.add({ uuid })

  console.log({ uuid })

  process.exit()
})()
