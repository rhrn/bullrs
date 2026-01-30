const Queue = require('bull')
const crypto = require('crypto')

const testQueue = new Queue('test-queue', 'redis://localhost:26379')

;(async () => {

  testQueue.process(async data => {
    // await new Promise(resolve => setTimeout(resolve, 100))
    console.log('data:', data)
  })

  const uuid = crypto.randomUUID()
  await testQueue.add({ uuid })

  console.log({ uuid })
})()
