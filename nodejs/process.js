const Queue = require('bull')

const testQueue = new Queue('test-queue', 'redis://0.0.0.0:6379')

;(async () => {

  testQueue.process(async data => {
    // await new Promise(resolve => setTimeout(resolve, 100))
    console.log('data:', data)
  })

})()
