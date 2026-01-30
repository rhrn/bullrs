const Queue = require('bull')

const testQueue = new Queue('test-queue', 'redis://127.0.0.1:26379')

;(async () => {

  testQueue.process(async data => {
    // await new Promise(resolve => setTimeout(resolve, 100))
    console.log('data:', data)
  })

})()
