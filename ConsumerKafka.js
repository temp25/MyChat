const fs = require('fs')
//const ip = require('ip')

const { Kafka, logLevel } = require('kafkajs')

console.log("timestamp : "+(new Date()).getTime());

//const host = process.env.HOST_IP || ip.address()

console.log("\nbrokers env : "+process.env.CLOUDKARAFKA_BROKERS+"\n");

const brokers = process.env.CLOUDKARAFKA_BROKERS.split(',');

console.log("\nbrokers array : "+brokers+"\n");

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: brokers, //[`${host}:9094`],
  clientId: 'example-consumer',
  ssl: {
    rejectUnauthorized: true
  },
  sasl: {
    mechanism: 'scram-sha-256',
    username: process.env.CLOUDKARAFKA_USERNAME, //'test',
    password: process.env.CLOUDKARAFKA_PASSWORD, //'testtest',
  },
})

console.log("\nusername : "+process.env.CLOUDKARAFKA_USERNAME+"\n");
console.log("\npassword : "+process.env.CLOUDKARAFKA_PASSWORD+"\n");

const topic = `${process.env.CLOUDKARAFKA_USERNAME}-default`; //'topic-test'
const gId = `${process.env.CLOUDKARAFKA_USERNAME}-consumer`;

console.log("\n topic : "+topic+"\n");
console.log("\n groupId : "+gId+"\n");

const consumer = kafka.consumer({ groupId: gId, fromBeginning: true });

const run = async () => {
  await consumer.connect()
  const groupMetadata = await consumer.describeGroup()
  console.log("\n\ngroupMetadata :\n"+groupMetadata+"\n\n");
  await consumer.subscribe({ topic: topic, fromBeginning: true })
  await consumer.run({
    partitionsConsumedConcurrently: 5,
    //eachBatch: async ({ batch }) => {
    //  console.log(batch)
    //},
    
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`\neachMessage: - ${prefix} ${message.key}#${message.value}\n`)
    },
    
  })
  
    //consumer.seek({ topic: topic, partition: 0, offset: 0 });
    //consumer.seek({ topic: topic, partition: 1, offset: 0 });
    //consumer.seek({ topic: topic, partition: 2, offset: 0 });
    //consumer.seek({ topic: topic, partition: 3, offset: 0 });
    //consumer.seek({ topic: topic, partition: 4, offset: 0 });
    
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})