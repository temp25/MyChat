const fs = require('fs')
const { Kafka, logLevel } = require('kafkajs')

console.log("timestamp : "+(new Date()).getTime());
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

const producer = kafka.producer();

const getRandomNumber = () => Math.round(Math.random(10) * 1000)
const createMessage = num => ({
  key: `key-${num}`,
  value: `value-${num}-${new Date().toISOString()}`,
})

const sendMessage = () => {
  return producer
    .send({
      topic,
      //compression: CompressionTypes.GZIP,
      messages: Array(getRandomNumber())
        .fill()
        .map(_ => createMessage(getRandomNumber())),
    })
    .then(console.log)
    .catch(e => console.error(`[example/producer] ${e.message}`, e))
}

const run = async () => {
  await producer.connect()
  //setInterval(sendMessage, 3000)
  sendMessage();
}

run().catch(e => console.error(`[example/producer] ${e.message}`, e))
