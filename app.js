var http = require('http');
var fileSystem = require('fs');
const { Kafka, logLevel } = require('kafkajs');

http.createServer(onRequest).listen(8888);
console.log('Server has started');

const kafka = new Kafka({
    clientId: 'mychat',
    brokers: [
        'velomobile-01.srvs.cloudkafka.com:9094',
        'velomobile-02.srvs.cloudkafka.com:9094',
        'velomobile-03.srvs.cloudkafka.com:9094'
    ],
    sasl: {
        mechanism: 'scram-sha-256', //plain or // scram-sha-256 or scram-sha-512
        username: '19uds2d2',
        password: '48yZeR87btROThUIxvSzmooG4v7QZ3Pe'
    },
    logLevel: logLevel.INFO
})

kafka.logger().setLogLevel(logLevel.INFO)

const producer = kafka.producer()
const consumer = kafka.consumer({groupId: '19uds2d2-consumer'});

producer.logger().setLogLevel(logLevel.INFO)
consumer.logger().setLogLevel(logLevel.INFO)

const run = async() => {
    //Producing
    /* await producer.connect()
    await producer.send({
        topic: 'chatApp-v1',
        messages: [
            //{value: 'Hello KafkaJS user!'},
            {value: 'Timestamp : '+(new Date()).toLocaleString()},
        ],
    }) */

    //Consuming
    await consumer.connect()
    await consumer.subscribe({
        topic: '19uds2d2-default',
        fromBeginning: true
    })

    await consumer.run({
        //eachMessage: async ({topic, partition, message }) => {
            /* console.log({
                partition,
                offset: message.offset,
                value: message.value.toString(),
            }) */

            //console.log("Message from  : "+message.value.toString());
        //},
        eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning }) => {
            for (let message of batch.messages) {
                /* console.log({
                    topic: batch.topic,
                    partition: batch.partition,
                    highWatermark: batch.highWatermark,
                    message: {
                        offset: message.offset,
                        key: message.key.toString(),
                        value: message.value.toString(),
                        headers: message.headers,
                    }
                }) */

                console.log("Batch Message : \nkey: " + message.key + "\tvalue : " + message.value.toString())

                await resolveOffset(message.offset)
                await heartbeat()
            }
        },
    })
}

run()//.catch(console.error)

function onRequest(request, response) {
    /* response.writeHead(200);
    response.write('Hello Noders');
    response.end(); */
    
    //console.log('request url '+request.url);
    fileSystem.readFile('./index.html', function(err, htmlContent){
        response.writeHead(200, {
            'Content-Type': 'text/html'
        });
        response.write(htmlContent+'');
        response.end();
    });
}