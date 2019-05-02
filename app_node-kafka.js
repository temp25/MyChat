var http = require('http');
var fileSystem = require('fs');
var kafka = require('kafka-node');
var kafkaClient = new kafka.KafkaClient({
    clientId: 'mychat',
    kafkaHost: 'localhost:9092,localhost:9093,localhost:9094',
});

var consumer = new kafka.Consumer(
    kafkaClient,
    [
        { topic: 'chatApp-v1', }
    ],
    {
        autoCommit: false
    }
);

consumer.on('message', function(message){
    console.log(message);
});

http.createServer(onRequest).listen(8888);
console.log('Server has started');

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