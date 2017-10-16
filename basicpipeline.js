var http = require('http');
var fs = require('fs');
var formidable = require("formidable");
var util = require('util');

const express = require('express')
const app = express()

// Imports the Google Cloud client library
const PubSub = require('@google-cloud/pubsub');

// Your Google Cloud Platform project ID
const projectId = 'ymg-basic-pipeline';
// PubSub topic name
const topicName = 'test-topic';

//function to create form
function displayForm(res) {
  fs.readFile('form.html', function (err, data) {
      res.writeHead(200, {
          'Content-Type': 'text/html',
              'Content-Length': data.length
      });
      res.write(data);
      res.end();
  });
}

//function to process form
function processAllFieldsOfTheForm(req, res) {
  var form = new formidable.IncomingForm();

  form.parse(req, function (err, fields) {
      //Store the data from the fields in your data store.
      //The data store could be a file or database or any other store based
      //on your application.
      res.writeHead(200, {
          'content-type': 'text/plain'
      });
      console.log('received data: ' + fields);
      res.write('received the data:\n\n');
      publishMessage(topicName, fields.message);
      res.end(util.inspect({
          "message received" : fields.message
      }));
  });
}

// [START pubsub_publish_message]
function publishMessage (topicName, data) {
    // Instantiates a client
    const pubsub = PubSub();
  
    // References an existing topic, e.g. "my-topic"
    const topic = pubsub.topic(topicName);
  
    // Create a publisher for the topic (which can include additional batching configuration)
    const publisher = topic.publisher();
  
    // Publishes the message as a string, e.g. "Hello, world!" or JSON.stringify(someObject)
    const dataBuffer = Buffer.from(data);
    return publisher.publish(dataBuffer)
      .then((results) => {
        const messageId = results[0];
        console.log(`Message ${messageId} published.`);
        return messageId;
      });
  }
  // [END pubsub_publish_message]
  

app.get('/', function (req, res) {
  displayForm(res);
  //publishMessage(topicName, "test");
})

app.post('/', function (req, res) {
  processAllFieldsOfTheForm(req, res);
})

app.listen(8080, function () {
  console.log('Example app listening on port 8080!')
})