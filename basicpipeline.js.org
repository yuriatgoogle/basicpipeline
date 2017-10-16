const express = require('express')
const app = express()

// Imports the Google Cloud client library
const PubSub = require('@google-cloud/pubsub');

// Your Google Cloud Platform project ID
const projectId = 'ymg-basic-pipeline';
// PubSub topic name
const topicName = 'test-topic';

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
  res.send('Hello World!')
  publishMessage(topicName, "test");
})

app.listen(8080, function () {
  console.log('Example app listening on port 8080!')
})