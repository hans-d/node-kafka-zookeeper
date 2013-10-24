kafkazoo
=======

A high-level client library in Node.js for the Apache Kafka project with Zookeeper integration


[Kafka](http://incubator.apache.org/kafka/index.html) is a persistent, efficient, distributed publish/subscribe
messaging system.

[kafka-zookeeper](https://github.com/devoncrouse/node-kafka-zookeeper.git) is the original module where this is
based on, but completely rewritten and moved to coffeescript (but delivery is still .js)

###Consumer example:

A `Kafkazoo` object handles broker enumeration and offset storage
```javascript
var Kafkazoo = require('kafka-zookeeper').Kafkazoo;
var kafka = new Kafkazoo({
  host: 'localhost',
  port: 2181,
  zkPath: '/'
});

var onMessages = function (error, messages, acknowledge) {
    if (error) return log.error(error);
    // log some details
    log.info('Received %d messages', messages.length);
    log.debug(messages[0].substring(0, 100) + '...');

    // and get next batch
    acknowledge(true); // false will resend the same messages after a delay
};

// Start consuming
kafka.consume('MessageHeaders', 'dcrouse', onMessages);

// Stop consuming

```

###Utility examples:

The `Kafkazoo` object also exposes some utility functions - used internally and useful for testing
```javascript
var _ = require('underscore');
var Kafkazoo = require('kafka-zookeeper');

var kafka = new Kafkazoo({
  host: 'localhost',
  port: 2181
});

var topic = 'KafkaTopic', group = 'ConsumerGroup';

// Retrieve all consumer offsets for topic/group
var onConsumerOffsets = function(offsets, error) {
  if (error) return console.error('onConsumerOffsets', error);
  console.log('Offsets', offsets);
};

kafka.getConsumerOffsets(topic, group, onConsumerOffsets);

// Initialize consumer offsets
var onInitializeConsumerOffsets = function(error) {
  if (error) return console.error('onInitializeConsumerOffsets', error);
  console.log('Consumer offsets initialized');
};

kafka.initializeConsumerOffsets(topic, group, onInitializeConsumerOffsets);
```

###Installation:

  npm install kafka-zookeeper

###Checkout the code and run the tests:

  git clone https://github.com/devoncrouse/node-kafka-zookeeper.git  
  cd node-kafka-zookeeper; npm test

