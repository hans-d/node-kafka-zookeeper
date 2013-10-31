// This file has been generated from coffee source files

/*
  A rebalance strategy determines what topic partitions should be consumed for a given
  topic by a consumer in consumer group. The {@link TopicConsumer} is responsible for
  the actual rebalancing (connect and disconnect) of the provided topic partitions.

  Initiate providing partitions via the 'partitions' event.
*/

/*
  Consumer strategy to connect to all available partitions as registered in zookeeper
  assuming there are no other consumers (thus no distributed read).

  Currently only gets the registered topic partition once.

  TODO: watch zookeeper for changes, and emit new set of partitions
*/

exports.standAlone = function() {
  var topic, zooKafka,
    _this = this;
  zooKafka = this.connections.zooKafka;
  topic = this.topic;
  return zooKafka.getRegisteredTopicPartitions(topic, function(err, partitions) {
    if (err) {
      return _this.emit('error', err);
    }
    return _this.emit('partitions', partitions);
  });
};

/*
//@ sourceMappingURL=rebalanceStrategy.js.map
*/