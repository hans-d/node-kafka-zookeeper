###
  A rebalance strategy determines what topic partitions should be consumed for a given
  topic by a consumer in consumer group. The {@link TopicConsumer} is responsible for
  the actual rebalancing (connect and disconnect) of the provided topic partitions.

  Initiate providing partitions via the 'partitions' event.
###

###
  Consumer strategy to connect to all available partitions as registered in zookeeper
  assuming there are no other consumers (thus no distributed read).

  Currently only gets the registered topic partition once.

  TODO: watch zookeeper for changes, and emit new set of partitions
###
exports.standAlone = () ->

    zooKafka = @connections.zooKafka
    topic = @topic

    zooKafka.getRegisteredTopicPartitions topic, (err, partitions) =>
      return @emit 'error', err if err
      @emit 'partitions', partitions
