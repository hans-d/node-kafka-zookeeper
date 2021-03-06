async = require 'async'
_ = require 'underscore'

###
  Navigating the zookeeper registry for Kafka, to be compliant with the official Kafka client

  v0.7 zookeeper structure, based on
  - [Kafka wiki](https://cwiki.apache.org/confluence/display/KAFKA/Writing+a+Driver+for+Kafka), Sep 2013
  - [Kafka source](https://github.com/apache/kafka/blob/0.7/core/src/main/scala/kafka/consumer/ZookeeperConsumerConnector.scala)

  General:
    - brokerId   : number, configured on broker
         eg: 2
    - creator    : assigned name, <ip>-<epoch stamp>
         eg: 10.0.0.12-1324306324402
    - groupId    : alphanumeric, app
         eg: cheeseLovers
    - topic      : alphanumeric
         eg: cheese
    - consumerId : alphanumeric, configured on consumer
         eg: mouse-1
    - partitionId: number
         eg: 4
   
    Broker registration:
    - path:  /brokers/ids/[brokerId]
    - value: [creator]:[host]:[port]
    - eg:    /brokers/ids/2 => 10.0.0.12-1324306324402:10.0.0.12:9092
   
    Broker topic registration:
    - path:  /brokers/topics/[topic]/[brokerId]
    - value: [numberOfPartitionsOnBroker]
    - eg:    /brokers/topics/cheese/2 => 4
   
    Consumer registration:
    - path:  /consumers/[groupId]/ids/[consumerId]
    - eg:    /consumers/cheeseLovers/ids/mouse-1
   
    Consumer topic registration:
    - path:  /consumers/[groupId]/ids/[consumerId]/[topic]
    - eg:    /consumers/cheeseLovers/ids/mouse-1/cheese
   
    Consumer owner tracking:
    - path:  /consumers/[groupId]/owner/[topic]/[brokerId]-[partitionId]
    - value: [consumerId]
    - eg:    /consumers/cheeseLovers/owner/cheese/2-4 => mouse-1
    - changes on re-balancing
   
    Consumer offset tracking:
    - path:  /consumers/[groupId]/offsets/[topic]/[broker_id-partition_id]
    - value: [offsetCounterValue]
    - eg:    /consumers/cheeseLovers/offsets/cheese/2-4 => 1024
   
    Producers (existing topic):
     1. Producer created per topic
     2. Read /brokers/ids/[brokerId], map [brokerId] -> Kafka connection
     3. Read /brokers/topics/[topic]/[brokerId], map [brokerId-PartitionId] -> broker connection
     4. On send: pick brokerId-PartitionId
    Watch:
     - add/delete child: /brokers/ids
     - change value: /brokers/topics/[topic]/[brokerId]
   
    New topic on broker:
     - /brokers/topics/[topic]/[brokerId] does not exist yet
     - send to partition 0
     - /brokers/topics/[topic]/[brokerId] will be updated
   
    Consumers:
      not always registered in zookeeper, eg when directly using Prozess
      normal java client behaviour, which we should follow:
      1. register
      2. trigger re-balance?
   
    Expired session:
    - release ownership, re-register,re-balance
   
    Re-balancing, as done by Java client
    - All Consumers in a ConsumerGroup will come to a consensus as to who is consuming what.
    - Each Broker+Topic+Partition combination is consumed by one and only one Consumer
         even if it means that some Consumers don't get anything at all.
    - A Consumer should try to have as many partitions on the same Broker as possible,
         sort the list by [Broker ID]-[Partition] (0-0, 0-1, 0-2, etc.), and assign them in chunks.
    - Consumers are sorted by their Consumer IDs.
         If there are three Consumers, two Brokers, and three partitions in each, the split might look like:
             Consumer A: [0-0, 0-1]
             Consumer B: [0-2, 1-0]
             Consumer C: [1-1, 1-2]
    - If the distribution can't be even and some Consumers must have more partitions than others,
         the extra partitions always go to the earlier consumers on the list.
         So you could have a distribution like 4-4-4-4 or 5-5-4-4, but never 4-4-4-5 or 4-5-4-4.

  TODO: cache data, watch for changes
###

module.exports = class ZooKafka


  ###
    Constructs the object, done by {@link Kafkazoo}

    @param {Object} client Zookeeper client
    @praram [options="{}"]
  ###
  constructor: (client, options) ->
    @client = client;
    @root = ''


  ###
    Returns all registered (active) brokers with their connection details

    @param {Function} onData Callback
    @param onData.error
    @param {Object} onData.brokers
    @param onData.brokers.[brokerId]
    @param onData.brokers.[brokerId].name
    @param onData.brokers.[brokerId].host
    @param onData.brokers.[brokerId].port
    @param onData.brokers.[brokerId].id
  ###
  getAllRegisteredBrokers: (onData) ->
    zkPath = '/brokers/ids'
    brokerMap = {}

    @client.getChildren zkPath, getChildData: true, (error, brokerDetails) =>
      return onData msg: 'Error retrieving brokers', error: error if error

      for brokerId, details of brokerDetails
        brokerMap[brokerId] = _.extend _.object(['name', 'host', 'port'], details.split ':'), id: brokerId

      onData null, brokerMap


  ###
    Returns connection details for a given broker

    @param {String} brokerId
    @param {Function} onData Callback
    @param onData.error
    @param {Object} onData.broker
    @param onData.broker.name
    @param onData.broker.host
    @param onData.broker.port
    @param onData.broker.id
  ###

  getRegisteredBroker: (brokerId, onData) ->
    zkPath = ['/brokers/ids', brokerId]

    @client.get zkPath, (error, stat, details) =>
      return onData msg: 'Error retrieving broker', error: error if error
      onData null,  _.extend _.object(['name', 'host', 'port'], details.split ':'), id: brokerId


  ###
    Returns all registered topics (names only)

    @param {Function} onData Callback
    @param onData.error
    @param {String[]} onData.topics List of topic names
  ###
  getAllRegisteredTopics: (onData) ->
    zkPath = '/brokers/topics'

    @client.getChildren zkPath, (error, topics) =>
      return onData msg: 'Error retrieving topics', error: error if error
      onData null, topics


  ###
    Get all registered (active) brokers for topic
  ###
  getRegisteredTopicBrokers: (topic, options, onData) ->
    # options optional
    if !onData && options && _.isFunction(options)
      onData = options
      options = {}

    options = _.defaults options || {}

    zkPath = ['/brokers/topics', topic]
    partitionMap = {};

    async.parallel

      activeBrokers: (asyncReady) =>
        @getAllRegisteredBrokers asyncReady

      topicBrokers: (asyncReady) =>
        @client.getChildren zkPath, (error, brokers) =>
          return onData msg: 'Error retrieving topic partitions', error: error if error
          asyncReady null, brokers

    , (error, result) =>
      return onData error if (error)

      brokers = result.activeBrokers.filter (broker) ->
        return broker in result.topicBrokers

      onData null, brokers

  ###
    Returns all registered topic partitions, by default only for registered brokers

    @param {String} topic
    @param {Object} [options="{onlyRegisteredBrokers: true}"]
    @param {Boolean} options.onlyRegisteredBrokers
    @param {Function} onData Callback
    @param onData.error
    @param {Object} onData.topicPartitions
    @param onData.topicPartitions.[brokerId-ParttionId]
    @param onData.topicPartitions.[brokerId-ParttionId].topic
    @param onData.topicPartitions.[brokerId-ParttionId].brokerPartitionId
    @param onData.topicPartitions.[brokerId-ParttionId].brokerId
    @param onData.topicPartitions.[brokerId-ParttionId].PartitionId
  ###
  getRegisteredTopicPartitions: (topic, options, onData) ->
    # optional options
    if !onData && options && _.isFunction(options)
        onData = options
        options = {}

    options = _.defaults options || {},
        onlyRegisteredBrokers: true

    zkPath = ['/brokers/topics', topic]
    partitionMap = {};

    async.parallel

      brokers: (asyncReady) =>
        return asyncReady null, null unless options.onlyRegisteredBrokers
        @getAllRegisteredBrokers asyncReady

      partitions: (asyncReady) =>
        @client.getChildren zkPath, getChildData: true, (error, brokers) =>
          return onData msg: 'Error retrieving topic partitions', error: error if error
          asyncReady null, brokers

    , (error, result) =>
      return onData error if (error)

      brokers = result.partitions
      brokers = _.pick result.partitions, _.keys result.brokers if options.onlyRegisteredBrokers

      for brokerId, partitions of brokers
        for partitionId in _.range Number partitions
          id = "#{brokerId}-#{partitionId}"
          partitionMap[id] =
              topic: topic
              brokerPartitionId: id
              brokerId: brokerId
              partitionId: partitionId

      onData null, partitionMap


  ###
    Returns registered consumer offset for given topicPartition and consumerGroup

    If no offset is registered, 0 is returned.

    @param {String} consumerGroup
    @param {Object} topicPartition Normally use the output from #getRegisteredTopicPartitions
    @param {String} topicPartition.topic
    @param {String} topicPartition.brokerPartitionId
    @param {Function} onData Callback
    @param onData.error
    @param {Object} onData.registeredOffset
    @param {String} onData.registeredOffset.consumerGroup As provided
    @param {Object} onData.registeredOffset.topicPartition As provided
    @param {String} onData.registeredOffset.offset
  ###
  getRegisteredConsumerOffset: (consumerGroup, topicPartition, onData) ->
    zkPath = ['/consumers', consumerGroup, 'offsets', topicPartition.topic, topicPartition.brokerPartitionId]

    @client.get zkPath, createPathIfNotExists:true, (error, stat, offset) ->
      return onData msg: 'Error retrieving consumer offset', error: error  if error && error.msg != 'no-node'
      onData null, _.extend consumerGroup: consumerGroup, offset: offset || '0', topicPartition


  ###
    Returns registered consumer offset for given topicPartition and consumerGroup, and the
    broker connection details for the topicPartition

    If no offset is registered, 0 is returned.

    @param {String} consumerGroup
    @param {Object} topicPartition Normally use the output from #getRegisteredTopicPartitions
    @param {String} topicPartition.topic
    @param {String} topicPartition.brokerPartitionId
    @param {String} topicPartition.brokerId
    @param {Function} onData Callback
    @param onData.error
    @param {Object} onData.details
    @param {Object} onData.details.topicPartition As provided
    @param {Object} onData.details.broker From #getRegisteredBroker
    @param {Object} onData.details.consumerOffset From #getRegisteredConsumerOffset
  ###
  getPartitionConnectionAndOffsetDetails: (consumerGroup, topicPartition, onData) ->
    async.parallel

      consumerOffset: (asyncReady) =>
        @getRegisteredConsumerOffset consumerGroup, topicPartition, asyncReady

      broker: (asyncReady) =>
        @getRegisteredBroker topicPartition.brokerId, asyncReady

    , (error, result) =>
        return onData error if error
        onData null, _.extend broker: result.broker, topicPartition, result.consumerOffset


  ###
    Registers the provided offset for the given consumerGroup and topicPartition

    @param {String} consumerGroup
    @param {Object} topicPartition Normally use the output from #getRegisteredTopicPartitions
    @param {String} topicPartition.topic
    @param {String} topicPartition.brokerPartitionId
    @param {String} offset The new offset value
    @param {Function} onReady Callback
    @param onReady.error
  ###
  registerConsumerOffset: (consumerGroup, topicPartition, offset, onReady) ->
    zkPath = ['/consumers', consumerGroup, 'offsets', topicPartition.topic, topicPartition.brokerPartitionId]

    @client.createOrUpdate zkPath, offset, (error) =>
      return onReady msg: 'Error registering offset', error: error if error
      onReady()
