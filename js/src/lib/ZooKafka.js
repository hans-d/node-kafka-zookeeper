// This file has been generated from coffee source files

var ZooKafka, async, _;

async = require('async');

_ = require('underscore');

module.exports = ZooKafka = (function() {
  function ZooKafka(client, options) {
    this.client = client;
    this.root = '';
  }

  ZooKafka.prototype.getAllRegisteredBrokers = function(onData) {
    var brokerMap, zkPath,
      _this = this;
    zkPath = '/brokers/ids';
    brokerMap = {};
    return this.client.getChildren(zkPath, {
      getChildData: true
    }, function(error, brokerDetails) {
      var brokerId, details;
      if (error) {
        return onData({
          msg: 'Error retrieving brokers',
          error: error
        });
      }
      for (brokerId in brokerDetails) {
        details = brokerDetails[brokerId];
        brokerMap[brokerId] = _.extend(_.object(['name', 'host', 'port'], details.split(':')), {
          id: brokerId
        });
      }
      return onData(null, brokerMap);
    });
  };

  ZooKafka.prototype.getRegisteredBroker = function(brokerId, onData) {
    var zkPath,
      _this = this;
    zkPath = ['/brokers/ids', brokerId];
    return this.client.get(zkPath, function(error, stat, details) {
      if (error) {
        return onData({
          msg: 'Error retrieving broker',
          error: error
        });
      }
      return onData(null, _.extend(_.object(['name', 'host', 'port'], details.split(':')), {
        id: brokerId
      }));
    });
  };

  ZooKafka.prototype.getAllRegisteredTopics = function(onData) {
    var zkPath,
      _this = this;
    zkPath = '/brokers/topics';
    return this.client.getChildren(zkPath, function(error, topics) {
      if (error) {
        return onData({
          msg: 'Error retrieving topics',
          error: error
        });
      }
      return onData(null, topics);
    });
  };

  ZooKafka.prototype.getRegisteredTopicPartitions = function(topic, options, onData) {
    var partitionMap, zkPath,
      _this = this;
    if (!onData && options && _.isFunction(options)) {
      onData = options;
      options = {};
    }
    options = _.defaults(options || {}, {
      onlyRegisteredBrokers: true
    });
    zkPath = ['/brokers/topics', topic];
    partitionMap = {};
    return async.parallel({
      brokers: function(asyncReady) {
        if (!options.onlyRegisteredBrokers) {
          return asyncReady(null, null);
        }
        return _this.getAllRegisteredBrokers(asyncReady);
      },
      partitions: function(asyncReady) {
        return _this.client.getChildren(zkPath, {
          getChildData: true
        }, function(error, brokers) {
          if (error) {
            return onData({
              msg: 'Error retrieving topic partitions',
              error: error
            });
          }
          return asyncReady(null, brokers);
        });
      }
    }, function(error, result) {
      var brokerId, brokers, id, partitionId, partitions, _i, _len, _ref;
      if (error) {
        return onData(error);
      }
      brokers = result.partitions;
      if (options.onlyRegisteredBrokers) {
        brokers = _.pick(result.partitions, _.keys(result.brokers));
      }
      for (brokerId in brokers) {
        partitions = brokers[brokerId];
        _ref = _.range(Number(partitions));
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          partitionId = _ref[_i];
          id = "" + brokerId + "-" + partitionId;
          partitionMap[id] = {
            topic: topic,
            brokerPartitionId: id,
            brokerId: brokerId,
            partitionId: partitionId
          };
        }
      }
      return onData(null, partitionMap);
    });
  };

  ZooKafka.prototype.getRegisteredConsumerOffset = function(consumerGroup, topicPartition, onData) {
    var zkPath;
    zkPath = ['/consumers', consumerGroup, 'offsets', topicPartition.topic, topicPartition.brokerPartitionId];
    return this.client.get(zkPath, {
      createPathIfNotExists: true
    }, function(error, stat, offset) {
      if (error && error.msg !== 'no-node') {
        return onData({
          msg: 'Error retrieving consumer offset',
          error: error
        });
      }
      return onData(null, _.extend({
        consumerGroup: consumerGroup,
        offset: offset || '0'
      }, topicPartition));
    });
  };

  ZooKafka.prototype.getPartitionConnectionAndOffsetDetails = function(consumerGroup, topicPartition, onData) {
    var _this = this;
    return async.parallel({
      consumerOffset: function(asyncReady) {
        return _this.getRegisteredConsumerOffset(consumerGroup, topicPartition, asyncReady);
      },
      broker: function(asyncReady) {
        return _this.getRegisteredBroker(topicPartition.brokerId, asyncReady);
      }
    }, function(error, result) {
      if (error) {
        return onData(error);
      }
      return onData(null, _.extend({
        broker: result.broker
      }, topicPartition, result.consumerOffset));
    });
  };

  ZooKafka.prototype.registerConsumerOffset = function(consumerGroup, topicPartition, offset, onReady) {
    var zkPath,
      _this = this;
    zkPath = ['/consumers', consumerGroup, 'offsets', topicPartition.topic, topicPartition.brokerPartitionId];
    return this.client.createOrUpdate(zkPath, offset, function(error) {
      if (error) {
        return onReady({
          msg: 'Error registering offset',
          error: error
        });
      }
      return onReady();
    });
  };

  return ZooKafka;

})();

/*
//@ sourceMappingURL=ZooKafka.js.map
*/