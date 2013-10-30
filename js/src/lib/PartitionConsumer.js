// This file has been generated from coffee source files

var Consumer, PartitionConsumer, Readable, bignum, _,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

Readable = require('stream').Readable;

bignum = require('bignum');

Consumer = require('prozess').Consumer;

_ = require('underscore');

/*
  Utility function to create properties for a class
*/


Function.prototype.property = function(prop, desc) {
  return Object.defineProperty(this.prototype, prop, desc);
};

/*
  Consumer for a specific topic partition. Wraps the underlying Prozess client.
*/


module.exports = PartitionConsumer = (function(_super) {
  __extends(PartitionConsumer, _super);

  /*
    Current offset, as reported by Prozess client
  */


  PartitionConsumer.property('offset', {
    get: function() {
      if (this.consumer) {
        return bignum(this.consumer.offset).toString();
      } else {
        return null;
      }
    }
  });

  /*
    Constructs the partition consumer
  */


  function PartitionConsumer(topicConsumer, topicPartition, options) {
    PartitionConsumer.__super__.constructor.call(this, {
      objectMode: true
    });
    options = _.defaults(options || {}, {
      consumeOnConnected: true
    });
    this.topicPartition = topicPartition;
    this.partitionDetails = {};
    this.zooKafka = topicConsumer.zooKafka;
    this.consumerGroup = topicConsumer.consumerGroup;
    this.consumer = null;
    this.consumerConfig = {
      maxMessageSize: options.maxMessageSize,
      polling: options.polling
    };
    this.latestOffset = null;
    this._defineHandlers(options);
    if (options.consumeOnConnected) {
      this.on('connected', this.consumeNext);
    }
  }

  /*
    Sets up default event handlers (part of construction).
  */


  PartitionConsumer.prototype._defineHandlers = function(options) {
    this.onNoMessages = options.onNoMessages || function(this_) {
      return setTimeout(function() {
        return this_.consumeNext();
      }, options.noMessagesTimeout || 2000);
    };
    this.onOffsetOutOfRange = options.onOffsetOutOfRange || function(this_) {
      return this_.consumer.getLatestOffset(function(error, offset) {
        if (error) {
          return this_.fatal('retrieving latest offset', error);
        }
        return this_._registerConsumerOffset(offset, function() {
          return this_.consumeNext();
        });
      });
    };
    return this.onConsumptionError = options.onConsumptionError || function(this_, error) {
      return this_.fatal('on consumption', error);
    };
  };

  /*
    Connects to Kafka
  */


  PartitionConsumer.prototype.connect = function() {
    var _this = this;
    return this._getPartitionConnectionAndOffsetDetails(function(error, details) {
      var config;
      if (error) {
        return _this.fatal('retrieving connection details', error);
      }
      _this.partitionDetails = details;
      config = {
        host: details.broker.host,
        port: details.broker.port,
        topic: details.topic,
        partition: details.partitionId,
        offset: details.offset,
        maxMessageSize: _this.consumerConfig.maxMessageSize,
        polling: _this.consumerConfig.polling
      };
      _this.consumer = new Consumer(config);
      _this.emit('connecting', config);
      return _this.consumer.connect(function(error) {
        if (error) {
          return _this.fatal('connecting', error);
        }
        return _this.emit('connected', config);
      });
    });
  };

  /*
    Disconnects from Kafka
  */


  PartitionConsumer.prototype.disconnect = function() {
    return this.consumer = null;
  };

  /*
    Shut down consumer
  */


  PartitionConsumer.prototype.shutdown = function() {
    this.disconnect();
    return this.push(null);
  };

  /*
    Utility function
  */


  PartitionConsumer.prototype._getPartitionConnectionAndOffsetDetails = function(onData) {
    return this.zooKafka.getPartitionConnectionAndOffsetDetails(this.consumerGroup, this.topicPartition, onData);
  };

  /*
    Utility function
  */


  PartitionConsumer.prototype._registerConsumerOffset = function(offset, onReady) {
    this.zooKafka.registerConsumerOffset(this.consumerGroup, this.topicPartition, offset, onReady);
    return this.emit('offsetUpdate', offset, this.partitionDetails.offset);
  };

  /*
    Dummy implementation of stream.Readable#_read
  */


  PartitionConsumer.prototype._read = function() {};

  /*
    Consume from Kafka.
  
    Kafka 0.7 is currently polling only.
  */


  PartitionConsumer.prototype.consumeNext = function() {
    var _this = this;
    this.emit('consuming');
    this.latestOffset = this.offset;
    return this.consumer.consume(function(error, messages) {
      var meta;
      meta = _.extend({}, _this.topicPartition, {
        offset: {
          previous: _this.latestOffset,
          current: _this.offset
        }
      });
      _this.emit('consumed', error, messages, meta);
      if (error) {
        if (error.message !== 'OffsetOutOfRange') {
          return _this.onConsumptionError(_this, error);
        }
        _this.emit('offsetOutOfRange', _this.partitionDetails.offset);
        return _this.onOffsetOutOfRange(_this);
      }
      if (messages.length === 0) {
        return _this.onNoMessages(_this);
      }
      return _this.push({
        messages: messages,
        meta: meta
      });
    });
  };

  /*
    Utility method
  */


  PartitionConsumer.prototype.fatal = function(msg, detail) {
    return this.emit('error', msg, detail);
  };

  return PartitionConsumer;

})(Readable);

/*
//@ sourceMappingURL=PartitionConsumer.js.map
*/