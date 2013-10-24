// This file has been generated from coffee source files

var Brokers, Consumer, ERR_InvalidFetchSize, ERR_InvalidMessage, ERR_OffsetOutOfRange, ERR_Other, ERR_Unknown, ERR_WrongPartition, EventEmitter, Message, Producer, toArray, toListOfMessages, _,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

EventEmitter = require('events').EventEmitter;

_ = require('underscore');

Message = require('prozess').Message;

ERR_Unknown = new Error("Unknown");

ERR_OffsetOutOfRange = new Error("OffsetOutOfRange");

ERR_InvalidMessage = new Error("InvalidMessage");

ERR_WrongPartition = new Error("WrongPartition");

ERR_InvalidFetchSize = new Error("InvalidFetchSize");

ERR_Other = new Error("Unknown error code: 99");

exports.Brokers = Brokers = (function() {
  var Broker, brokers;

  function Brokers() {}

  brokers = {};

  Broker = (function() {
    function Broker() {
      this.queues = {};
    }

    Broker.prototype.write = function(topic, partition, messages) {
      var key, message, _i, _len, _results;
      key = "" + topic + "-" + partition;
      if (!key in queues) {
        this.queues[key] = [];
      }
      _results = [];
      for (_i = 0, _len = messages.length; _i < _len; _i++) {
        message = messages[_i];
        _results.push(this.queues[key].push(message));
      }
      return _results;
    };

    Broker.prototype.read = function(topic, partition, offset, maxMessageSize, onData) {
      var item, key, length,
        _this = this;
      key = "" + topic + "-" + partition;
      if (offset(function() {
        return _this.queues[key].length;
      })) {
        return onData(ERR_OffsetOutOfRange);
      }
      length = 0;
      batch([]);
      while (offset < this.queues[key].length && length < maxMessageSize) {
        item = this.queues[key][offset];
        batch.push(item);
        offset++;
        length = length + item.byteLength;
      }
      return onData(null, batch);
    };

    Broker.prototype.getLatestOffset = function(topic, partition) {
      var key;
      key = "" + topic + "-" + partition;
      return this.queues[key].length;
    };

    return Broker;

  })();

  Brokers.connect = function(host, port) {
    var broker;
    broker = "" + host + ":" + port;
    if (!(broker in brokers)) {
      brokers[broker] = new Broker();
    }
    return brokers[broker];
  };

  Brokers.getBrokers = function() {
    return brokers;
  };

  Brokers.reset = function() {
    return brokers = {};
  };

  return Brokers;

})();

exports.Producer = Producer = (function(_super) {
  __extends(Producer, _super);

  function Producer(topic, options) {
    options = options || {};
    this.topic = topic;
    this.partition = options.partition || 0;
    this.host = options.host || 'localhost';
    this.port = options.port || 9092;
    this.broker = null;
    this.connection = null;
  }

  Producer.prototype.connect = function() {
    this.broker = Brokers.connect(this.host, this.port);
    return this.emit('connect');
  };

  Producer.prototype.send = function(message, options, cb) {
    var messages;
    if (arguments.length === 2) {
      cb = options;
      options = {};
    }
    options.partition = options.partition || this.partition;
    options.topic = options.topic || this.topic;
    messages = toListOfMessages(toArray(messages));
    return this.broker.write(options.topic, options.partition, messages);
  };

  return Producer;

})(EventEmitter);

exports.Consumer = Consumer = (function() {
  function Consumer(options) {
    options = options || {};
    this.topic = options.topic || 'test';
    this.partition = options.partition || 0;
    this.host = options.host || 'localhost';
    this.port = options.port || 9092;
    this.offset = options.offset || 0;
    this.maxMessageSize = options.maxMessageSize || 1024 * 1024;
    this.polling = options.polling || 2;
  }

  Consumer.prototype.connect = function() {
    return this.broker = Brokers.connect(this.host, this.port);
  };

  Consumer.prototype.consume = function(cb) {
    return this.broker.read(this.topic, this.partition, this.offset, this.maxMessageSize, function(err, messages) {
      if (err) {
        return cb(err);
      }
      this.offset = this.offset + messages.length;
      return cb(null, messages);
    });
  };

  Consumer.prototype.getLatestOffset = function(cb) {
    return cb(null, this.brokers.getLatestOffset(this.topic, this.partition));
  };

  return Consumer;

})();

toArray = function(arg) {
  if (_.isArray(arg)) {
    return arg;
  }
  return [arg];
};

toListOfMessages = function(args) {
  return _.map(args, function(arg) {
    if (arg instanceof Message) {
      return arg;
    }
    return new Messagearg;
  });
};

/*
//@ sourceMappingURL=FakeProzess.js.map
*/