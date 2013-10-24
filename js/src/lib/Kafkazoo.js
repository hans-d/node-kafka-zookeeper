// This file has been generated from coffee source files

var EventEmitter, Kafkazoo, TopicConsumer, ZooKafka, zookeeper, _,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

EventEmitter = require('events').EventEmitter;

_ = require('underscore');

zookeeper = require('zookeeper-hd');

TopicConsumer = require('./TopicConsumer');

ZooKafka = require('./ZooKafka');

module.exports = Kafkazoo = (function(_super) {
  __extends(Kafkazoo, _super);

  function Kafkazoo(options) {
    options = _.defaults(options || {}, {
      zookeeper: {}
    });
    options.zookeeper = _.defaults(options.zookeeper, {
      connect: 'localhost:2181',
      root: '/',
      clientConfig: {}
    });
    options.zookeeper.clientConfig = _.defaults(options.zookeeper.clientConfig, {
      connect: options.zookeeper.connect,
      root: options.zookeeper.root
    });
    this._zookeeper = new zookeeper.PlusClient(options.zookeeper.clientConfig);
    this.config = _.omit(options, zookeeper);
    this.connections = function() {};
    this.connections.zooKafka = new ZooKafka(this._zookeeper);
    this.connections.topicConsumer = {};
  }

  Kafkazoo.prototype.connect = function() {
    var _this = this;
    return this._zookeeper.connect(function(err) {
      if (err) {
        _this.fatal('Error connecting zookeeper', err);
      }
      return _this.emit('connected');
    });
  };

  Kafkazoo.prototype.fatal = function(message, detail) {
    return this.emit('error', message, detail);
  };

  Kafkazoo.prototype.createConsumer = function(topic, consumerGroup, options) {
    return this.connections.topicConsumer["" + consumerGroup + "-" + topic] = new TopicConsumer(this.connections, consumerGroup, topic, options);
  };

  Kafkazoo.prototype.getAllRegisteredBrokers = function(onData) {
    return this.connections.zooKafka.getAllRegisteredBrokers(onData);
  };

  return Kafkazoo;

})(EventEmitter);

/*
//@ sourceMappingURL=Kafkazoo.js.map
*/