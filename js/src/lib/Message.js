// This file has been generated from coffee source files

var BaseMessage, Message, _,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

_ = require('underscore');

BaseMessage = require('prozess').Message;

module.exports = Message = (function(_super) {
  __extends(Message, _super);

  Message.asListOfMessages = function(messages, options) {
    options = _.defaults(options || {});
    if (!_.isArray(messages)) {
      messages = [messages];
    }
    return _.map(messages, function(message) {
      if (message instanceof Message) {
        return message;
      }
      return new Message(message, options);
    });
  };

  function Message(payload, options) {
    options = _.defaults(options || {}, {
      checkusm: void 0,
      magic: void 0,
      compression: void 0,
      brokerId: 'default',
      partitionId: 'default'
    });
    if (payload instanceof BaseMessage) {
      payload = payload.payload;
    }
    Message.__super__.constructor.call(this, payload, options.checksum, options.magic, options.compression);
    this.brokerId = options.brokerId;
    this.partitionId = options.partitionId;
  }

  return Message;

})(BaseMessage);

/*
//@ sourceMappingURL=Message.js.map
*/