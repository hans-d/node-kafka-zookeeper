_ = require 'underscore'

BaseMessage = require('prozess').Message


module.exports = class Message extends BaseMessage

  @asListOfMessages: (messages, options) ->
    options = _.defaults options || {}

    messages = [messages] unless _.isArray messages

    return _.map messages, (message) ->
      return message if message instanceof Message
      return new Message message, options


  constructor: (payload, options) ->
    options = _.defaults options || {},
      checkusm: undefined
      magic: undefined
      compression: undefined
      brokerId: 'default'
      partitionId: 'default'

    payload = payload.payload if payload instanceof BaseMessage
    super payload, options.checksum, options.magic, options.compression

    @brokerId = options.brokerId
    @partitionId = options.partitionId


