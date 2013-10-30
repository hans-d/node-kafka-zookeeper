{Transform} = require 'stream'
zlib = require 'zlib'
util = require 'util'

async = require 'async'
_ = require 'underscore'

###
  Decompressing messages received from Kafka

  Transformation stream

  TODO: implement snappy
###
exports.Decompressor = class Decompressor extends Transform

  ###
    Constructs the decompressor.

        onErrorDecompressing: function(message, error, detail, next) {
          next({msg: error, detail: detail});
        };

    @param {Object} options (optional)
    @param {Function} options.onErrorDecompressing (optional)
  ###
  constructor: (options) ->
    options = options || {}
    options.objectMode = true
    super options

    @onErrorDecompressing = options.onErrorDecompressing || (message, error, detail, next) ->
      next msg: error, detail: detail


  ###
    Transformation

    If an error is encountered during decompression, calls #onErrorDecompressing

    @see stream.Transform#_transform
  ###
  _transform: (data, encoding, done) ->
    return @fatal 'transforming', 'unexpected data' if !data || !data.messages

    async.map data.messages, (message, asyncReady) =>

      switch message.compression

        when 0 then return asyncReady null, message.payload

        when 1 then zlib.gunzip message.payload, (error, buffer) =>
          return @onErrorDecompressing.apply @, [
            message, 'Error unzipping', error, asyncReady
          ] if error
          batched = while buffer.length > 0
            size = buffer.readUInt32BE 0
            endpos = size + 4
            msg = buffer.toString 'utf8', 4, endpos
            buffer = buffer.slice endpos
            msg
          return asyncReady null, batched

        when 2 then return @onErrorDecompressing.apply @, [
          message, 'Snappy not implemented', null, asyncReady
        ]

        else return @onErrorDecompressing.apply @, [
          message, 'Unknown compression: ' + message.compression, null, asyncReady
        ]

    , (error, results) =>
      return @fatal 'decompressing', error if error
      data.messages = results
      @push data
      done()

  fatal: (msg, detail) ->
    @emit 'error', msg, detail
