{Transform} = require 'stream'
async = require 'async'
zlib = require 'zlib'
util = require 'util'
_ = require 'underscore'

exports.Decompressor = class Decompressor extends Transform

  constructor: (options) ->
    options = options || {}
    options.objectMode = true
    super options

    @onErrorDecompressing = options.onErrorDecompressing || (this_, message, error, detail, next) ->
      next msg: error, detail: detail


  _transform: (data, encoding, done) ->
    return @fatal 'transforming', 'unexpected data' if !data || !data.messages

    async.map data.messages, (message, asyncReady) =>

      switch message.compression

        when 0 then return asyncReady null, message.payload

        when 1 then zlib.gunzip message.payload, (error, buffer) =>
          return @onErrorDecompressing @, message, 'Error unzipping', error, asyncReady if error
          batched = while buffer.length > 0
            size = buffer.readUInt32BE 0
            endpos = size + 4
            msg = buffer.toString 'utf8', 4, endpos
            buffer = buffer.slice endpos
            msg
          return asyncReady null, batched

        when 2 then return @onErrorDecompressing @, message, 'Snappy not implemented', null, asyncReady

        else return @onErrorDecompressing @, message, 'Unknown compression: ' + message.compression, null, asyncReady

    , (error, results) =>
      return @fatal 'decompressing', error if error
      data.messages = _.flatten results
      @push data
      done()

  fatal: (msg, detail) ->
    @emit 'error', msg, detail
