// This file has been generated from coffee source files

var Decompressor, Transform, async, util, zlib, _,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

Transform = require('stream').Transform;

zlib = require('zlib');

util = require('util');

async = require('async');

_ = require('underscore');

/*
  Decompressing messages received from Kafka

  Transformation stream

  TODO: implement snappy
*/


exports.Decompressor = Decompressor = (function(_super) {
  __extends(Decompressor, _super);

  /*
    Constructs the decompressor.
  
        onErrorDecompressing: function(message, error, detail, next) {
          next({msg: error, detail: detail});
        };
  
    @param {Object} options (optional)
    @param {Function} options.onErrorDecompressing (optional)
  */


  function Decompressor(options) {
    options = options || {};
    options.objectMode = true;
    Decompressor.__super__.constructor.call(this, options);
    this.onErrorDecompressing = options.onErrorDecompressing || function(message, error, detail, next) {
      return next({
        msg: error,
        detail: detail
      });
    };
  }

  /*
    Transformation
  
    If an error is encountered during decompression, calls #onErrorDecompressing
  
    @see stream.Transform#_transform
  */


  Decompressor.prototype._transform = function(data, encoding, done) {
    var _this = this;
    if (!data || !data.messages) {
      return this.fatal('transforming', 'unexpected data');
    }
    return async.map(data.messages, function(message, asyncReady) {
      switch (message.compression) {
        case 0:
          return asyncReady(null, message.payload);
        case 1:
          return zlib.gunzip(message.payload, function(error, buffer) {
            var batched, endpos, msg, size;
            if (error) {
              return _this.onErrorDecompressing.apply(_this, [message, 'Error unzipping', error, asyncReady]);
            }
            batched = (function() {
              var _results;
              _results = [];
              while (buffer.length > 0) {
                size = buffer.readUInt32BE(0);
                endpos = size + 4;
                msg = buffer.toString('utf8', 4, endpos);
                buffer = buffer.slice(endpos);
                _results.push(msg);
              }
              return _results;
            })();
            return asyncReady(null, batched);
          });
        case 2:
          return _this.onErrorDecompressing.apply(_this, [message, 'Snappy not implemented', null, asyncReady]);
        default:
          return _this.onErrorDecompressing.apply(_this, [message, 'Unknown compression: ' + message.compression, null, asyncReady]);
      }
    }, function(error, results) {
      if (error) {
        return _this.fatal('decompressing', error);
      }
      data.messages = results;
      _this.push(data);
      return done();
    });
  };

  Decompressor.prototype.fatal = function(msg, detail) {
    return this.emit('error', msg, detail);
  };

  return Decompressor;

})(Transform);

/*
//@ sourceMappingURL=Compression.js.map
*/