// This file has been generated from coffee source files

var Buffer, BufferMaker, Compression, mockery, should, sinon, stream, zlib, _;

Buffer = require('buffer').Buffer;

stream = require('stream');

zlib = require('zlib');

BufferMaker = require('buffermaker');

mockery = require('mockery');

should = require('should');

sinon = require('sinon');

_ = require('underscore');

Compression = require('../../src/lib/Compression');

describe('Decompressor', function() {
  var decompressor, input, original1, original2, payload1, payload2, uncompressedBatch;
  original1 = original2 = payload1 = payload2 = uncompressedBatch = decompressor = input = null;
  before(function() {
    original1 = 'test 1';
    original2 = 'test message 2';
    payload1 = new Buffer(original1, 'utf8');
    payload2 = new Buffer(original2, 'utf8');
    return uncompressedBatch = new BufferMaker().UInt32BE(original1.length).string(original1).UInt32BE(original2.length).string(original2).make();
  });
  beforeEach(function() {
    input = new stream.Readable({
      objectMode: true
    });
    input._read = function() {};
    decompressor = new Compression.Decompressor();
    return input.pipe(decompressor);
  });
  describe('construction', function() {
    return it('can be constructed and has expected defaults', function() {
      decompressor = new Compression.Decompressor();
      return decompressor.onErrorDecompressing.should.be["instanceof"](Function);
    });
  });
  describe('default handlers', function() {
    return it('onErrorDecompressing continues with error', function(done) {
      var providedDetails;
      providedDetails = {
        foo: 'bar'
      };
      return decompressor.onErrorDecompressing(decompressor, 'foo error', providedDetails, function(err) {
        err.msg.should.equal('foo error');
        err.detail.should.equal(providedDetails);
        return done();
      });
    });
  });
  it('forwards uncompressed messages', function(done) {
    var msg1, msg2;
    decompressor.on('readable', function() {
      var result;
      result = decompressor.read();
      result.should.eql({
        messages: [payload2, payload1, payload2],
        offset: '123'
      });
      return done();
    });
    msg1 = {
      compression: 0,
      payload: payload1
    };
    msg2 = {
      compression: 0,
      payload: payload2
    };
    input.push({
      messages: [msg2, msg1, msg2],
      offset: '123'
    });
  });
  it('decompresses zipped messages', function(done) {
    decompressor.on('readable', function() {
      var result;
      result = decompressor.read();
      result.should.eql({
        messages: [[original1, original2], [original1, original2]],
        offset: '123'
      });
      return done();
    });
    return zlib.gzip(uncompressedBatch, function(error, buffer) {
      var compressed;
      if (error) {
        throw "should not happen";
      }
      compressed = {
        compression: 1,
        payload: buffer
      };
      return input.push({
        messages: [compressed, compressed],
        offset: '123'
      });
    });
  });
  it('does not handle snappy compressed messages', function(done) {
    var msg;
    sinon.stub(decompressor, 'fatal', function(msg, detail) {
      msg.should.equal('decompressing');
      detail.should.eql({
        msg: 'Snappy not implemented',
        detail: null
      });
      return done();
    });
    msg = {
      compression: 2,
      payload: payload1
    };
    return input.push({
      messages: [msg],
      offset: '123'
    });
  });
  return it('handles multiple compression types', function(done) {
    var msg1;
    decompressor.on('readable', function() {
      var result;
      result = decompressor.read();
      result.should.eql({
        messages: [payload1, [original1, original2], payload2],
        offset: '123'
      });
      return done();
    });
    msg1 = {
      compression: 0,
      payload: payload1
    };
    return zlib.gzip(uncompressedBatch, function(error, buffer) {
      var compressedMsg, uncompressedMsg1, uncompressedMsg2;
      if (error) {
        throw "should not happen";
      }
      compressedMsg = {
        compression: 1,
        payload: buffer
      };
      uncompressedMsg1 = {
        compression: 0,
        payload: payload1
      };
      uncompressedMsg2 = {
        compression: 0,
        payload: payload2
      };
      return input.push({
        messages: [uncompressedMsg1, compressedMsg, uncompressedMsg2],
        offset: '123'
      });
    });
  });
});

/*
//@ sourceMappingURL=Compression.js.map
*/