{Buffer} = require 'buffer'
stream = require 'stream'
zlib = require 'zlib'

BufferMaker = require 'buffermaker'
mockery = require 'mockery'
should = require 'should'
sinon = require 'sinon'
_ = require 'underscore'

Compression = require('../../src/lib/Compression');

describe 'Decompressor', ->
  original1 = original2 = payload1 = payload2 = uncompressedBatch = decompressor = input = null

  before ->
    original1 = 'test 1'
    original2  = 'test message 2'

    payload1 = new Buffer original1, 'utf8'
    payload2 = new Buffer original2, 'utf8'

    uncompressedBatch = new BufferMaker()
      .UInt32BE(original1.length).string(original1)
      .UInt32BE(original2.length).string(original2)
      .make()

      
  beforeEach ->
    input = new stream.Readable objectMode: true
    input._read = ->
    decompressor = new Compression.Decompressor()
    input.pipe decompressor

        
  describe 'construction', ->

    it 'can be constructed and has expected defaults', ->
        decompressor = new Compression.Decompressor();
        decompressor.onErrorDecompressing.should.be.instanceof Function

      
  describe 'default handlers', ->

    it 'onErrorDecompressing continues with error', (done) ->
      providedDetails = foo: 'bar'
      decompressor.onErrorDecompressing decompressor, {}, 'foo error', providedDetails, (err) ->
        err.msg.should.equal 'foo error'
        err.detail.should.equal providedDetails
        done()

           
  it 'forwards uncompressed messages', (done) ->

    decompressor.on 'readable', ->
      result = decompressor.read()
      result.should.eql
        messages: [payload2, payload1, payload2]
        offset: '123'
      done()

    msg1 = compression: 0, payload: payload1
    msg2 = compression: 0, payload: payload2

    input.push
      messages: [msg2, msg1, msg2],
      offset: '123'
    return


  it 'decompresses zipped messages', (done) ->

    decompressor.on 'readable', ->
      result = decompressor.read()
      result.should.eql
        messages: [original1, original2, original1, original2]
        offset: '123'
      done()

    zlib.gzip uncompressedBatch, (error, buffer) ->
      throw "should not happen" if error

      compressed = compression: 1, payload: buffer
      input.push
        messages: [compressed, compressed]
        offset: '123'


  it 'does not handle snappy compressed messages', (done) ->

    sinon.stub decompressor, 'fatal', (msg, detail) ->
      msg.should.equal 'decompressing'
      detail.should.eql
        msg: 'Snappy not implemented'
        detail: null
      done()

    msg = compression: 2, payload: payload1
    input.push
      messages: [msg]
      offset: '123'


  it 'handles multiple compression types', (done) ->

    decompressor.on 'readable', ->
      result = decompressor.read()
      result.should.eql
        messages: [payload1, original1, original2, payload2]
        offset: '123'
      done()

    msg1 = compression: 0, payload: payload1

    zlib.gzip uncompressedBatch, (error, buffer) ->
      throw "should not happen" if error

      compressedMsg = compression: 1, payload: buffer
      uncompressedMsg1 = compression: 0, payload: payload1
      uncompressedMsg2 = compression: 0, payload: payload2
      input.push
        messages: [uncompressedMsg1, compressedMsg, uncompressedMsg2]
        offset: '123'
