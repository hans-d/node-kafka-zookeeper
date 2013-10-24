should = require 'should'
{FakeProzess} = require '../../src/index'

describe 'FakeProzess', ->

  beforeEach ->
    FakeProzess.Brokers.reset()

  describe 'Producer', ->

    describe 'construction', ->

      it 'can be constructed', ->
        producer = new FakeProzess.Producer 'topic'

      it 'has localhost as default host', ->
        producer = new FakeProzess.Producer 'topic'
        producer.host.should.equal 'localhost'

      it 'can be given a specific host', ->
        producer = new FakeProzess.Producer 'topic', host: 'otherhost'
        producer.host.should.equal 'otherhost'

    describe '#connect', ->
      it 'can connect', ->
        producer = new FakeProzess.Producer()
        producer.connect()

        FakeProzess.Brokers.getBrokers().should.have.keys 'localhost:9092'

      it 'can connect', ->
        producer = new FakeProzess.Producer 'topic', port:9093
        producer.connect()

        FakeProzess.Brokers.getBrokers().should.have.keys 'localhost:9093'
