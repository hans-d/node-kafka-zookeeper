should = require 'should'
sinon = require 'sinon'
mockery = require 'mockery'

{FakeZookeeper} = require('zookeeper-hd')
ZooKafka = require '../../src/lib/ZooKafka'
_ = require 'underscore'


describe 'ZooKafka class', ->
    client = zooKafka = PlusClient = null

    before ->
      mockery.enable useCleanCache: true
      mockery.registerMock 'zookeeper', FakeZookeeper

      mockery.registerAllowables [
        'events', 'path',
        'async', 'underscore',
        'zookeeper-hd',
        './lib/SimpleClient', './lib/PlusClient', './lib/FakeZookeeper', './SimpleClient', '..'
      ]


      {PlusClient} = require 'zookeeper-hd'

    after ->
      mockery.deregisterAll()
      mockery.disable()


    beforeEach (done) ->
      client = new PlusClient()
      count = 0

      zooKafka = new ZooKafka client

      ready = ->
        count++
        done() if count == 9

      client.mkdir '/brokers/ids', ->
        client.create ['/brokers/ids', 1], '127.0.0.1-1373962374351:127.0.0.1:9092', ready
        client.create ['/brokers/ids', 5], '127.0.0.1-1373962374351:127.0.0.2:9093', ready

      client.mkdir '/brokers/topics', ->
        client.mkdir ['/brokers/topics', 'topic1'], ->
          client.create ['/brokers/topics', 'topic1', '1'], '1', ready
          client.create ['/brokers/topics', 'topic1', '2'], '1', ready
          client.create ['/brokers/topics', 'topic1', '5'], '2', ready
        client.mkdir ['/brokers/topics', 'topic2'], ready

      client.mkdir ['/consumers', 'groupA', 'offsets', 'topic1'], ->
        client.create ['/consumers', 'groupA', 'offsets', 'topic1', '1-0'], '100', ready
        client.create ['/consumers', 'groupA', 'offsets', 'topic1', '2-0'], '200', ready
        client.create ['/consumers', 'groupA', 'offsets', 'topic1', '5-1'], '510', ready



    describe 'construct', ->

      it 'can be constructed', ->
        zooKafka = new ZooKafka client

    describe '#getAllRegisteredBrokers', ->

      it 'should return all registered brokers in a hash', (done) ->
        zooKafka.getAllRegisteredBrokers (err, res) ->
          should.not.exist err
          res.should.have.keys '1', '5'
          res['1'].should.include host: '127.0.0.1', port:'9092', id: '1'
          res['5'].should.include host: '127.0.0.2', port:'9093', id: '5'
          done()


    describe '#getRegisteredBroker', ->

      it 'should return registered broker details directly', (done) ->
        zooKafka.getRegisteredBroker '1', (err, res) ->
          should.not.exist err
          res.should.include host: '127.0.0.1', port:'9092', id: '1'
          done()

      it 'should return error for non-existing broker', (done) ->
        zooKafka.getRegisteredBroker '2', (err, res) ->
          err.should.have.keys 'msg', 'error'
          err.msg.should.equal 'Error retrieving broker'
          err.error.should.include
            rc: FakeZookeeper.ZKCONST.ZNONODE
            msg: 'no node'
          done()


    describe '#getAllRegisteredTopics', ->

      it 'should return all registered topics in a collection', (done) ->
        zooKafka.getAllRegisteredTopics (err, res) ->
          should.not.exist err
          res.should.eql ['topic1', 'topic2']
          done()


    describe '#getRegisteredTopicPartitions', ->

        it 'should return all topic partitions where broker is still registered', (done) ->
            zooKafka.getRegisteredTopicPartitions 'topic1', (err, res) ->
                should.not.exist(err);
                res.should.have.keys('1-0', '5-0', '5-1');
                res['1-0'].should.eql(
                    {topic: 'topic1', brokerPartitionId: '1-0', brokerId: '1', partitionId: 0}
                );
                res['5-0'].should.eql(
                    {topic: 'topic1', brokerPartitionId: '5-0', brokerId: '5', partitionId: 0}
                );
                res['5-1'].should.eql(
                    {topic: 'topic1', brokerPartitionId: '5-1', brokerId: '5', partitionId: 1}
                );
                done();

        it 'should return all topic partitions', (done) ->
            zooKafka.getRegisteredTopicPartitions 'topic1', {onlyRegisteredBrokers: false}, (err, res) ->
                should.not.exist(err);
                res.should.have.keys('1-0', '2-0', '5-0', '5-1');
                res['1-0'].should.eql(
                    {topic: 'topic1', brokerPartitionId: '1-0', brokerId: '1', partitionId: 0}
                );
                res['2-0'].should.eql(
                    {topic: 'topic1', brokerPartitionId: '2-0', brokerId: '2', partitionId: 0}
                );
                res['5-0'].should.eql(
                    {topic: 'topic1', brokerPartitionId: '5-0', brokerId: '5', partitionId: 0}
                );
                res['5-1'].should.eql(
                    {topic: 'topic1', brokerPartitionId: '5-1', brokerId: '5', partitionId: 1}
                );
                done();

    describe '#getRegisteredConsumerOffset', ->

        it 'should return consumer offset', (done) ->
            topicPartition = { brokerPartitionId: '1-0', topic: 'topic1'};
            zooKafka.getRegisteredConsumerOffset 'groupA', topicPartition, (err, res) ->
                should.not.exist(err);
                res.should.include({
                    consumerGroup: 'groupA',
                    offset: '100',
                    brokerPartitionId: '1-0',
                    topic: 'topic1'
                });
                done();

        it 'should work with getRegisteredTopicPartitions output', (done) ->
            zooKafka.getRegisteredTopicPartitions 'topic1', (err, res) ->

                zooKafka.getRegisteredConsumerOffset 'groupA', res['1-0'], (err, res) ->
                    should.not.exist(err);
                    res.should.include
                        consumerGroup: 'groupA',
                        offset: '100',
                        brokerPartitionId: '1-0',
                        topic: 'topic1'
                    done();

        it 'should return consumer offset 0 when no offset is registered', (done) ->
            topicPartition = { brokerPartitionId: '5-0', topic: 'topic1'};
            zooKafka.getRegisteredConsumerOffset 'groupA', topicPartition, (err, res) ->
                should.not.exist(err);
                res.should.include
                    consumerGroup: 'groupA',
                    offset: '0',
                    brokerPartitionId: '5-0',
                    topic: 'topic1'
                done();


    describe '#_getPartitionConnectionAndOffsetDetails', ->

        it 'should return consumer offset and broker details', (done) ->
            topicPartition = { brokerPartitionId: '1-0', topic: 'topic1', brokerId: '1'};
            zooKafka.getPartitionConnectionAndOffsetDetails 'groupA', topicPartition, (err, res) ->
                should.not.exist(err);
                res.should.include({
                    consumerGroup: 'groupA',
                    offset: '100',
                    brokerPartitionId: '1-0',
                    brokerId: '1',
                    topic: 'topic1'
                });
                res.should.have.property('broker');
                res['broker'].should.include({host: '127.0.0.1', port:'9092', id: '1'})
                done();


        it 'should work with getRegisteredTopicPartitions output', (done) ->
            zooKafka.getRegisteredTopicPartitions 'topic1', (err, res) ->

                zooKafka.getPartitionConnectionAndOffsetDetails 'groupA', res['1-0'], (err, res) ->
                    should.not.exist(err);
                    res.should.include({
                        consumerGroup: 'groupA',
                        offset: '100',
                        brokerPartitionId: '1-0',
                        brokerId: '1',
                        topic: 'topic1'
                    });
                    res.should.have.property('broker');
                    res['broker'].should.include({host: '127.0.0.1', port:'9092', id: '1'});
                    done();

        it 'should return consumer offset 0 when no offset is registered', (done) ->
            topicPartition = { brokerPartitionId: '5-0', topic: 'topic1', brokerId: '5'};
            zooKafka.getPartitionConnectionAndOffsetDetails 'groupA', topicPartition, (err, res) ->
                should.not.exist(err);
                res.should.include({
                    consumerGroup: 'groupA',
                    offset: '0',
                    brokerPartitionId: '5-0',
                    brokerId: '5',
                    topic: 'topic1'
                });
                res.should.have.property('broker');
                res['broker'].should.include({host: '127.0.0.2', port:'9093', id: '5'});
                done();

        it 'should return error when broker is not registered', (done) ->
            topicPartition = { brokerPartitionId: '2-0', topic: 'topic1', brokerId: '2'};

            zooKafka.getPartitionConnectionAndOffsetDetails 'groupA', topicPartition, (err, res) ->
                err.should.have.keys 'msg', 'error'
                err.msg.should.equal 'Error retrieving broker'
                err.error.should.include
                    rc: FakeZookeeper.ZKCONST.ZNONODE
                    msg: 'no node'
                done()

    describe '#_registerConsumerOffset', ->

        it 'should create or update an offset', (done) ->
            topicPartition = brokerPartitionId: '1-0', topic: 'topic1', brokerId: '1'

            zooKafka.registerConsumerOffset 'groupA', topicPartition, 1000, (error) ->
                should.not.exist(error);
                done();
