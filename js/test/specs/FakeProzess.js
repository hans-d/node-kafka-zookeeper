// This file has been generated from coffee source files

var FakeProzess, should;

should = require('should');

FakeProzess = require('../../src/lib/FakeProzess');

describe('FakeProzess', function() {
  beforeEach(function() {
    return FakeProzess.Brokers.reset();
  });
  return describe('Producer', function() {
    describe('construction', function() {
      it('can be constructed', function() {
        var producer;
        return producer = new FakeProzess.Producer('topic');
      });
      it('has localhost as default host', function() {
        var producer;
        producer = new FakeProzess.Producer('topic');
        return producer.host.should.equal('localhost');
      });
      return it('can be given a specific host', function() {
        var producer;
        producer = new FakeProzess.Producer('topic', {
          host: 'otherhost'
        });
        return producer.host.should.equal('otherhost');
      });
    });
    return describe('#connect', function() {
      it('can connect', function() {
        var producer;
        producer = new FakeProzess.Producer();
        producer.connect();
        return FakeProzess.Brokers.getBrokers().should.have.keys('localhost:9092');
      });
      return it('can connect', function() {
        var producer;
        producer = new FakeProzess.Producer('topic', {
          port: 9093
        });
        producer.connect();
        return FakeProzess.Brokers.getBrokers().should.have.keys('localhost:9093');
      });
    });
  });
});

/*
//@ sourceMappingURL=FakeProzess.js.map
*/