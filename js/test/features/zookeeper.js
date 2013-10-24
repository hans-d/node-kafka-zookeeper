// This file has been generated from coffee source files

var FakeZookeeper, PlusClient, Yadda, afterEach, beforeEach, context, executeFeature, featureName, mockery, path;

mockery = require('mockery');

FakeZookeeper = require('zookeeper-hd').FakeZookeeper;

PlusClient = Yadda = null;

context = null;

FakeZookeeper.knownInstance = function() {
  return FakeZookeeper._instance;
};

beforeEach = function() {
  var zookeeperClient;
  FakeZookeeper._instance = new FakeZookeeper();
  mockery.enable({
    useCleanCache: true,
    warnOnUnregistered: false
  });
  mockery.registerMock('zookeeper', FakeZookeeper.knownInstance);
  Yadda = require('yadda');
  Yadda.plugins.mocha();
  PlusClient = require('zookeeper-hd').PlusClient;
  zookeeperClient = new PlusClient();
  zookeeperClient.mkdir('/brokers/ids', function() {
    zookeeperClient.create(['/brokers/ids', 1], '127.0.0.1-1373962374351:127.0.0.1:9092', function() {});
    zookeeperClient.create(['/brokers/ids', 5], '127.0.0.1-1373962374351:127.0.0.2:9093', function() {});
    zookeeperClient.mkdir('/brokers/topics', function() {});
    zookeeperClient.mkdir(['/brokers/topics', 'topic1'], function() {});
    zookeeperClient.create(['/brokers/topics', 'topic1', '1'], '1', function() {});
    zookeeperClient.create(['/brokers/topics', 'topic1', '2'], '1', function() {});
    zookeeperClient.create(['/brokers/topics', 'topic1', '5'], '2', function() {});
    return zookeeperClient.mkdir(['/brokers/topics', 'topic2'], function() {});
  });
  return context = {
    zookeeperClient: zookeeperClient,
    fakeZookeeper: FakeZookeeper
  };
};

afterEach = function() {
  mockery.deregisterAll();
  return mockery.disable();
};

executeFeature = function(featureFile, featureLibrary) {
  return feature(featureFile, function(feature) {
    var library, yadda;
    library = require(featureLibrary);
    yadda = new Yadda.Yadda(library, context);
    return scenarios(feature.scenarios, function(scenario, done) {
      return yadda.yadda(scenario.steps, done);
    });
  });
};

path = 'features';

featureName = 'zookeeper';

beforeEach();

executeFeature("features/" + featureName + ".feature", "../../../features/libraries/" + featureName);

afterEach();

/*
//@ sourceMappingURL=zookeeper.js.map
*/