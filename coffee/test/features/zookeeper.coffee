mockery = require 'mockery'
{FakeZookeeper} = require 'zookeeper-hd'

PlusClient = Yadda = null
context = null

FakeZookeeper.knownInstance = ->
  return FakeZookeeper._instance

beforeEach = () ->
  FakeZookeeper._instance = new FakeZookeeper()

  mockery.enable useCleanCache: true, warnOnUnregistered: false
  mockery.registerMock 'zookeeper', FakeZookeeper.knownInstance

  Yadda = require 'yadda'
  Yadda.plugins.mocha()

  {PlusClient} = require 'zookeeper-hd'
  zookeeperClient = new PlusClient()
  zookeeperClient.mkdir '/brokers/ids', ->
    zookeeperClient.create ['/brokers/ids', 1], '127.0.0.1-1373962374351:127.0.0.1:9092', ->
    zookeeperClient.create ['/brokers/ids', 5], '127.0.0.1-1373962374351:127.0.0.2:9093', ->

    zookeeperClient.mkdir '/brokers/topics', ->
    zookeeperClient.mkdir ['/brokers/topics', 'topic1'], ->
    zookeeperClient.create ['/brokers/topics', 'topic1', '1'], '1', ->
    zookeeperClient.create ['/brokers/topics', 'topic1', '2'], '1', ->
    zookeeperClient.create ['/brokers/topics', 'topic1', '5'], '2', ->
    zookeeperClient.mkdir ['/brokers/topics', 'topic2'], ->


  context =
    zookeeperClient: zookeeperClient
    fakeZookeeper: FakeZookeeper


afterEach = () ->
  mockery.deregisterAll()
  mockery.disable()

executeFeature = (featureFile, featureLibrary) ->
  feature featureFile, (feature) ->

    library = require featureLibrary
    yadda = new Yadda.Yadda library, context

    scenarios feature.scenarios, (scenario, done) ->
      yadda.yadda scenario.steps, done

path = 'features'
featureName = 'zookeeper'

beforeEach()
executeFeature "features/#{featureName}.feature", "../../../features/libraries/#{featureName}"
afterEach()