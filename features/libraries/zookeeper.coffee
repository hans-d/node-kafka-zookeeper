should = require 'should'
English = require('yadda').localisation.English

{Kafkazoo} = require '../../coffee/src'

kafka = null
results = null

module.exports = English.library()

  .given 'the background', (next) ->
    kafka = new Kafkazoo()

    client = this.zookeeperClient

    client.mkdir ['/consumers', 'groupA', 'offsets', 'topic1'], ->
      client.create ['/consumers', 'groupA', 'offsets', 'topic1', '1-0'], '100', ->
      client.create ['/consumers', 'groupA', 'offsets', 'topic1', '2-0'], '200', ->
      client.create ['/consumers', 'groupA', 'offsets', 'topic1', '5-1'], '510', ->

    next()

  .when 'retrieving all registered broker ids', (next) ->
    kafka.getAllRegisteredBrokers (err, data) ->
      results = data
      next()

  .then 'the resulting data should be \\[(.*)\\]', (values, next) ->
    values = values.replace /\s/g,''
    results.should.have.keys values.split(',')

    next()

