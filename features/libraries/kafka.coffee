should = require 'should'
English = require('yadda').localisation.English

{Kafkazoo} = require '../../coffee/src'

kafka = null
results = null

module.exports = English.library()

  .given 'the background', (next) ->
    next()

  .given 'a connected kafkazoo client', (next) ->
    kafka = new Kafkazoo()
    next()

  .given 'a subscribeable topic that has unconsumed messages', (next) ->


.when 'retrieving all registered broker ids', (next) ->
    kafka.getAllRegisteredBrokers (err, data) ->
      results = data
      next()

.then 'the resulting data should be \\[(.*)\\]', (values, next) ->
    values = values.replace /\s/g,''
    results.should.have.keys values.split(',')

    next()

