// This file has been generated from coffee source files

var _;

_ = require('underscore');

/*
  A broker selection strategy determines which brokers are available for producing to,
*/


/*
  Selects one registered (active) broker where the topic is already available.
  If none is present, a random broker is picked.
*/


exports.pickOne = function() {
  var broker, zooKafka,
    _this = this;
  ({
    select: function(brokers) {
      var brokerIds, selectedId;
      brokerIds = _.keys(brokers);
      selectedId = brokerIds[Math.floor(Math.random() * brokerIds.length)];
      return this.emit('brokers', _.pick(brokers, selectedId), selectedId);
    }
  });
  zooKafka = connections.zooKafka;
  broker = null;
  return zooKafka.getRegisteredTopicBrokers(this.topic, function(err, brokers) {
    if (brokers.length > 0) {
      select(brokers);
    }
    return zooKafka.getAllRegisteredBrokers(function(err, brokers) {
      if (brokers.length === 0) {
        return _this.emit('error', 'no brokers available');
      }
      return select(brokers);
    });
  });
};

/*
//@ sourceMappingURL=brokerSelectionStrategy.js.map
*/