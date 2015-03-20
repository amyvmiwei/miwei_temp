var hypertable = require('hypertable');
var util = require('util');
var async = require('async');

var client = hypertable.thriftClient.create("localhost", 15867);

async.waterfall([
    function openNamespace (callback) {
      client.namespace_open("/", callback);
    },
    function getListing (ns, callback) {
      client.hql_query(ns, "get listing", callback);
    },
    function processGetListingResponse (response, callback) {
      for (i = 0; i < response.results.length; i++) {
        console.log(util.format('%s', response.results[i]));
      }
      callback(null);
    }
  ], function (error) {
      if (error) {
        console.log(error);
      }
      client.closeConnection();
});
