'use strict';
var hypertable = require('hypertable');
var util = require('util');
var async = require('async');

/**
 * This callback type is called `requestCallback` and is displayed as a global
 * symbol.
 * @callback requestCallback
 * @param {Object} exception Exception object
 * @param {string} responseMessage Response message
 */

/**
 * Hypertable client
 * @type {Object}
 */
var client = hypertable.thriftClient.create("localhost", 15867);

/**
 * Holds 'test' namespace ID
 * @type {Int64}
 */
var testNamespace;


/**
 * Opens the 'test' namespace.  Assigns the returned namespace ID to the
 * variable {@link testNamespace}.  
 * @param {requestCallback} callback - Callback object used for chaining
 */
var openNamespaceTest = function (callback) {
  console.log('[openNamespaceTest]');
  client.namespace_open("test", function (error, result) {
    if (error)
      console.log(util.format('%s: %s', error.name, error.message));
    else
      testNamespace = result;
    callback(null, 'openNamespaceTest');
  });
}

/**
 * Opens the 'bad' namespace.  Verifies that the namespace_open call gets called
 * back with an Exception and displays the exception class name and exception
 * message to the console.
 * @param {requestCallback} callback - Callback object used for chaining
 */
var badNamespaceTest = function (callback) {
  console.log('[badNamespaceTest]');
  client.namespace_open("bad", function (error, result) {
      if (error)
        console.log(util.format('%s: %s', error.name, error.message));
      else
        assert(false, "Namespace 'bad' should not exist!");
      callback(null, 'badNamespaceTest');
    });
}

/**
 * Issues the HQL command <code>SHOW TABLES</code> and displays results to the
 * console.
 * @param {requestCallback} callback - Callback object used for chaining
 */
var showTablesTest = function(callback) {
  console.log('[showTablesTest]');
  var namespace;
  async.waterfall([
    function openNamespace (callback) {
      client.namespace_open("test", callback);
    },
    function getListing (ns, callback) {
      namespace = ns;
      client.hql_query(ns, "SHOW TABLES", callback);
    },
    function processGetListingResponse (response, callback) {
      for (var i = 0; i < response.results.length; i++) {
        console.log(util.format('%s', response.results[i]));
      }
      client.namespace_close(namespace, callback);
    },
    function closeNamespace (responsee, callback) {
      callback();
    }
    ],
    function(error) { callback(error, 'showTablesTest'); });
}

/**
 * Issues the HQL command <code>SELECT * FROM thrift_test LIMIT 10</code> and
 * displays the result to the console.
 * @param {requestCallback} callback - Callback object used for chaining
 */
var selectTestTable = function(callback) {
  console.log('[selectTestTable]');
  async.waterfall([
    function getListing (callback) {
      client.hql_query(testNamespace, "SELECT * FROM thrift_test LIMIT 10",
                       callback);
    },
    function processGetListingResponse (response, callback) {
      for (var i = 0; i < response.cells.length; i++) {
        hypertable.printCell(response.cells[i]);
      }
      callback(null);
    }
  ],
  function(error) { callback(error, 'selectTestTable'); });
}

/**
 * Inserts a test Cell into the table <code>thrift_test</code> using a mutator.
 * @param {requestCallback} callback - Callback object used for chaining
 */
var mutatorTest = function (callback) {
  console.log('[mutatorTest]');
  var theMutator;
  async.waterfall([
    function mutatorOpen(callback) {
      client.mutator_open(testNamespace, "thrift_test", 0, 0, callback);
    },
    function setCell(mutator, callback) {
      theMutator = mutator;
      var key = new hypertable.Key({row: "js-k1", column_family: "col"});
      client.mutator_set_cell(theMutator,
                              new hypertable.Cell({ key: key, value: "js-v1"}),
                              callback);
    },
    function closeMutator(mutator, callback) {
      client.mutator_close(theMutator, callback);
    }
  ],
  function(error) { callback(error, 'mutatorTest'); });
}

/**
 * Inserts a test Cell into the table <code>thrift_test</code> using a shared
 * mutator.
 * @param {requestCallback} callback - Callback object used for chaining
 */
var sharedMutatorTest = function (callback) {
  console.log('[sharedMutatorTest]');
  var mutateSpec = new hypertable.MutateSpec({appname: 'test_js',
                                              flush_interval: 1000,
                                              flags: 0});
  async.waterfall([
    function setCell1(callback) {
      var key = new hypertable.Key({row: "js-put-k1", column_family: "col"});
      client.shared_mutator_set_cell(testNamespace, "thrift_test", mutateSpec,
                                     new hypertable.Cell({key: key, value: "js-put-v1"}),
                                     callback);
    },
    function refresh(mutator, callback) {
      client.shared_mutator_refresh(testNamespace, "thrift_test", mutateSpec,
                                    callback);
    },
    function setCell2(result, callback) {
      var key = new hypertable.Key({row: "js-put-k2", column_family: "col"});
      client.shared_mutator_set_cell(testNamespace, "thrift_test", mutateSpec,
                                     new hypertable.Cell({key: key, value: "js-put-v2"}),
                                     callback);
    },
    function processResult(result, callback) {
      setTimeout(function() { callback(null); }, 2000);
    }
  ],
  function(error) { callback(error, 'sharedMutatorTest'); });
}

/**
 * Creates a scanner and Scans over the first 10 rows of the the table
 * <code>thrift_test</code> displaying the results to the console.
 * @param {requestCallback} callback - Callback object used for chaining
 */
var scannerTest = function (callback) {
  console.log('[scannerTest]');
  var scanner;
  async.waterfall([
    function createScanner(callback) {
      var scanSpec = new hypertable.ScanSpec({row_limit: 10});
      client.scanner_open(testNamespace, "thrift_test", scanSpec, callback);
    },
    function getCells(result, callback) {
      scanner = result;
      client.scanner_get_cells(scanner, callback);
    },
    function processCells(result, callback) {
      for (var i = 0; i < result.length; i++) {
        hypertable.printCell(result[i]);
      }
      client.scanner_close(scanner, callback);
    },
    function handleCloseMutatorResult(result, callback) {
      callback(null);
    }
  ],
  function(error) { callback(error, 'scannerTest'); });

}

var asyncMutatorPipeline = function(future, keyPrefix, callback) {
  var mutator;
  async.waterfall([
    function createAsyncMutator1(callback) {
      client.async_mutator_open(testNamespace, "thrift_test", future, 0, callback);
    },
    function asyncMutate1(result, callback) {
      mutator = result;
      var key = new hypertable.Key({row: keyPrefix, column_family: "col"});
      client.async_mutator_set_cell(mutator,
                                    new hypertable.Cell({key: key, value: keyPrefix+"-async"}),
                                    callback);
    },
    function asyncMutatorFlush1(result, callback) {
      client.async_mutator_flush(mutator, callback);
    },
    function handleAsyncMutatorFlushResult1(result, callback) {
      callback();
    }
  ],
  function (error, result) {
    callback(error, mutator);
  });
}


var asyncTest = function (callback) {
  console.log('[asyncTest]');
  var future;
  var asyncMutator1;
  var asyncMutator2;
  var asyncScanner = [];
  async.waterfall([
    function createFuture(callback) {
      client.future_open(0, callback);
    },
    function runAsyncMutators(result, callback) {
      future = result;
      async.parallel([
        function mutatorPipeline1(callback) {
          async.waterfall([
            function runMutatorPipeline1(callback) {
              asyncMutatorPipeline(future, "js-k1", callback);
            },
            function fetchMutator1(result, callback) {
              asyncMutator1 = result;
              callback(null)
            }
          ],
          function(error) { callback(error); });
        },
        function mutatorPipeline2(callback) {
          async.waterfall([
            function runMutatorPipeline1(callback) {
              asyncMutatorPipeline(future, "js-k2", callback);
            },
            function fetchMutator1(result, callback) {
              asyncMutator2 = result;
              callback(null)
            }
          ],
          function(error) { callback(error); });
        }
      ],
      function(error) { callback(error); });
    },
    function fetchResults (callback) {
      var numResults=0;
      var futureResult;
      async.doWhilst(
        function (callback) {
          async.waterfall([
            function fetchFutureResult(callback) {
              client.future_get_result(future, 10000, callback);
            },
            function processFutureResult(result, callback) {
              futureResult = result;
              if (!futureResult.is_empty) {
                numResults++;
                if (futureResult.is_error || futureResult.is_scan)
                  callback(new Error('Unexpected result'));
                else
                  callback(null);
              }
              else
                callback(null);
            }
          ],
          function(error) { callback(error); });
        },
        function () { return !futureResult.is_empty; },
        function (err) {
          if (numResults != 2)
            callback(new Error('Expected 2 results, got ' + numResults));
          else
            callback(null);
        }                     
      );
    },
    function futureIsCancelled (callback) {
      client.future_is_cancelled(future, callback);
    },
    function futureIsFull (result, callback) {
      if (result)
        callback(new Error('Future was cancelled'));
      else 
        client.future_is_full(future, callback);
    },
    function futureIsEmpty (result, callback) {
      if (result)
        callback(new Error('Future is full'));
      else 
        client.future_is_empty(future, callback);
    },
    function futureHasOutstanding (result, callback) {
      if (!result)
        callback(new Error('Future is not empty'));
      else 
        client.future_has_outstanding(future, callback);
    },
    function closeMutators (result, callback) {
      if (result)
        callback(new Error('Future has outstanding'));
      else {
        async.parallel([
          function closeAsyncMutator1 (callback) {
            async.waterfall([
              function closeAsyncMutator(callback) {
                client.async_mutator_close(asyncMutator1, callback);
              },
              function processAsyncMutatorClose(result, callback) {
                callback();
              }
            ],
            function(error) { callback(error); });
          },
          function closeAsyncMutator2 (callback) {
            async.waterfall([
              function closeAsyncMutator(callback) {
                client.async_mutator_close(asyncMutator2, callback);
              },
              function processAsyncMutatorClose(result, callback) {
                callback();
              }
            ],
            function(error) { callback(error); });
          }
        ],
        function(error) { callback(error); });
      }
    },
    function createScanners (callback) {
      async.parallel([
        function (callback) {
          client.async_scanner_open(testNamespace, "FruitColor", future,
                                    new hypertable.ScanSpec({versions: 1}),
                                    callback);

        },
        function (callback) {
          client.async_scanner_open(testNamespace, "FruitLocation", future,
                                    new hypertable.ScanSpec({versions: 1}),
                                    callback);

        },
        function (callback) {
          client.async_scanner_open(testNamespace, "FruitEnergy", future,
                                    new hypertable.ScanSpec({versions: 1}),
                                    callback);

        }
      ],
      function(error, results) {
        asyncScanner = results;
        callback(error);
      });
    },
    function fetchScanResults (callback) {
      var num_cells = 0;
      async.series([
        function (callback) {
          async.doWhilst(
            function (callback) {
              async.waterfall([
                function (callback) {
                  client.future_get_result(future, 10000, callback);
                },
                function (result, callback) {
                  if (result.is_empty || result.is_error || !result.is_scan)
                    callback(new Error('Unexpected result'));
                  else {
                    for (var i = 0; i < result.cells.length; i++)
                      hypertable.printCell(result.cells[i]);
                    num_cells += result.cells.length;
                    callback();
                  }
                }
              ],
              function(error) { callback(error); });
            },
            function () { return num_cells < 6; },
            function (err) {
              callback(err);
            }
          );
        },
        function cancelFuture (callback) {
          async.waterfall([
            function (callback) {
              client.future_cancel(future, callback);
            },
            function (result, callback) {
              client.future_is_cancelled(future, callback);
            },
            function (result, callback) {
              if (!result)
                callback(new Error('Expected future to be cancelled'));
              else
                callback();
            }
          ],
          function(error) { callback(error); });
        }
      ],
      function(error) { callback(error); });
    }
  ],
  function(error) { callback(error, 'asyncTest'); });
}
    


console.log("HQL examples");

async.series([
  openNamespaceTest,
  badNamespaceTest,
  showTablesTest,
  selectTestTable,
  mutatorTest,
  sharedMutatorTest,
  scannerTest,
  asyncTest
  ],
  function(error, results) {
    if (error) {
      console.log(util.format('%s: %s', error.name, error.message));
    }
    client.closeConnection();
  }
);
