/**
 * Created by teklof on 9.2.15.
 */

var _ = require('lodash');
var Promise = require('bluebird');
var dbg = require('debug');
var _enabledOrig = dbg.enabled; // NOTE: temporarily force debug logging on
dbg.enabled = function(ns) {
  if(/s4qs/.test(ns)) return true; else return _enabledOrig(ns);
};
var error = dbg('s4qs-rs:utils:error');
var debug = dbg('s4qs-rs:utils');
var querystring = require('querystring');
var util = require('util');
var inspect = _.partialRight(util.inspect, {depth: 2});

/**
 * Return a bluebird Disposer that contains a Postgres client. Use with Promise.using
 * @param connString Postgres connection string
 * @param pg Postgres module
 */
exports.getPgClient = function getPgClient(pg, connString) {
  var _done;
  return pg.connectAsync(connString)
    .spread((client, done) => {
      _done = done;
      return client;
    })
    .disposer((client, promise) => {
      if(_done) {
        if(promise.isFulfilled()) {
          _done();
        } else {
          _done(client); // if something fails, remove the client from the pool
        }
      }
    });
};

function inTransaction(client, ctx, fn) {
  if(!ctx) {
    ctx = "no context";
  }

  var txId = ((Date.now() / 1000).toFixed(0)) + exports.randomString(3);
  debug(`Beginning transaction ${txId} (${ctx}) ${client.processID}`);

  return client.queryAsync("begin").return(client)
    .then(fn)
    .then((res) => {
        debug(`Committing transaction ${txId} (${ctx}) ${client.processID}`);
        return client.queryAsync("commit")
          .tap(() => debug(`Committed transaction ${txId} (${ctx}) ${client.processID}`))
          .return(res);
      }
      , (err) => {
        if (err instanceof Promise.TimeoutError) {
          error(`Timeout inside transaction ${txId} (${ctx}) ${client.processID}`);
          throw err;
        }

        if (/connection terminated/i.test(err.message)) {
          error("Connection terminated, not doing ROLLBACK");
          throw err;
        }


      error(`ROLLBACK for transaction ${txId} (${ctx}) ${client.processID} due to ${err.stack || err}`);
      return client.queryAsync("rollback")
        .tap(() => error(`Rolled back transaction ${txId} (${ctx}) ${client.processID}`))
        .throw(err);
    })
}

exports.inTransaction = inTransaction;

/**
 * Sends a cancel message to a postgres server. Same as in pg's client.js, but without the active query check.
 * @param client
 * @return {bluebird|exports|module.exports} Promise that will be fulfilled if the cancel message is sent successfully,
 * rejected otherwise
 */
function cancelClient(client) {
  debug("Cancel requested for client " + client.processID);
  return new Promise((resolve, reject) => {
    var con = client.connection;

    if(client.host && client.host.indexOf('/') === 0) {
      con.connect(client.host + '/.s.PGSQL.' + client.port);
    } else {
      con.connect(client.port, client.host);
    }

    con.on('connect', () => {
      debug("Sending cancel for client + " + client.processID);
      con.cancel(client.processID, client.secretKey);
      resolve(true);
    });

    con.on('error', reject);
  });
}

exports.cancelClient = cancelClient;

/**
 * takes a function fn, and returns a function that takes an array and applies map fn to it. Basically partial right
 * application of _.map
 * @example
 * splat(function(n){return n+1})([1,2,3,4]) // returns [2,3,4,5]
 * @param fn
 * @returns {Function}
 */
function splat(fn) {
  return function (array) {
    return _.map(array, fn);
  };
}

exports.splat = splat;

function toEach(fn) {
  return function (array) {
    return _.each(array, fn);
  };
}

exports.toEach = toEach;

function toAll(fn) {
  return function(array) {
    return _.all(array, fn);
  };
}

exports.toAll = toAll;

/**
 * Takes a method name, and returns a function that takes an instance and binds the instance's method with the given name
 * @example
 * bound('pop')(someArray) // returns a properly bound .pop function for the array, i.e. someArray.pop.bind(someArray)
 * @returns {Function}
 */
function bound() {
  var messageName = arguments[0],
    args = Array.prototype.slice.call(arguments, 1);

  if (arguments.length === 1) {
    return function (instance) {
      return instance[messageName].bind(instance);
    };
  }
  else {
    return function (instance) {
      return Function.prototype.bind.apply(
        instance[messageName],
        [instance].concat(args)
      );
    };
  }
}

exports.bound = bound;

/**
 * Takes a method name, and returns a function that takes an instance calls the given method on it
 * @example
 * splat(send('toString')) // returns a function that takes an array and calls the method 'toString()' on all its
 * elements, i.e. _.map(array, function(it){return it.toString()});
 * @returns {Function}
 */
function send () {
  var fn = bound.apply(this, arguments);

  return function (instance) {
    return fn(instance)();
  };
}

exports.send = send;

function get (attr) {
  return function (object) { return object[attr]; };
}

exports.get = get;

var randomString = exports.randomString = function randomString(len) {
  var charCodes = _.times(len,  () => _.random(97, 122));
  return String.fromCharCode.apply(null, charCodes);
};

/**
 * Takes an S3 event and returns an array of S3 URIs contained in the Records field.
 * @param {Object} evt S3 event
 * @return {Array} S3 URI strings
 */
var eventToS3URIs = exports.eventToS3URIs = function eventToS3URIs(evt) {
  if (!evt) {
    return [];
  }

  if (evt.Type === "Notification") {
    error("Suspicious SQS message. Are you sure the SNS message delivery is set to raw?");
    return [];
  }

  if (_.isArray(evt.Records) && evt.Records.length > 0) {
    return _.reduce(evt.Records, (acc, record) => {
      var s3 = record.s3;
      if (record.eventVersion == "2.0" && s3 && s3.s3SchemaVersion == "1.0") {
        acc.push(querystring.unescape("s3://" + s3.bucket.name + "/" + s3.object.key))
      } else {
        error("Unknown event (" + record.eventVersion + ") or S3 event version (" +
              ((s3 && s3.s3SchemaVersion) || "N/A") + ")");
      }
      return acc;
    }, []);
  }
  return [];
};


var tryParse = exports.tryParse = function tryParse(msg) {
  try {
    return JSON.parse(msg.Body);
  } catch (e) {
    error('Error parsing message "' + msg.Body + '": ' + e);
    return null;
  }
};

/**
 * A function that takes an SQS message and turns it into an array of S3 URIs.
 * @type {Function}
 * @private
 */
var messageToURIs = exports.messageToURIs = function(sqsMsg) {
  return sqsMsg.toURIs();
};

/**
 * A function that takes an array of SQS messages, compacts it (removes falsey values), tries to parse the message
 * body of each message, extracts the S3 URI from each message and then finally flattens the resulting array
 * @type {Function}
 * @private
 */
var messagesToURIs = exports.messagesToURIs = _.flow(_.compact, splat(messageToURIs), _.flatten);

/**
 * Takes a string and turns it into a function that returns a table name for an S3 URI. If the string starts with
 * a '/' it's assumed to be a regex, and the returned function will use that regex's first capture group to build table
 * names. If the string doesn't start with a '/', the function will always return whatever 'table' contains.
 * @param {String} table string given in config
 * @returns {Function} Function that takes an S3 URI and returns a table name
 */
function tableStrToNamer(table) {

  if(_.first(table) === '/') { // happily assume it's a regex
    var lastSlash = _.lastIndexOf(table, '/')
      , pattern = _.initial(_.rest(table)).join('').substring(0,lastSlash - 1)
      , flags = table.substring(lastSlash + 1)
      , re = new RegExp(pattern, flags);

    return _.partial(_URIToTbl, re);
  } else {
    return () => table;
  }
}

exports.tableStrToNamer = tableStrToNamer;

exports._URIToTbl = _URIToTbl;


function _URIToTbl(regex, uri) {
  var match = uri.match(regex);
  if(match && match[1]) {
    return match[1].split('.').join('_');
  } else {
    throw new Error("Can't turn '" + uri + "' into table name with regex " + regex);
  }
}

/**
 * Takes an array of base table names (like the keys in tableConfig) and a function that takes an S3 URI
 * and turns it into a base table name, and returns a function which takes an S3 URI and returns true if the message
 * matches any known base table name, false otherwise.
 * @param {Array} names Array of base table names
 * @param {Function} namerFn A function that takes an S3 URI and turns it into a base table name
 * @returns {Function} a function that takes an SQS message and returns true if the message
 * matches any known base table name, false otherwise.
 */
function nameFilterFnFor(names, namerFn) {

  if(!_.isFunction(namerFn)) {
    throw new Error("The table parameter wasn't something I recognize as a regex or function?");
  }

  var knownNamesMap = _.reduce(names, (acc,val) => {
    acc[val] = true;
    return acc;
  }, {});

// a function that returns true if the parameter is found in knownNamesMap, undefined otherwise
  var nameKnown = _.propertyOf(knownNamesMap);

// returns true if the S3 URI can be turned to a known base table name, false otherwise
  return function nameFilter(uri) {
    var name;

    try {
      name = namerFn(uri)
    } catch (e) {
    }

    return !!nameKnown(name);
  }
}

exports.nameFilterFnFor = nameFilterFnFor;

function defer() {
  var resolver, rejecter;
  var p = new Promise((resolve, reject) => {resolver = resolve; rejecter = reject;});
  return {reject: _.once(rejecter), resolve: _.once(resolver), promise: p};
}

exports.defer = defer;

/**
 * redshiftDateFix is a kluge to get correct times out of the database; pg converts the UTC time returned by Redshift
 * to a local time
 * @param {Date} dateFromDb Date you got from Redshift
 * @return {Date}
 */
exports.redshiftDateFix = function redshiftDateFix(dateFromDb) {
  if(!dateFromDb) {
    return new Date(0);
  }

  return new Date(Date.UTC(dateFromDb.getFullYear(), dateFromDb.getMonth(), dateFromDb.getDate(), dateFromDb.getHours(), dateFromDb.getMinutes(),
    dateFromDb.getSeconds()));
};