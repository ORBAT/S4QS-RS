/**
 * Created by teklof on 9.2.15.
 */

var _ = require('lodash');
var Promise = require('bluebird');
var error = require('debug')('s4qs-rs:utils:error');


/**
 * Return a bluebird Disposer that contains a Postgres client. Use with Promise.using
 * @param connString Postgres connection string
 * @param pg Postgres module
 */
exports.getPgClient = function getPgClient(pg, connString) {
  var _done;
  return pg.connectAsync(connString)
    .spread(function (client, done) {
      _done = done;
      return client;
    })
    .disposer(function(client, promise) {
      if(_done) {
        if(promise.isFulfilled()) {
          _done();
        } else {
          _done(client); // if something fails, remove the client from the pool
        }
      }
    });
};

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
  var charCodes = _.times(len, function () {
    return _.random(97, 122);
  });
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
    return _.reduce(evt.Records, function (acc, record) {
      var s3 = record.s3;
      if (record.eventVersion == "2.0" && s3 && s3.s3SchemaVersion == "1.0") {
        acc.push("s3://" + s3.bucket.name + "/" + s3.object.key)
      } else {
        error("Unknown event (" + record.eventVersion + ") or S3 event version (" +
              ((s3 && s3.s3SchemaVersion) || "N/A") + ")");
      }
      return acc;
    }, []);
  }
  return [];
};


function tryParse(msg) {
  try {
    return JSON.parse(msg.Body);
  } catch (e) {
    error('Error parsing message "' + msg + '": ' + e);
    return null;
  }
}

/**
 * A function that takes an SQS message and turns it into an array of S3 URIs.
 * @type {Function}
 * @private
 */
var messageToURIs = exports.messageToURIs = _.flow(tryParse, eventToS3URIs);

/**
 * A function that takes an array of SQS messages, compacts it (removes falsey values), tries to parse the message
 * body of each message, extracts the S3 URI from each message and then finally flattens the resulting array
 * @type {Function}
 * @private
 */
var messagesToURIs = exports.messagesToURIs = _.flow(_.compact, splat(messageToURIs), _.flatten);