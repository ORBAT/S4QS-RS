var _ = require('lodash');
var $ = require('highland');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var ut = require('./utils');
var Promise = require('bluebird');
var dbg = require('debug');
var _enabledOrig = dbg.enabled; // NOTE: temporarily force debug logging on
dbg.enabled = function(ns) {
  if(/s4qs/.test(ns)) return true; else return _enabledOrig(ns);
};
var debug = dbg('s4qs-rs:sqs-poller');
var error = dbg('s4qs-rs:sqs-poller:error');
error.log = console.error;
var inspect = _.partialRight(util.inspect, {depth: 10});

/**
 * Creates new SQS poller. The messageStream property is a stream of SQS messages.
 *
 * @param {object} sqs Initialized SQS object with params like QueueUrl etc specified properly
 *
 * @param {Number} options.repeatPoll repeat poll this many times.
 */
var Poller = module.exports.Poller = function Poller(sqs, options) {
  EventEmitter.call(this);

  options = options || {};

  if (!sqs) {
    throw new Error("sqs must be non-null");
  }

  if(!_.isFunction(options.filter)) {
    throw new Error("filter must be a function");
  }

  this._filter = options.filter;
  this._pollIntervalS = options.pollIntervalSeconds || 60;
  this.repeatPoll = options.repeatPoll || 1;
  this.sqs = sqs;
  this.messageStream = $(function (push, next) {
    debug("messageStream generator call");

    var hadZero = false;
    var messagesP = Promise.reduce(_.times(this.repeatPoll, function () {
      return this._rcv.bind(this);
    }.bind(this)), function (msgs, fn) {
      return Promise.method(function () {
        var numZeros = (_.groupBy(msgs, "length")[0] || []).length;
        if (hadZero || numZeros >= this.repeatPoll / 2) { // if over half of repeatPoll polls returned zero, just skip this poll.
          if(!hadZero) debug("messageStream generator has " + numZeros + "/" + this.repeatPoll +
                             " zero len replies, skipping");
          hadZero = true;
          return msgs;
        }

        return fn().then(function (rcvd) {
          msgs.push(rcvd);
          return msgs;
        })
      }.bind(this))();
    }.bind(this), []);

    messagesP
      .then(_.flow(_.flatten, _.compact))
      .then(function (msgs) {
        debug("Poller got " + msgs.length + " messages");
        try {
          push(null, msgs);
          next();
        } catch (e) {
          error("messageStream push/next failed: stream was probably ended, so don't worry about it");
        }
      });
  }.bind(this)).ratelimit(1, this._pollIntervalS * 1000).flatten();
  // the ratelimit above means that we'll try to get 1 chunk of events every _pollIntervalS seconds. The
  // arrays of events are then flattened before sent downstream so consumers always see single messages
};

util.inherits(Poller, EventEmitter);

Poller.prototype._rcv = function () {
  var self = this;
  return new Promise(function (resolve, reject) {
    var req = self.sqs.receiveMessage();
    req.on('success', function (resp) {
      if (resp && resp.data) {
        var msgs = resp.data.Messages;
        // groups messages by first turning each message to S3 URIs and then checking if any of them passes the filter
        var grpByKnown = _.groupBy(msgs, _.flow(ut.messageToURIs, _.partial(_.any, _, self._filter)));

        // messages that don't pass self._filter end up in grpByKnown.false
        if(grpByKnown.false) {
          debug("Got " + grpByKnown.false.length + " messages that I don't know how to handle. Deleting them");
          debug("Filtered URIs were " + ut.messagesToURIs(grpByKnown.false));
          self.deleteMsgs(grpByKnown.false).tap(function() {
            resolve(grpByKnown.true || []);
          });
        } else {
          resolve(grpByKnown.true || []);
        }
      } else {
        error("[_rcv] received no data?!");
        resolve([]);
      }
    });

    req.on('error', function (err) {
      error("[_rcv] request error: " + err);
      reject(err);
    });

    req.send();
  });

};

/**
 * Deletes messages in a batch. The 'messages' parameter should be an array of received message objects, *not* receipt
 * handles or DeleteMessageBatch entries.
 * @param {Array} messages Array of messages
 * @return {Promise} Promise of deletion result.
 */
Poller.prototype.deleteMsgs = function(messages) {
  if(messages.length == 0) {
    return Promise.resolve();
  }

  var self = this;

  var entries = _.map(messages, function (msg, idx) {
    return {Id: idx.toString(), ReceiptHandle:  msg.ReceiptHandle}
  });

  var chunked = _.chunk(entries, 10); // 10 is the maximum allowed by SQS

  return Promise.map(chunked, function (entryChunk) {
    return new Promise(function (ok, err) {
      self.sqs.deleteMessageBatch({Entries: entryChunk}).on('success', ok).on('error', err).send();
    });
  });
};