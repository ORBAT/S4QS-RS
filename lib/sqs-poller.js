var _ = require('lodash');
var $ = require('highland');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var ut = require('./utils');
var Promise = require('bluebird');
var debug = require('debug')('s4qs-rs:sqs-poller');
var error = require('debug')('s4qs-rs:sqs-poller:error');
error.log = console.error;
var inspect = _.partialRight(util.inspect, {depth: 10});

/**
 * Creates new SQS poller which emits 'messages' events when it receives new messages. Emits 'error' events on
 * receiveMessage errors.
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
  this.messageStream = $(_.throttle(function (push, next) {
    debug("messageStream generator call");
    Promise.all(_.times(this.repeatPoll, this._rcv.bind(this)))
      .then(_.flow(_.flatten, _.compact))
      .then(function (msgs) {

        try {
          _.each(msgs, _.partial(push, null));
          next();
        } catch (e) {
          error("messageStream push/next failed: stream was probably ended, so don't worry about it");
        }
      });
  }.bind(this), this._pollIntervalS * 1000));
};

util.inherits(Poller, EventEmitter);

Poller.prototype._rcv = function () {
  var self = this;
  return new Promise(function (resolve, reject) {
    var req = self.sqs.receiveMessage();
    req.on('success', function (resp) {
      if (resp && resp.data) {
        var msgs = resp.data.Messages;

        if (!msgs || msgs.length == 0) {
          debug('[_rcv] no messages in reply');
        }

        // groups messages by first turning each message to S3 URIs and then checking if any of them passes the filter
        var grpByKnown = _.groupBy(msgs, _.flow(ut.messageToURIs, _.partial(_.any, _, self._filter)));

        // messages that don't pass self._filter end up in grpByKnown.false
        if(grpByKnown.false) {
          error("Got " + grpByKnown.false.length + " messages that I don't know how to handle. Deleting them");
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


Poller.prototype.poll = function () {
  debug("Polling for new messages");
  var self = this;

  this.pollPending = Promise.all(_.times(self.repeatPoll, function () {
      return self._rcv();
    }))
    .then(_.flatten)
    .then(function (msgs) {
      debug("[poll] total messages received: " + msgs.length);
      self.emit('messages', msgs);
      return msgs;
    });

  return this.pollPending;
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