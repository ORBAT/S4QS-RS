var _ = require('lodash');
var $ = require('highland');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var ut = require('./utils');
var Promise = require('bluebird');
var pause = require('promise-pauser');
var instr = require('./instrumentation');
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

  this._pauser = pause.pauser();
  this._pauser.pause();

  var topStatsd = new Statsd(options.statsd);
  this._instrumenter = new instr.Instrumenter(topStatsd.getChildClient("sqs-poller"));

  this._msgSource = $(function (push, next) {
    try {
      push(null, $(this._instrumenter.instrument("rcv", this._rcv.bind(this)).delay(this._pollIntervalS * 1000)));
      next();
    } catch (e) {
      error("messageStream push/next failed: " + e);
    }
  }.bind(this))
    .flatMap($.compose($, pause.waitFor(this._pauser)))
    .parallel(this.repeatPoll)
    .flatten()
    // NOTE: this "rethrowing" map is here because Promise rejection doesn't seem to propagate when using generator functions that push streams.
    // The generator function above does push(null, $(promise)), but rejected promises don't turn into stream errors
    .map(function(val) {
      if(_.isError(val)) {
        throw val;
      }
      return val;
    });


  this._msgSource.observe().batchWithTimeOrCount(this._pollIntervalS * 1000, this.repeatPoll * 10).each(function (msgs) {
    debug("_msgSource received " + msgs.length + " messages");
  });

  this._unknownMsgs = this._msgSource.observe()
    .flatMap($.compose($, pause.waitFor(this._pauser)))
    .filter(_.negate(this.msgPassesFilter.bind(this)))
    .batchWithTimeOrCount(this._pollIntervalS * 1000, 50)
    .doto(function (msgs) {
      debug("Deleting " + msgs.length + " messages I don't know how to handle. URIs are " + ut.messagesToURIs(msgs));
    })
    .flatMap(function(msgs) {
      return $(this.deleteMsgs(msgs));
    }.bind(this)).flatten();


  this.messageStream = this._msgSource.fork()
    .flatMap($.compose($, pause.waitFor(this._pauser)))
    .filter(this.msgPassesFilter.bind(this))
  ;
};

util.inherits(Poller, EventEmitter);

Poller.prototype.start = function() {
  if(!this._started) {
    this._msgSource.resume();
    this.messageStream.resume();
    this._unknownMsgs.resume();
    this._pauser.unpause();
    this._started = true;
  }
};

Poller.prototype.stop = function() {
  if(this._started) {
    this._pauser.pause();
    this._started = false;
  }
};

Poller.prototype.msgPassesFilter = function(msg) {
  return _.any(ut.messageToURIs(msg), this._filter);
};

Poller.prototype._rcv = function () {
  return new Promise(function (resolve, reject) {
    var req = this.sqs.receiveMessage();
    req.on('success', function (resp) {
      if (resp && resp.data) {
        resolve(resp.data.Messages || []);
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
  }.bind(this));

};

/**
 * Deletes messages in a batch. The 'messages' parameter should be an array of received message objects, *not* receipt
 * handles or DeleteMessageBatch entries.
 * @param {Array} messages Array of messages
 * @return {Promise} Promise of deletion result.
 */
Poller.prototype.deleteMsgs = function(messages) {
  if(messages.length == 0) {
    return Promise.resolve([]);
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
  }).return(ut.messagesToURIs(messages));
};