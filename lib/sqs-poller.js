var _ = require('lodash');
var $ = require('highland');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var ut = require('./utils');
var Promise = require('bluebird');
var pause = require('promise-pauser');
var Statsd = require('statsd-client');
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
 * Creates a new SQS poller
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

  this._options = options;

  this._filter = options.filter;
  this._pollIntervalS = options.pollIntervalSeconds || 60;
  this.parallelPolls = options.parallelPolls || 1;
  this.sqs = sqs;

  this._id = _.last(this.sqs.config.params.QueueUrl.split("/"));

  this.visTimeoutUpdater = new VisTimeoutUpdater(this.sqs, options.visibilityTimeoutUpdater || {});
  this.visTimeoutUpdater.start();

  this._pauser = pause.pauser();
  this._pauser.pause();

  var topStatsd = new Statsd(options.statsd);

  this._instrumenter = new instr.Instrumenter(topStatsd.getChildClient("sqs-poller"));
  var rcvInstr = new instr.Instrumenter(this._instrumenter, "msgSource");
  var incMsgs = rcvInstr.usingInstrFn("increment", "msgs.count", ut.get("length"));

  var rcv = rcvInstr.instrumentCalls("rcv", this._rcv.bind(this), {count: true});

  this._msgSource = $((push, next) => {
    try {
      push(null, $(rcv().tap(incMsgs).delay(this._pollIntervalS * 1000)));
      next();
    } catch (e) {
      error("messageStream push/next failed: " + e);
      push(e);
    }
  })
    .mergeWithLimit(this.parallelPolls)
    .flatMap($.compose($, pause.waitFor(this._pauser)))
    .flatten()
    // NOTE: this "rethrowing" map is here because Promise rejection doesn't seem to propagate when using generator functions that push streams.
    // The generator function above does push(null, $(promise)), but rejected promises don't turn into stream errors
    .map((val) => {
      if(_.isError(val)) {
        throw val;
      }
      return val;
    });

  this._unknownMsgs = this._msgSource.observe()
    .flatMap($.compose($, pause.waitFor(this._pauser)))
    .filter(_.negate(this.msgPassesFilter.bind(this)))
    .batchWithTimeOrCount(60 * 1000, 50)
    .doto((msgs) => {
      debug(`${this._id} deleting ${msgs.length} messages I don't know how to handle. URIs are ${ut.messagesToURIs(msgs)}`);
    })
    .flatMap((msgs) => $(this.deleteMsgs(msgs))).flatten();


  this.messageStream = this._msgSource.fork()
    .flatMap($.compose($, pause.waitFor(this._pauser)))
    .filter(this.msgPassesFilter.bind(this))
  ;

  this._deletionStream = $();

  this._deletionStream.batchWithTimeOrCount(5000, 10)
    .flatMap(messages => {
      this.visTimeoutUpdater.removeMsgs(messages);
      var entries = _.map(messages, (msg, idx) => ({Id: idx.toString(), ReceiptHandle: msg.ReceiptHandle}));
      return $(new Promise((ok, err) => this.sqs.deleteMessageBatch({Entries: entries}).on('success', ok).on('error', err).send())
        .return(ut.messagesToURIs(messages)));
    })
    .each(() => {}); // make sure the stream gets thunked

};

util.inherits(Poller, EventEmitter);

Poller.prototype.start = function() {
  if(!this._started) {
    debug(`Starting poller ${this._id}`);
/*    this._msgSource.resume();
    this.messageStream.resume();
    this._unknownMsgs.resume();*/
    this._pauser.unpause();
    this._started = true;
  }
};

Poller.prototype.stop = function() {
  if(this._started) {
    debug(`stopping poller ${this._id}`);
    this._pauser.pause();
    this.visTimeoutUpdater.stop();
    this._started = false;
  }
};

Poller.prototype.msgPassesFilter = function(msg) {
  return _.any(ut.messageToURIs(msg), this._filter);
};

Poller.prototype._rcv = function () {
  return new Promise((resolve, reject) => {
    var req = this.sqs.receiveMessage();
    req.on('success', (resp) => {
      if (resp && resp.data) {
        var msgs = resp.data.Messages || [];
        this.visTimeoutUpdater.addMsgs(msgs);
        resolve(_.map(msgs, msg => new EnrichedMessage(msg, this)));
      } else {
        error("[_rcv] received no data?!");
        resolve([]);
      }
    });

    req.on('error', (err) => {
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
    return Promise.resolve([]);
  }

  var entries = _.map(messages, (msg, idx) => ({Id: idx.toString(), ReceiptHandle:  msg.ReceiptHandle}));

  var chunked = _.chunk(entries, 10); // 10 is the maximum allowed by SQS

  return Promise.map(chunked, (entryChunk) => new Promise((ok, err) =>
    this.sqs.deleteMessageBatch({Entries: entryChunk}).on('success', ok).on('error', err).send()))
    .return(ut.messagesToURIs(messages));
};

function EnrichedMessage(origMessage, poller) {
  this.Body = origMessage.Body;
  this.ReceiptHandle = origMessage.ReceiptHandle;
  this._poller = poller;
  this._uris = null;
}

EnrichedMessage.prototype.deleteMsg = function deleteMsg() {
  this._poller._deletionStream.write(this);
};

EnrichedMessage.prototype.toURIs = function toURIs() {
  if(!this._uris) {
    this._uris = ut.eventToS3URIs(ut.tryParse(this));
  }

  return this._uris;
};

function VisTimeoutUpdater(sqs, options) {
  this._msgMap = {};
  this._visibilityTimeout = options.visibilityTimeoutSeconds || 5 * 60;
  this._interval = (options.visibilityUpdateIntervalSeconds || this._visibilityTimeout / 3) * 1000;
  this._sqs = sqs;
  this._id = _.last(this._sqs.config.params.QueueUrl.split("/"));
  this._started = false;
}

VisTimeoutUpdater.prototype.addMsgs = function(msgs) {
  if(msgs.length == 0) {
    return;
  }

  _.each(msgs, msg => this._msgMap[msg.ReceiptHandle] = msg);
};

VisTimeoutUpdater.prototype.removeMsgs = function(msgs) {
  if(msgs.length == 0) {
    return;
  }

  _.each(msgs, msg => delete this._msgMap[msg.ReceiptHandle]);
};

VisTimeoutUpdater.prototype.start = function() {
  debug(`starting VisTimeoutUpdater ${this._id}`);
  this._started = true;
  this._updateLoop();
};

VisTimeoutUpdater.prototype._updateLoop = function() {
  if(!this._started) {
    return;
  }

  var msgs = _.values(this._msgMap);
  var nMsgs = msgs.length;

  if(nMsgs == 0) {
    return Promise.delay(this._interval).tap(() => {
        setImmediate(this._updateLoop.bind(this));
      });
  }

  var runId = _.uniqueId("vtu-");
  debug(`VisTimeoutUpdater ${this._id} runId ${runId} updating timeouts for ${nMsgs} messages`);

  var entries = _.map(msgs, (msg, idx) => ({Id: idx.toString(), ReceiptHandle: msg.ReceiptHandle,
    VisibilityTimeout: this._visibilityTimeout}));

  var chunked = _.chunk(entries, 10); // 10 is the maximum allowed by SQS

  return Promise.map(chunked, (entryChunk) => new Promise((ok, err) =>
      this._sqs.changeMessageVisibilityBatch({Entries: entryChunk}).on('success', ok).on('error', err).send())
    .catch(e => {
      error(`${this._id} runId ${runId} error updating visibility timeouts for ${entryChunk.length} messages: ${e}`);
      return {data: {}}
    })
    .tap(res => {
      if (res.data.Failed && res.data.Failed.length) {
        error(`VisTimeoutUpdater ${this._id} runId ${runId} failed to update visibility timeout for ${res.data.Failed.length} messages`);
      }
    }))
    .tap(() => {
      debug(`VisTimeoutUpdater ${this._id} runId ${runId} update done`);
    })
    .delay(this._interval)
    .tap(() => {
      setImmediate(this._updateLoop.bind(this));
    });
};

VisTimeoutUpdater.prototype.stop = function() {
  if(this._started) {
    debug(`stopping VisTimeoutUpdater ${this._id}`);
    this._started = false;
  }
};

module.exports.EnrichedMessage = EnrichedMessage;