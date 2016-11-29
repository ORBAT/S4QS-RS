const _ = require('lodash');
const $ = require('highland');
const util = require('util');
const ut = require('./utils');
const Promise = require('bluebird');
const pause = require('promise-pauser');
const Statsd = require('statsd-client');
const instr = require('./instrumentation');

const moduleName = "sqs-poller";

/**
 * Creates a new SQS poller
 */
const Poller = module.exports.Poller = function Poller(sqs, options) {

  options = options || {};

  if(!options.logger) {
    throw new Error("no logger");
  }

  if (!sqs) {
    throw new Error("sqs must be non-null");
  }

  if(!_.isFunction(options.filter)) {
    throw new Error("filter must be a function");
  }

  this.sqs = sqs;
  this._id = _.last(this.sqs.config.params.QueueUrl.split("/"));

  this._logger = options.logger.child({module: moduleName, pollerId: this._id});

  this._options = options;

  this._filter = options.filter;
  this._pollIntervalS = options.pollIntervalSeconds || 60;
  this.parallelPolls = options.parallelPolls || 1;


  let visTOptions = options.visibilityTimeoutUpdater || {};
  visTOptions.logger = this._logger;
  this.visTimeoutUpdater = new VisTimeoutUpdater(this.sqs, visTOptions);
  this.visTimeoutUpdater.start();

  this._pauser = pause.pauser();
  this._pauser.pause();

  const topStatsd = new Statsd(options.statsd);

  this._instrumenter = new instr.Instrumenter(topStatsd, moduleName);
  const rcvInstr = new instr.Instrumenter(this._instrumenter, "msgSource");
  const incMsgs = rcvInstr.usingInstrFn("increment", "msgs.count", ut.get("length"));

  const rcv = rcvInstr.instrumentCalls("rcv", this._rcv.bind(this), {count: true});

  this._msgSource = $((push, next) => {
    try {
      push(null, $(rcv().tap(incMsgs).delay(this._pollIntervalS * 1000)));
      next();
    } catch (e) {
      this._logger.error({err: e}, "messageStream push/next failed");
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
    .tap(ut.splat(ut.send("stopVisibilityUpd")))
    .doto((msgs) => {
      const chunked = _.chunk(msgs, 20);
      _.each(chunked, chunk => this._logger.warn({uris: ut.messagesToURIs(chunk)}, "Ignoring messages I don't know how to handle"))

    })
    .each(() => {});  // make sure the stream gets thunked


  this.messageStream = this._msgSource.fork()
    .flatMap($.compose($, pause.waitFor(this._pauser)))
    .filter(this.msgPassesFilter.bind(this))
  ;

  this._deletionStream = $();

  this._deletionStream
    .batchWithTimeOrCount(5000, 10)
    .tap(ut.splat(ut.send("stopVisibilityUpd")))
    .flatMap(messages => {
      const entries = _.map(messages, (msg, idx) => ({Id: idx.toString(), ReceiptHandle: msg.ReceiptHandle}));
      return $(new Promise((ok, err) => this.sqs.deleteMessageBatch({Entries: entries}).on('success', ok).on('error', err).send())
        .return(ut.messagesToURIs(messages)));
    })
    .errors(err => this._logger.warn({err: err}, "Error deleting messages"))
    .each(() => {}); // make sure the stream gets thunked

};

Poller.prototype.start = function() {
  if(!this._started) {
    this._logger.info({QueueUrl: this.sqs.config.params.QueueUrl}, "Starting");
    this._pauser.unpause();
    this._started = true;
  }
};

Poller.prototype.stop = function() {
  if(this._started) {
    this._logger.info("Stopping");
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
    const req = this.sqs.receiveMessage();
    req.on('success', (resp) => {
      if (resp && resp.data) {
        const msgs = resp.data.Messages || [];
        const enriched = _.map(msgs, msg => new EnrichedMessage(msg, this));
        this.visTimeoutUpdater.addMsgs(enriched);
        resolve(enriched);
      } else {
        this._logger.warn("_rcv received no data?!");
        resolve([]);
      }
    });

    req.on('error', (err) => {
      this._logger.error({err: err}, "_rcv request error");
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

  const entries = _.map(messages, (msg, idx) => ({Id: idx.toString(), ReceiptHandle:  msg.ReceiptHandle}));

  const chunked = _.chunk(entries, 10); // 10 is the maximum allowed by SQS

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

EnrichedMessage.prototype.stopVisibilityUpd = function stopVisibilityUpd() {
  this._poller.visTimeoutUpdater.removeMsg(this);
};

EnrichedMessage.prototype.toURIs = function toURIs() {
  if(!this._uris) {
    this._uris = ut.eventToS3URIs(ut.tryParse(this));
  }

  if(!this._uris) {
    this._poller._logger.error({Body: this.Body}, "Message doesn't have S3 URIs?")
  }

  return this._uris;
};

function VisTimeoutUpdater(sqs, options) {
  this._msgMap = {};
  this._visibilityTimeout = options.visibilityTimeoutSeconds || 5 * 60;
  this._interval = (options.visibilityUpdateIntervalSeconds || this._visibilityTimeout / 3) * 1000;
  this._maxFailures = (options.maxFailures || 3);
  this._sqs = sqs;
  this._id = _.last(this._sqs.config.params.QueueUrl.split("/"));
  this._started = false;
  this._logger = options.logger.child({module: "VisTimeoutUpd"});
}

VisTimeoutUpdater.prototype.addMsgs = function(msgs) {
  if(msgs.length == 0) {
    return;
  }

  _.each(msgs, msg => {msg._nTimesFailed = 0; this._msgMap[msg.ReceiptHandle] = msg});
};

VisTimeoutUpdater.prototype.removeMsg = function(msg) {
  delete this._msgMap[msg.ReceiptHandle];
};

VisTimeoutUpdater.prototype.start = function() {
  this._logger.info("Starting");
  this._started = true;
  this._updateLoop();
};

VisTimeoutUpdater.prototype._markFailed = function(msgs) {
  _.each(msgs, msg => {
    if(this._msgMap[msg.ReceiptHandle]) {
      this._msgMap[msg.ReceiptHandle]._nTimesFailed++
    }
  })
};

VisTimeoutUpdater.prototype._removeFailed = function (maxFails) {
  const failed = _.filter(_.values(this._msgMap), it => it._nTimesFailed > maxFails);

  _.each(failed, it => {
    // ${it.toURIs()} ${it._nTimesFailed} times. Updates will be stopped
    this._logger.warn({uris: it.toURIs(), nTimesFailed: it._nTimesFailed}, "Stopping visibility updates for this message since it has failed too many times");
    this.removeMsg(it);
  });
};

VisTimeoutUpdater.prototype._updateLoop = function() {
  if(!this._started) {
    return;
  }

  this._removeFailed(this._maxFailures);

  const msgs = _.values(this._msgMap);
  const nMsgs = msgs.length;

  if(nMsgs == 0) {
    return Promise.delay(this._interval).tap(() => {
        setImmediate(this._updateLoop.bind(this));
      });
  }

  const entries = _.map(msgs, (msg, idx) => ({Id: idx.toString(), ReceiptHandle: msg.ReceiptHandle,
    VisibilityTimeout: this._visibilityTimeout}));

  const chunked = _.chunk(entries, 10); // 10 is the maximum allowed by SQS

  return Promise.map(chunked, (entryChunk) => new Promise((ok, err) =>
      this._sqs.changeMessageVisibilityBatch({Entries: entryChunk}).on('success', ok).on('error', err).send())
    .catch(e => {
      this._logger.warn({err: e}, "Error calling ChangeMessageVisibilityBatch");
      return {data: {}}
    })
    .tap(res => {
      const fails = res.data.Failed;

      if (fails && fails.length) {
        const badMsgs = _.reduce(fails, (acc, it) => {
          const idx = Number.parseInt(it.Id);
          if (!_.isNaN(idx)) {
             acc.push(msgs[idx])
          }
          return acc;
        }, []);

        this._logger.warn({uris: ut.messagesToURIs(badMsgs), errorCodes: _.uniq(_.pluck(fails, "Code"))}, "Error updating visibility timeouts");
        this._markFailed(badMsgs);
      }
    }))
    .delay(this._interval)
    .tap(() => {
      setImmediate(this._updateLoop.bind(this));
    });
};

VisTimeoutUpdater.prototype.stop = function() {
  if(this._started) {
    this._logger.info("Stopping");
    this._started = false;
  }
};

module.exports.EnrichedMessage = EnrichedMessage;