var _ = require('lodash');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var when = require('when');
var debug = require('debug')('sqs-poller');
var error = require('debug')('sqs-poller:error');
error.log = console.error;
var LRU = require('lru-cache');
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

  this.repeatPoll = options.repeatPoll || 1;
  this.sqs = sqs;
  this.seenMsgs = LRU({max: 1000});
  // if an poll call is running, this'll contain a promise that'll be fulfilled when it finishes
  this.pollPending = null;
};

util.inherits(Poller, EventEmitter);

Poller.prototype._markSeen = function(msgs) {
  var self = this;
  _.each(msgs, function (msg) {
    self.seenMsgs.set(msg.MessageId, true);
  });
};

Poller.prototype._dedup = function(msgs) {
  var self = this;
  var grouped = _.groupBy(msgs, function (msg) {
    return !!self.seenMsgs.get(msg.MessageId); // bang bang to coerce undefined into a bool
  }); // {true: [duplicates], false: [nondups]}

  var dups = grouped.true || [];
  var nonDups = grouped.false || [];

  if (dups.length > 0) {
    debug("Found " + dups.length + " duplicate messages. Deleting");
    return this.deleteMsgs(dups)
      .catch(function (err) {
        error("Error deleting duplicates: " + err);
      })
      .yield(nonDups)
      .tap(function () {
        debug("Duplicates deleted");
      });
  }

  return when(nonDups);

};

Poller.prototype._rcv = function () {
  var self = this
    , req = this.sqs.receiveMessage()
    , msgsDefer = when.defer();

  req.on('success', function (resp) {
    if (resp && resp.data) {
      var msgs = resp.data.Messages;

      if (!msgs || msgs.length == 0) {
        debug('[_rcv] no messages in reply');
      }

      msgsDefer.resolve(msgs || []);
    } else {
      error("[_rcv] received no data?!");
      msgsDefer.resolve([]);
    }
  });

  req.on('error', function (err) {
    error("[_rcv] request error: " + err);
    msgsDefer.resolve([]);
    self.emit('error', err);
  });

  req.send();
  return msgsDefer.promise;
};


Poller.prototype.poll = function () {
  debug("Polling for new messages");
  var self = this
    , deferred = when.defer()
    ;

  this.pollPending = deferred.promise;
  when.all(_.times(this.repeatPoll, function() {
    return self._rcv();
  })).then(_.flatten)
    .done(function(msgs) {
      debug("[poll] total messages received: " + msgs.length);
      if (msgs.length != 0) {
        self._dedup(msgs).done(function(nonDups) {
          self._markSeen(nonDups);
          self.emit('messages', nonDups);
        });
      } else {
        error('[poll] no messages in reply');
      }

      deferred.resolve();
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
    return when();
  }

  var self = this;

  var entries = _.map(messages, function (msg, idx) {
    return {Id: idx.toString(), ReceiptHandle:  msg.ReceiptHandle}
  });

  var chunked = _.chunk(entries, 10); // 10 is the maximum allowed by SQS

  var delPs = _.map(chunked, function (entryChunk) {
    return when.promise(function (ok, err) {
      self.sqs.deleteMessageBatch({Entries: entryChunk}).on('success', ok).on('error', err).send();
    });
  });

  return when.all(delPs);

};