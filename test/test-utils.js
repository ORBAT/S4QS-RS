/**
 * Created by teklof on 31.1.15.
 */
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var LRU = require('lru-cache');
var _ = require('lodash');
var Promise = require('bluebird');
var debug = require('debug')('test-utils');
var inspect = _.partialRight(util.inspect, {depth: 10});
var sinon = require('sinon');
var ut = require('../lib/utils');
var $ = require('highland');
var sinonAsPromised = require('sinon-as-promised')(Promise);

var randomString = exports.randomString = function randomString(len) {
  var charCodes = _.times(len, function () {
    return _.random(97, 122);
  });
  return String.fromCharCode.apply(null, charCodes);
};

var FakePoller = exports.FakePoller = function FakePoller(prefix) {
  this.handles = LRU(); // infinitely big
  this.prefix = prefix;
  this.messageStream = $();
};

FakePoller.prototype.deleteMsgs = function(messages) {
  if(!messages.length) {
    return Promise.resolve();
  }

  var self = this;

  return Promise.all(_.map(messages, function (msg) {
    if(self.handles.get(msg.ReceiptHandle)) {
      self.handles.del(msg.ReceiptHandle);
      return Promise.resolve("ok");
    } else {
      return Promise.reject(new Error(msg.ReceiptHandle + " not found"));
    }
  }));
};

var SQSMessage = exports.SQSMessage = function SQSMessages(n, bukkit, prefix) {
  this.ResponseMetadata = {
    RequestId: randomString(8)
  };

  if (n) {
    this.Messages = _.times(n, function () {
      var s3Event = new S3Event(bukkit, prefix);
      return {
        MessageId: randomString(16), ReceiptHandle: randomString(32), Body: JSON.stringify(s3Event), _s3Event: s3Event
      }
    });
  }
};

SQSMessage.prototype.s3URIs = function s3URIs() {
  return _.flatten(ut.splat(ut.send('s3URIs'))(_.pluck(this.Messages, '_s3Event')));
};

SQSMessage.prototype.handles = function() {
  return _.pluck(this.Messages, 'ReceiptHandle');
};

var S3Event = exports.S3Event = function S3Event(bukkit, prefix) {
  this.Records = [
    {
      "eventVersion": "2.0",
      "s3": {
        "s3SchemaVersion": "1.0",
        "bucket": {
          "name": bukkit
        },
        "object": {
          "key": prefix + randomString(24)
        }
      }
    }
  ];
};

S3Event.prototype.s3URIs = function s3URIs() {
  return _.map(this.Records, function (record) {
    return "s3://" + record.s3.bucket.name + "/" + record.s3.object.key;
  });
};

var FakePg = exports.FakePg = function FakePg(connErr, queryErr, doneCb) {
  this.connErr = connErr;
  this.queryErr = queryErr;
  this.doneCb = doneCb;
  this.client = null;
};

FakePg.prototype.connect = function connect(connStr, cb) {
  var self = this;


  var client = {
    query: sinon.stub().yields(self.queryErr)
    , queryAsync: sinon.stub()
    /*function query(query, cb) {
      setImmediate(cb.bind(null, self.queryErr));
    }*/
  };

  if(self.queryErr) {
    client.queryAsync.rejects(self.queryErr);
  } else {
    client.queryAsync.resolves();
  }

  setImmediate(
    function () {
    self.client = client;
    cb(self.connErr, client, self.doneCb);
  });
};

var FakeAWSReq = exports.FakeAWSReq = function FakeAWSReq(eventName, content) {
  EventEmitter.call(this);
  this.eventName = eventName;
  this.content = content;
};

util.inherits(FakeAWSReq, EventEmitter);

FakeAWSReq.prototype.send = function send() {
  setImmediate(this.emit.bind(this, this.eventName, this.content));
};

var FakeSQS = exports.FakeSQS = function FakeSQS(rcv, del) {
  this.rcv = rcv || {};
  this.del = del || {};
};

FakeSQS.prototype.receiveMessage = function receiveMessage() {
  return new FakeAWSReq(this.rcv.event, this.rcv.content);
};


FakeSQS.prototype.deleteMessageBatch = function deleteMessageBatch() {
  return new FakeAWSReq(this.del.event, this.del.content);
};


var FakeS3 = exports.FakeS3 = function FakeS3(put, del) {
  this.put = put || {};
  this.del = del || {};
};

FakeS3.prototype.putObject = function putObject(params) {
  return new FakeAWSReq(this.put.eventName, this.put.content);
};


FakeS3.prototype.deleteObject = function deleteObject(params) {
  return new FakeAWSReq(this.del.eventName, this.del.content);
};

var FakeRedshift = exports.FakeRedshift = function FakeRedshift(clusterId, clusterStatus, fail) {
  this.clusterId = clusterId;
  this.clusterStatus = clusterStatus;
  this.fail = !!fail;
};

FakeRedshift.prototype.describeClusters = function describeClusters(params, callback) {
  var err, res
    , clusterId = this.clusterId
    , clusterStatus = this.clusterStatus
    ;

  if(this.fail) {
    err = new Error("FALE");
  } else {
    res = {
      Clusters: [
        {
          ClusterIdentifier: clusterId
          , ClusterStatus: clusterStatus
        }
      ]
    };
  }
  if(_.isFunction(params)) {
    callback = params;
  }

  setImmediate(_.bind(callback, callback, err, res));
};