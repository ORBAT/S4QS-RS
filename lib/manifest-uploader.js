/**
 * Created by teklof on 17.2.15.
 */
var ut = require('./utils');
var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');
var util = require('util');
var Promise = require('bluebird');
var Statsd = require('statsd-client');
var instr = require('./instrumentation');
var dbg = require('debug');
var _enabledOrig = dbg.enabled; // NOTE: temporarily force debug logging on
dbg.enabled = function(ns) {
  if(/s3-to-rs/.test(ns)) return true; else return _enabledOrig(ns);
};
var debug = dbg('s4qs-rs:manifest');
var error = dbg('s4qs-rs:manifest:error');
var $ = require('highland');
error.log = console.error;

var inspect = _.partialRight(util.inspect, {depth: 10});


/**
 * Bundles SQS messages containing S3 object creation events into manifest files. Emits 'manifest' events with
 * uploaded manifests, represented by Manifest instances.
 *
 * Manifest URIs will be of the form s3://[bucket]/[prefix][Redshift table name]-[timestamp]-[5 random letters].json
 *
 * where [prefix] can be anything that's kosher for an S3 key.
 *
 * @param {Object} s3 An initialized AWS S3 object. The params passed to its constructor <i>must</i> include
 * ACL
 * @param {Number} options.maxToUpload minimum amount of files to include in each manifest
 * @param {Number} options.maxWaitSeconds send a manifest after maxWaitSeconds seconds, even if the number
 * of files is smaller than options.maxToUpload
 * @param {Boolean} options.mandatory The value of the "mandatory" property of each manifest entry. See Redshift
 * COPY documentation http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
 * @param {String} options.bucket S3 bucket to upload manifests to
 * @param {String} options.prefix Key prefix to use for manifests
 * @param {Function} options.grouper Function that takes an S3 URI and turns it into an
 * array of table names (S3 events have a Records array which can ostensibly contain more than one record).
 * Used for grouping SQS messages into the correct manifest.
 * @constructor
 * @type {Uploader}
 */
var Uploader = exports.Uploader = function Uploader(s3, options) {
  EventEmitter.call(this);

  if (!s3) {
    throw new Error("s3 required");
  }

  if (!options) {
    throw new Error("No options provided");
  }

  if (!options.grouper) {
    throw new Error("options.grouper required");
  }

  if (!options.maxToUpload) {
    throw new Error("options.maxToUpload required");
  }

  if (!options.maxWaitSeconds) {
    throw new Error("options.maxWaitSeconds required");
  }

  if (!options.bucket) {
    throw new Error("no bucket specified");
  }

  if (!options.prefix) {
    throw new Error("no key prefix specified");
  }

  this._maxRetries = options.retries || 0;

  if (_.isUndefined(options.mandatory)) {
    throw new Error("options.mandatory required");
  }

  var topStatsd = new Statsd(options.statsd);
  this._instrumenter = new instr.Instrumenter(topStatsd.getChildClient("manif-uploader"));

  this._grouper = options.grouper;
  this._mandatory = options.mandatory;
  this._minToUpload = options.maxToUpload;
  this._maxWaitSeconds = options.maxWaitSeconds;
  this._bucket = options.bucket;
  this._prefix = options.prefix;
  this._options = options;
  this._s3 = s3;
  this._manifestGroups = {};

  // TODO: add property with in-progress uploads, return it from stop()

};

util.inherits(Uploader, EventEmitter);

Uploader.prototype.stop = function stop() {
  // TODO: do something about manifests that have already been uploaded?
};

Uploader.prototype.start = function start() {

};

/**
 * Takes a message stream, batches it and produces a stream of streams of manifests (each manifest is in its own
 * stream.) Probably best used with highland's through() and sequence() / parallel()
 * @param {Stream} msgStream Stream of SQS messages
 * @returns {Stream} A stream of streams of manifests.
 */
Uploader.prototype.msgsToManifests = function(msgStream) {
  var _instr = new instr.Instrumenter(this._instrumenter, "msgsToManifests");
  var gauger = _instr.usingInstrFn("gauge", "batchsize", ut.get("length"));
  var perGroupIncr = _instr.postfixedInstrFn("increment", "msgcount.pergroup");

  return msgStream
    .batchWithTimeOrCount(this._maxWaitSeconds * 1000, this._minToUpload)
    .map(gauger)
    .map(function (msgs) {
      debug("msgsToManifests msgs " + msgs.length);
      // not using groupBy since it could be possible a message belongs to several groups,
      // i.e. it could have multiple S3 events (although currently each message only seems to have 1 event)
      var grouped = _.reduce(msgs, function (acc, msg) {
        var uris = _messageToURIs(msg)
          , groups = _.map(uris, this._grouper)
          ;
        _.each(groups, function (group) {
          acc[group] = (acc[group] || []).concat(msg)
        });
        return acc;
      }.bind(this), {});

      var mfStreams = _.map(grouped, function (msgs, grp) {
        debug("msgsToManifests got " + msgs.length + " messages for group " + grp);
        perGroupIncr(grp, msgs.length);
        var mf = this._newManifest(grp);
        mf._addAll(msgs);
        return $(_instr.instrumentCalls("upload", mf._upload.bind(mf))());
      }.bind(this)); // Array[Stream[Promise[Manifest]]]

      return $(mfStreams).sequence(); // Stream[Stream[Promise[Manifest]]] --> Stream[Stream[Manifest]]

    }.bind(this));
};

Uploader.prototype._newManifest = function _newManifest(group) {

  var manifest = new Manifest(this._s3,
    {mandatory: this._mandatory
      , bucket: this._bucket
      , prefix: this._prefix
      , table: group
      , retries: this._maxRetries
      , statsd: this._options.statsd
    });

  debug("Created new manifest for " + group);
  this._manifestGroups[group] = manifest;
  return manifest;
};


/**
 * A Manifest contains S3 URIs/SQS messages and can be turned into a manifest file for Redshift's COPY.
 * The manifestURI and table properties are meant for users of Manifest, all other properties are intended to be
 * private.
 * @constructor
 * @param {Object} s3 Initialized S3 client
 * @param {Boolean} options.mandatory if true, all URIs will have mandatory:true in the manifest
 * @param {String} options.bucket Bucket to upload to
 * @param {String} options.prefix Key prefix to use
 * @type {Manifest}
 */
var Manifest = exports.Manifest = function Manifest(s3, options) {
  this._mandatory = !!options.mandatory;
  this._uris = [];
  this._msgs = [];
  this._s3 = s3;
  this._bucket = options.bucket;
  // the table this manifest is meant for
  this.table = options.table;

  this._maxRetries = options.retries || 0;

  var datePart = new Date().toISOString().split("T")[0] + "/"; // get YYYY-MM-DD part of date

  this._key = options.prefix + datePart + this.table + '-' + ut.randomString(7) + ".json";
  // this is the S3 URI of this manifest
  this.manifestURI = "s3://" + this._bucket + "/" + this._key;

  this._retryBackoff = 1000; // initial retry backoff in milliseconds

  var defer = ut.defer();
  this._promise = defer.promise;
  this._resolve = defer.resolve;
  this._reject = defer.reject;

  var topStatsd = new Statsd(options.statsd);
  this._instrumenter = new instr.Instrumenter(topStatsd.getChildClient("manif-uploader.manifests"));
  this._uploadInstr = new instr.Instrumenter(this._instrumenter, "upload");

  this._lastError;
};

/**
 * Tries to upload the Manifest to S3. After the upload completes, the Manifest's promise property will be fulfilled
 * with the Manifest.
 * In case of errors the promise will be rejected with the error.
 *
 * @return {Promise} A Promise of the manifest
 * @private
 */
Manifest.prototype._upload = function _upload(retries) {
  if(_.isUndefined(retries)) {
    retries = 0;
  }

  var uplInstr = this._uploadInstr;

  debug("Uploading " + this.manifestURI);

  var req = this._s3.putObject({ContentType: "application/json",
    Bucket: this._bucket, Key: this._key, Body: JSON.stringify(this.toJSON())});

  req.on('success', function () {
    debug("Uploaded manifest " + this.manifestURI);
    this._resolve(this);
  }.bind(this));

  req.on('error', function(err) {
    var after = Math.pow(1.5, retries) * this._retryBackoff;
    error("Error uploading manifest " + this.manifestURI +": " + err);
    uplInstr.instrumenterFn("increment", "error")();
    this._lastError = err;

   if(retries + 1 > this._maxRetries) {
     this._reject(this._lastError);
     return;
   }
    debug("Retrying " + this.manifestURI + " " + (retries + 1) + "/" + this._maxRetries + " after " + after.toFixed(0) + "ms")
    return Promise.delay(after).bind(this).then(function() {
      this._upload(retries + 1);
    });
  }.bind(this));
  req.send();

  return this._promise;

};

/**
 * Deletes this manifest from S3 and returns a promise that'll be fulfilled with the deleted manifest's URI, or rejected
 * with S3's error in case of deletion error.
 * result.
 * @return {Promise} Promise of deletion result
 */
Manifest.prototype.delete = function delete_() {
  var self = this;
  return new Promise(function (resolve, reject) {
    debug("Deleting manifest " + self.manifestURI);
    var req = self._s3.deleteObject({Bucket: self._bucket, Key: self._key});
    req.on('success', function () {
      debug("Deleted manifest " + self.manifestURI);
      resolve(self.manifestURI);
    });
    req.on('error', function(err) {
      error("Error deleting manifest " + self.manifestURI + ": " + err);
      reject(err, self);
    });
    req.send();
  });
};

/**
 * Number of URIs in the Manifest
 */
Object.defineProperty(Manifest.prototype, "length", {
  get: function () {return this._uris.length}, set: function (n) {this._uris.length = n}
});

/**
 * Array of SQS messages in the Manifest
 */
Object.defineProperty(Manifest.prototype, "msgs", {
  get: function () {return _.clone(this._msgs)}
});

/**
 * Array of URIs in the Manifest
 */
Object.defineProperty(Manifest.prototype, "uris", {
  get: function () {return _.clone(this._uris)}
});

/**
 * Turn a Manifest into Redshift COPY manifest JSON
 * @return {{entries: *}}
 */
Manifest.prototype.toJSON = function toJSON() {
  var self = this;
  return {entries: _.reduce(this._uris, function (acc, uri) {
    return acc.concat({url: uri, mandatory: self._mandatory});
  }, [])};
};

/**
 * Adds SQS messages containing S3 events to the Manifest
 * @param {Array} msgs Messages to add
 * @return {Number} new amount of URIs in the Manifest
 * @private
 */
Manifest.prototype._addAll = function _addAll(msgs) {
  if (!msgs || msgs.length == 0) {
    return this._uris.length;
  }

  _.each(msgs, this._push.bind(this));
  return this.length;
};

/**
 * Add SQS message to the Manifest
 * @param {String} msg S3 URI to add
 * @return {Number} new amount of URIs in the Manifest
 * @private
 */
Manifest.prototype._push = function _push(msg) {
  if (!msg) {
    return this._uris.length;
  }

  this._msgs.push(msg);
  this._uris = this._uris.concat(_messageToURIs(msg));

  return this.length;
};

var _messageToURIs = ut.messageToURIs;

var _messagesToURIs = ut.messagesToURIs;