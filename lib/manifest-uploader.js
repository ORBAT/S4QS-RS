/**
 * Created by teklof on 17.2.15.
 */
const ut = require('./utils');
const _ = require('lodash');
const util = require('util');
const Promise = require('bluebird');
const Statsd = require('statsd-client');
const $ = require('highland');
const instr = require('./instrumentation');
const moduleName = "manif-uploader";

/**
 *
 * The Uploader turns input streams of S3 object creation SQS messages into streams that output Redshift manifest files that
 * contain the URIs of the S3 objects.
 *
 * Uploader has one input and one output stream per provided base table name, so if you have 3 tables defined, there will
 * be 3 input and 3 output streams. The output streams are of the type Stream[Stream[Manifest]], and the "inner" stream
 * is readable when the Manifest has finished uploading itself to S3.
 *
 * Manifest URIs will be of the form s3://[bucket]/[prefix][base table name]-[timestamp]-[5 random letters].json
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
 * @constructor
 * @type {Uploader}
 */
const Uploader = exports.Uploader = function Uploader(s3, options) {
  if (!s3) {
    throw new Error("s3 required");
  }

  if (!options) {
    throw new Error("No options provided");
  }

  if(!options.logger) {
    throw new Error("no logger");
  }

  if (!options.maxToUpload) {
    throw new Error("options.maxToUpload required");
  }

  if (!options.maxWaitSeconds) {
    throw new Error("options.maxWaitSeconds required");
  }

  if (!options.bucket) {
    throw new Error("options.bucket required");
  }

  if (!options.prefix) {
    throw new Error("options.prefix required");
  }

  if(!options.baseTables && !_.isArray(options.baseTables)) {
    throw new Error("options.baseTables required & must be an array");
  }

  this._baseNames = options.baseTables;

  this._maxRetries = options.retries || 0;

  if (_.isUndefined(options.mandatory)) {
    throw new Error("options.mandatory required");
  }

  const topStatsd = new Statsd(options.statsd);

  this._instrumenter = new instr.Instrumenter(topStatsd, moduleName);

  this._logger = options.logger.child({module: moduleName});

  this._mandatory = options.mandatory;
  this._maxToUpload = options.maxToUpload;
  this._maxWaitSeconds = options.maxWaitSeconds;
  this._bucket = options.bucket;
  this._prefix = options.prefix;
  this._options = options;
  this._s3 = s3;
};

/**
 * Takes a message stream, batches it and produces a stream of streams of manifests (each manifest is in its own
 * stream.) Probably best used with highland's through() and sequence() / parallel()
 * @param {Stream} msgStream Stream of SQS messages
 * @param {String} baseName
 * @returns {Stream} A stream of streams of manifests.
 */
Uploader.prototype.msgsToManifests = function(baseName, msgStream) {
  const _instr = new instr.Instrumenter(this._instrumenter, "msgsToManifests")
    , gauger = _instr.usingInstrFn("gauge", "batchsize", ut.get("length"))
    , perGroupIncr = _instr.postfixedInstrFn("increment", "msgcount.pergroup")
    , logger = this._logger.child({baseName: baseName});

  logger.debug(`started msgsToManifests`);
  return msgStream
    .batchWithTimeOrCount(this._maxWaitSeconds * 1000, this._maxToUpload)
    .map(gauger)
    .map((msgs) => {
      logger.debug({nMsgs: msgs.length}, "msgsToManifests got a batch of messages");

      if(logger.debug()) {
        const chunked = _.chunk(msgs, 20);
        _.each(chunked, chunk => logger.debug({uris: ut.messagesToURIs(chunk)}, "URIs for new manifest"));
      }

      perGroupIncr(baseName, msgs.length);
      const mf = this._newManifest(baseName);
      mf._addAll(msgs);
      return $(_instr.instrumentCalls("upload", mf._upload.bind(mf))());
    });
};

Uploader.prototype._newManifest = function _newManifest(baseName) {
  return new Manifest(this._s3,
    {mandatory: this._mandatory
      , bucket: this._bucket
      , prefix: this._prefix
      , baseName: baseName
      , retries: this._maxRetries
      , statsd: this._options.statsd
      , logger: this._logger
    });
};


/**
 * A Manifest contains S3 URIs/SQS messages and can be turned into a manifest file for Redshift's COPY.
 * The manifestURI and baseName properties are meant for users of Manifest, all other properties are intended to be
 * private.
 * @constructor
 * @param {Object} s3 Initialized S3 client
 * @param {Boolean} options.mandatory if true, all URIs will have mandatory:true in the manifest
 * @param {String} options.bucket Bucket to upload to
 * @param {String} options.prefix Key prefix to use
 * @type {Manifest}
 */
const Manifest = exports.Manifest = function Manifest(s3, options) {
  this._mandatory = !!options.mandatory;
  this._uris = [];
  this._msgs = [];
  this._s3 = s3;
  this._bucket = options.bucket;
  this.baseName = options.baseName;

  this._maxRetries = options.retries || 0;

  const datePart = new Date().toISOString().split("T")[0] + "/"; // get YYYY-MM-DD part of date
  const dateAndFile = `${datePart}${this.baseName}-${ut.randomString(7)}.json`;

  this._key = `${options.prefix}inflight/${dateAndFile}`;
  this._path = `${this._bucket}/${this._key}`;

  // this is the S3 URI of this manifest, for use with COPY
  this.manifestURI = "s3://" + this._path;

  // the manifest will be moved here if the COPY went OK
  this._successPath = `${options.prefix}successful/${dateAndFile}`;
  // and here if not
  this._failPath = `${options.prefix}failed/${dateAndFile}`;
  this._retryBackoff = 1000; // initial retry backoff in milliseconds

  this._logger = options.logger.child({module: "Manifest", manifestURI: this.manifestURI});

  const defer = ut.defer();
  this._promise = defer.promise;
  this._resolve = defer.resolve;
  this._reject = defer.reject;

  const topStatsd = new Statsd(options.statsd);
  this._instrumenter = new instr.Instrumenter(topStatsd, "manif-uploader.manifests");
  this._uploadInstr = new instr.Instrumenter(this._instrumenter, "upload");
};

/**
 * Tries to upload the Manifest to S3. After the upload completes, the returned Promise will be fulfilled
 * with the Manifest.
 *
 * @return {Promise} A Promise of the manifest
 * @private
 */
Manifest.prototype._upload = function _upload(retries) {
  if(_.isUndefined(retries)) {
    retries = 0;
  }

  const uplInstr = this._uploadInstr;

  this._logger.debug({nMsgs: this.length}, "Starting upload");

  const req = this._s3.putObject({ContentType: "application/json",
    Bucket: this._bucket, Key: this._key, Body: JSON.stringify(this.toJSON())});

  req.on('success', () => {
    this._logger.debug("Upload done");
    this._resolve(this);
  });

  req.on('error', (err) => {
    const after = Math.pow(1.5, retries) * this._retryBackoff;
    this._logger.warn({err: err}, "Upload error");
    uplInstr.instrumenterFn("increment", "error")();
    this._lastError = err;

   if(retries + 1 > this._maxRetries) {
     this._reject(this._lastError);
     return;
   }

    this._logger.info({retries: retries, maxRetries: this._maxRetries}, `Will retry upload after ${after.toFixed(0)}ms`);
    return Promise.delay(after).then(() => this._upload(retries + 1));
  });
  req.send();

  return this._promise;

};

/**
 * Copies the manifest to either _successPath or _failPath depending on whether it was successfully handled or not,
 * and then deletes the manifest. Will return the result of #delete()
 * @param successful Whether the manifest was successfully handled
 */
Manifest.prototype.done = function done(successful) {
  return new Promise((resolve, reject) => {
    const params = {Bucket: this._bucket, CopySource: this._path, Key: successful ? this._successPath : this._failPath};
    const destPath = `s3://${params.Bucket}/${params.Key}`;
    this._logger.trace({destPath: destPath, successful: successful}, "Moving manifest");

    const req = this._s3.copyObject(params);

    req.on("success", () => {
      this._logger.trace("Move done");
      resolve(this.delete());
    });

    req.on("error", (err) => {
      this._logger.warn({err: err}, "Moving failed");
      reject(err);
    });

    req.send();
  });
};

/**
 * Deletes this manifest from S3 and returns a promise that'll be fulfilled with the deleted manifest's URI, or rejected
 * with S3's error in case of deletion error.
 * result.
 * @return {Promise} Promise of original S3 URI
 */
Manifest.prototype.delete = function delete_() {
  return new Promise((resolve, reject) => {
    this._logger.trace("Deleting");
    const req = this._s3.deleteObject({Bucket: this._bucket, Key: this._key});
    req.on('success', () => {
      this._logger.trace("Deleted");
      resolve(this.manifestURI);
    });
    req.on('error', (err) => {
      this._logger.error({err: err}, "Error when deleting");
      reject(err, this);
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
  return {entries: _.reduce(this._uris, (acc, uri) => acc.concat({url: uri, mandatory: this._mandatory}), [])};
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

const _messageToURIs = ut.messageToURIs;