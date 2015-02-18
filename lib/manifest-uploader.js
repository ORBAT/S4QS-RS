/**
 * Created by teklof on 17.2.15.
 */
var ut = require('./utils');
var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');
var util = require('util');
var Promise = require('bluebird');
var debug = require('debug')('manifest');
var error = require('debug')('manifest:error');
var $ = require('highland');
error.log = console.error;

var inspect = _.partialRight(util.inspect, {depth: 10});


/**
 * Bundles S3 URIs into manifest files, uploads them once enough have been accumulated (or a certain amount of time
 * has passed) and emits a 'manifest' event with Manifest objects.
 *
 * Manifest URIs will be of the form s3://[bucket]/[prefix][timestamp]-[5 random letters].json
 *
 * Where [prefix] can be anything that's kosher for an S3 key.
 *
 * @param {Object} s3 An initialized AWS S3 object. The params passed to its constructor <i>must</i> include
 * Bucket and ACL.
 * @param {Number} options.minToUpload minimum amount of files to include in each manifest
 * @param {Number} options.maxWaitTime consider manifest finished after maxWaitTime milliseconds, even if the number
 * of files is smaller than options.minToUpload
 * @param {Boolean} options.mandatory The value of the "mandatory" property of each manifest entry. See Redshift
 * COPY documentation http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
 * @param {String} options.bucket S3 bucket to upload manifests to
 * @param {String} options.prefix Key prefix to use for manifests
 * @constructor
 * @type {Uploader}
 */
var Uploader = exports.Uploader = function Uploader(s3, options) {
  EventEmitter.call(this);

  if(!s3) {
    throw new Error("s3 required");
  }

  if(!options) {
    throw new Error("No options provided");
  }

  if(!options.minToUpload) {
    throw new Error("options.minToUpload required");
  }

  if(!options.maxWaitTime) {
    throw new Error("options.minToUpload required");
  }

  if(!options.bucket) {
    throw new Error("no bucket specified");
  }

  if(!options.prefix) {
    throw new Error("no key prefix specified");
  }

  if(_.isUndefined(options.mandatory)) {
    throw new Error("options.mandatory required");
  }

  this._mandatory = options.mandatory;
  this._minToUpload = options.minToUpload;
  this._maxWaitTime = options.maxWaitTime;
  this._bucket = options.bucket;
  this._prefix = options.prefix;
  this._s3 = s3;

  this._currentManifest = this._newManifest();
};

util.inherits(Uploader, EventEmitter);

Uploader.prototype.start = function start() {
  this._interval = setInterval(this._uploadCurrent.bind(this), this._maxWaitTime);
  this._interval.unref();
};

Uploader.prototype._newManifest = function _newManifest() {
  return new Manifest(this._mandatory, this._bucket, this._prefix, this._s3);
};

/**
 * Will upload the current manifest file to S3 if it's not empty. Will emit successfully uploaded manifests with
 * event type 'manifest', and errors with type 'error'.
 * @private
 */
Uploader.prototype._uploadCurrent = function _uploadCurrent() {
  if(this._currentManifest.length == 0) {
    return;
  }

  var self = this;

  this._currentManifest._upload()
    .then(function(mf) {
      debug("Upload of " + mf.manifestURI + " done");
      self.emit('manifest', mf);
    })
    .catch(function (err) {
      error("Error uploading manifest with URI " + self._currentManifest.manifestURI + ": " + err);
      self.emit('error', err);
    })

};

/**
 * A Manifest contains S3 URIs and can be turned into a manifest file for Redshift's COPY
 * @constructor
 * @param {Boolean} mandatory if true, all URIs will have mandatory:true in the manifest
 * @param {Object} s3 Initialized S3 client
 * @param {String} bucket Bucket to upload to
 * @param {String} prefix Key prefix to use
 * @type {Manifest}
 */
var Manifest = exports.Manifest = function Manifest(mandatory, bucket, prefix, s3) {
  this.mandatory = !!mandatory;
  this._uris = [];
  this._msgs = [];
  this._s3 = s3;
  this._bucket = bucket;
  this._key = prefix + Date.now().toString() + '-' + ut.randomString(5) + ".json";

  // this is the S3 URI of this manifest
  this.manifestURI = "s3://" + this._bucket + "/" + this._key
};

/**
 * Uploads the Manifest to S3, returns a promise that will be fulfilled with either the Manifest instance once the
 * upload completes, or rejected with whatever error S3 gave.
 *
 * @return {Promise} Promise of Manifest or S3 error
 * @private
 */
Manifest.prototype._upload = function _upload() {
  var self = this;
  return new Promise(function (resolve, reject) {
    var req = self._s3.putObject({ContentType: "application/json",
      Bucket: self._bucket, Key: self._key, Body: JSON.stringify(self.toJSON())});

    req.on('success', function() {
      resolve(self);
    });
    req.on('error', reject);
    req.send();
  });
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
    var req = self._s3.deleteObject({Bucket: self._bucket, Key: self._key});
    req.on('success', function() {
      resolve(self.manifestURI);
    });
    req.on('error', reject);
    req.send();
  });
};

/**
 * Number of URIs in the Manifest
 */
Object.defineProperty(Manifest.prototype, 'length', {
  get: function() {return this._uris.length}
  , set: function(n) {this._uris.length = n}
});

/**
 * Array of SQS messages in the Manifest
 */
Object.defineProperty(Manifest.prototype, 'msgs', {
  get: function() {return _.clone(this._msgs)}
});

/**
 * Array of URIs in the Manifest
 */
Object.defineProperty(Manifest.prototype, 'uris', {
  get: function() {return _.clone(this._uris)}
});

/**
 * Turn a Manifest into Redshift COPY manifest JSON
 * @return {{entries: *}}
 */
Manifest.prototype.toJSON = function toJSON() {
  var self = this;
  return {entries: _.reduce(this._uris, function (acc, uri) {
    return acc.concat({url: uri, mandatory: self.mandatory});
  }, [])};
};

/**
 * Adds SQS messages containing S3 events to the Manifest
 * @param {Array} msgs Messages to add
 * @return {Number} new amount of URIs in the Manifest
 * @private
 */
Manifest.prototype._addAll = function _addAll(msgs) {
  this._msgs = this._msgs.concat(msgs);
  this._uris = this._uris.concat(_toUris(msgs));
  return this._uris.length;
};

/**
 * Add SQS message to the Manifest
 * @param {String} msg S3 URI to add
 * @return {Number} new amount of URIs in the Manifest
 * @private
 */
Manifest.prototype._push = function _push(msg) {
  this._msgs.push(msg);
  var items = _toUris([msg]);
  debug("pushing URIs " + items);
  this._uris = this._uris.concat(items);
  return this._uris.length;
};

/**
 * Takes an S3 event and returns an array of S3 URIs contained in the Records field.
 * @param {Object} evt S3 event
 * @return {Array} S3 URI strings
 */
var _eventToS3URIs = exports._eventToS3URIs = function _eventToS3URIs(evt) {
  if(!evt) {
    return [];
  }

  if(evt.Type === "Notification") {
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
    error('Error parsing message "' + msg +'": '+ e);
    return null;
  }
}

var _toUris = exports._toUris = _.flow(_.compact, ut.splat(_.flow(tryParse, _eventToS3URIs)), _.flatten);