/**
 * Created by teklof on 17.2.15.
 */
var ut = require('./utils');
var EventEmitter = require('events').EventEmitter;
var stream = require('stream');
var _ = require('lodash');
var util = require('util');
var Promise = require('bluebird');
var debug = require('debug')('manifest');
var error = require('debug')('manifest:error');
var $ = require('highland');
error.log = console.error;

var inspect = _.partialRight(util.inspect, {depth: 10});


/**
 * A transform stream that reads S3 URIs and outputs Manifest objects.
 * @param {Object} s3 An initialized AWS S3 object. The params passed to its constructor <i>must</i> include
 * Bucket and ACL.
 * @param {Number} options.minToUpload minimum amount of files to include in each manifest
 * @param {Number} options.maxWaitTime consider manifest finished after maxWaitTime milliseconds, even if number
 * of files is smaller than options.minToUpload
 * @param {Boolean} options.mandatory Whether to add mandatory:true to all manifest entries.
 * @constructor
 * @type {Uploader}
 */
var Uploader = exports.Uploader = function Uploader(s3, options) {
  stream.Transform.call(this);

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

  if(_.isUndefined(options.mandatory)) {
    throw new Error("options.mandatory required");
  }

  this._mandatory = options.mandatory;
  this._minToUpload = options.minToUpload;
  this._maxWaitTime = options.maxWaitTime;
  this._s3 = s3;

  this._writableState.objectMode = false;
  this._readableState.objectMode = true;

  this._currentManifest = new Manifest(this._mandatory, this._s3);

};

Uploader.prototype._uploadCurrent = function _uploadCurrent() {

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
  // ContentType: application/json

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
 * Adds S3 URIs to the Manifest
 * @param {Array} uris S3 URIs to add
 * @return {Number} new amount of URIs in the Manifest
 * @private
 */
Manifest.prototype._addAll = function _addAll(uris) {
  this._uris = this._uris.concat(uris);
  return this._uris.length;
};

/**
 * Add S3 URI to the Manifest
 * @param {String} uri S3 URI to add
 * @return {Number} new amount of URIs in the Manifest
 * @private
 */
Manifest.prototype._push = function _push(uri) {
  return this._uris.push(uri);
};