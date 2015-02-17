/**
 * Created by teklof on 17.2.15.
 */
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
 * @param {String} key Key to use
 * @type {Manifest}
 */
var Manifest = exports.Manifest = function Manifest(mandatory, bucket, key, s3) {
  this.mandatory = !!mandatory;
  this._uris = [];
  this._s3 = s3;
  this._bucket = bucket;
  this._key = key;

  this.manifestURI = "s3://" + this._bucket + "/" + this._key; // this is the S3 URI of this manifest
};

Manifest.prototype._upload = function _upload() {
  return Promise.reject(new Error("not done yet"));
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

Object.defineProperty(Manifest.prototype, 'manifestURI', {
  get: function() {return this._manifestURI}
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
 */
Manifest.prototype.addAll = function addAll(uris) {
  this._uris = this._uris.concat(uris);
  return this._uris.length;
};

/**
 * Add S3 URI to the Manifest
 * @param {String} uri S3 URI to add
 * @return {Number} new amount of URIs in the Manifest
 */
Manifest.prototype.push = function push(uri) {
  return this._uris.push(uri);
};