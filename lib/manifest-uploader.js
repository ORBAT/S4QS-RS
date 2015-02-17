/**
 * Created by teklof on 17.2.15.
 */
var _ = require('lodash');
var util = require('util');
var Promise = require('bluebird');
var debug = require('debug')('manifest');
var error = require('debug')('manifest:error');
error.log = console.error;

var inspect = _.partialRight(util.inspect, {depth: 10});


var Uploader = exports.Uploader = function Uploader(s3, options) {

};

/**
 * A Manifest contains S3 URIs and can be turned into a manifest file for Redshift's COPY
 * @constructor
 * @param {Boolean} mandatory if true, all URIs will have mandatory:true in the manifest
 * @type {Manifest}
 */
var Manifest = exports.Manifest = function Manifest(mandatory) {
  this.mandatory = !!mandatory;
  this._uris = [];
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