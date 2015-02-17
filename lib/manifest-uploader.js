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


var Manifest = exports.Manifest = function Manifest(minAmount, mandatory) {
  this.minAmount = minAmount;
  this.mandatory = !!mandatory;
  this._uris = [];
};

Object.defineProperty(Manifest.prototype, 'length', {
  get: function() {return this._uris.length}
  , set: function(n) {this._uris.length = n}
});

Object.defineProperty(Manifest.prototype, 'uris', {
  get: function() {return _.clone(this._uris)}
});

Manifest.prototype.toJSON = function toJSON() {
  var self = this;
  return {entries: _.reduce(this._uris, function (acc, uri) {
    return acc.concat({url: uri, mandatory: self.mandatory});
  }, [])};
};

Manifest.prototype.addAll = function addAll(uris) {
  this._uris = this._uris.concat(uris);
  return uris.length;
};

Manifest.prototype.push = function push(uri) {
  return this._uris.push(uri);
};