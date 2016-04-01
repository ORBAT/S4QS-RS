/**
 * Created by teklof on 1.4.16.
 */
var co = require('co');
var Etcd = require("node-etcd");
var _ = require('lodash');

var util = require('util');
var inspect = _.partialRight(util.inspect, {depth: 2});

var dbg = require('debug');
var _enabledOrig = dbg.enabled;
dbg.enabled = function(ns) {
  if(/s4qs/.test(ns)) return true; else return _enabledOrig(ns);
};

var debug = dbg('s4qs-rs:health-checker');
var error = dbg('s4qs-rs:health-checker:error');


var HealthChecker = exports.HealthChecker = function HealthChecker(options) {
  this._etcd = Promise.promisifyAll(new Etcd(options.etcdServers));
  this._etcdKey = options.etcdKey;
  this._checkInterval = options.checkIntervalSeconds * 1000;
  this._checkerFn = _.matches(options.matchAgainst);
  this.statusOk = false;
  this._interval = null;
};


HealthChecker.prototype._checkStatus = co.wrap(function* _checkStatus() {
  try {
    this.statusOk = this._checkerFn(yield this._etcd.getAsync(this._etcdKey))
  } catch(e) {
    // code 100 is key not found. Anything else is an actual error
    if (e.errorCode != 100) {
      error(`couldn't find etcd key ${this._etcdKey}`);
    } else {
      error(`etcd returned an error, code ${e.errorCode}: ${inspect(e)}`);
    }
    this.statusOk = false;
  }
});

HealthChecker.prototype.start = co.wrap(function* start() {
  debug("starting");
  yield this._checkStatus();
  debug(`initial health check status: ${this.statusOk}, check interval ${this._checkInterval/1000}s`);
  this._interval = setInterval(this._checkStatus.bind(this), this._checkInterval);
  return this.statusOk;
});

HealthChecker.prototype.stop = function() {
  debug("stopping HealthChecker");
  clearInterval(this._interval);
};