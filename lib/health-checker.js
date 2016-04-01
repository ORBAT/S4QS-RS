/**
 * Created by teklof on 1.4.16.
 */
var co = require('co');
var Etcd = require("node-etcd");
var _ = require('lodash');
var Promise = require('bluebird');
var util = require('util');
var inspect = _.partialRight(util.inspect, {depth: 2});

var dbg = require('debug');
var _enabledOrig = dbg.enabled;
dbg.enabled = function(ns) {
  if(/s4qs/.test(ns)) return true; else return _enabledOrig(ns);
};

var debug = dbg('s4qs-rs:healthchecker');
var error = dbg('s4qs-rs:healthchecker:error');


var HealthChecker = exports.HealthChecker = function HealthChecker(options) {
  this._etcd = Promise.promisifyAll(new Etcd(options.etcdServers));
  this._etcdKey = options.key;
  this._checkerFn = _.matches(options.matchAgainst);
  this.healthOk = false;
  this._watcher = null;
};

HealthChecker.prototype._startWatcher = function(idx) {
  debug(`starting watcher`);
  var w = this._etcd.watcher(this._etcdKey, idx);
  this._watcher = w;
  w.on("error", e => {
    error(`restarting watcher due to watcher error ${inspect(e)}`);
    this.stop();
    this.start();
  });

  w.on("change", e => {
    var action = e.action;
    var value = e.node.value;
    if(value && this._checkerFn(value)) {
      if(!this.healthOk) {
        this.healthOk = true;
        debug("pipeline health now good");
      }
    } else if(this.healthOk) {
      error(`pipeline health went bad. etcd action was "${action}", current node value is "${value}"`);
      this.healthOk = false;
    }
  });

};

HealthChecker.prototype._checkStatus = co.wrap(function* _checkStatus() {
  var index = null;

  try {
    var res = yield this._etcd.getAsync(this._etcdKey);
    if(res) {
      var node = res[0].node;
      this.healthOk = this._checkerFn(node.value);
      index = node.modifiedIndex;
    } else {
      error(`get for key ${this._etcdKey} returned a falsy value "${res}"`);
      this.healthOk = false;
    }
  } catch(e) {
    // code 100 is key not found. Anything else is an actual error
    if (e.errorCode == 100) {
      error(`couldn't find etcd key ${this._etcdKey}`);
    } else {
      error(`etcd returned an error, code ${e.errorCode}: ${inspect(e)}`);
    }
    this.healthOk = false;
  }

  return index;
});

HealthChecker.prototype.start = co.wrap(function* start() {
  debug("starting");
  var idx = yield this._checkStatus();
  debug(`initial health check status: ${this.healthOk}, index ${idx}`);

  if(idx !== null) {
    // need to start watching for changes _after_ the last index we saw, if there was one
    idx++;
  }

  this._startWatcher(idx);

  return this.healthOk;
});

HealthChecker.prototype.stop = function() {
  if(this._watcher) {
    debug("stopping watcher");
    this._watcher.removeAllListeners();
    this._watcher.stop();
  }
};