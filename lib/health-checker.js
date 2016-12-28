/**
 * Created by teklof on 1.4.16.
 */
const co = require('co');
const Etcd = require("node-etcd");
const _ = require('lodash');
const Promise = require('bluebird');
const util = require('util');

const HealthChecker = exports.HealthChecker = function HealthChecker(options) {
  this._etcd = Promise.promisifyAll(new Etcd(options.etcdServers));
  this._etcdKey = options.key;
  this._checkerFn = _.matches(options.matchAgainst);
  this.healthOk = false;
  this._watcher = null;
  this._logger = options.logger.child({module: "HealthChecker"})
};

HealthChecker.prototype._startWatcher = function(idx) {
  this._logger.debug("starting watcher");
  const w = this._etcd.watcher(this._etcdKey, idx);
  this._watcher = w;
  w.on("error", e => {
    this._logger.warn({err: e}, "restarting watcher due to watcher error");
    this.stop();
    this.start();
  });

  w.on("change", e => {
    const action = e.action;
    const value = e.node.value;
    if(value && this._checkerFn(value)) {
      if(!this.healthOk) {
        this.healthOk = true;
        this._logger.info("pipeline health now good");
      }
    } else if(this.healthOk) {
      this._logger.error({action: action, nodeValue: value}, "pipeline health went bad");
      this.healthOk = false;
    }
  });

};

HealthChecker.prototype._checkStatus = co.wrap(function* _checkStatus() {
  let index = null;

  try {
    const res = yield this._etcd.getAsync(this._etcdKey);
    if(res) {
      const node = res[0].node;
      this.healthOk = this._checkerFn(node.value);
      index = node.modifiedIndex;
    } else {
      this._logger.error({res: res}, "get for key returned an unexpected falsy value");
      this.healthOk = false;
    }
  } catch(e) {
    // code 100 is key not found. Anything else is an actual error
    if (e.errorCode == 100) {
      this._logger.error({etcdKey: this._etcdKey}, "couldn't find etcd key");
    } else {
      this._logger.error({err: e, errCode: e.errCode}, "etcd returned an error");
    }
    this.healthOk = false;
  }

  return index;
});

HealthChecker.prototype.start = co.wrap(function* start() {
  this._logger.info("starting health checker");
  let idx = yield this._checkStatus();
  this._logger.info({healthOk: this.healthOk}, "initial health check done");

  if(idx !== null) {
    // need to start watching for changes _after_ the last index we saw, if there was one
    idx++;
  }

  this._startWatcher(idx);

  return this.healthOk;
});

HealthChecker.prototype.stop = function() {
  if(this._watcher) {
    this._logger.info("stopping");
    this._watcher.removeAllListeners();
    this._watcher.stop();
  }
};