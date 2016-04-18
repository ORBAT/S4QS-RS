/**
 * Created by teklof on 7.9.15.
 */

var _ = require('lodash');
var dbg = require('debug');
var inspect = _.partialRight(require("util").inspect, {depth: 2});
var _enabledOrig = dbg.enabled; // NOTE: temporarily force debug logging on
dbg.enabled = function(ns) {
  if(/s4qs/.test(ns)) return true; else return _enabledOrig(ns);
};
var debug = dbg('s4qs-rs:instr');
var error = dbg('s4qs-rs:instr:error');


function Instrumenter(client, prefix, zabbixSender) {
  var cl;

  if(client instanceof Instrumenter) {
    cl = client._statsd.getChildClient(prefix);
    if(client._zbx) {
      this._zbx = client._zbx;
    }
  } else {
    cl = client.getChildClient(prefix);
  }

  if(zabbixSender) {
    this._zbx = zabbixSender;
  }

  this._statsd = cl;
  this._timers = {};
}

Instrumenter.prototype.usingInstrFn = function (type, name, fn) {
  var instFn = this.instrumenterFn(type, name);
  return function(x) {
    instFn(fn(x));
    return x;
  };
};

Instrumenter.prototype.postfixedInstrFn = function postfixedInstrFn(type, name) {
  return function(postfix, val) {
    var fullName = name + "." + postfix;
    this._statsd[type](fullName, val);
  }.bind(this);
};

Instrumenter.prototype.instrumenterFn = function instrumenterFn(type, name) {
  return function(val) {
    this._statsd[type](name, val);
  }.bind(this);
};

Instrumenter.prototype.zabbixFn = function zabbixFn(key) {
  return (val) => {
    if(this._zbx) {
      debug(`zabbix key ${key}, value ${val}`);
      this._zbx.send({[key]: val}, (res) => {
        debug(`zabbix sender responded with ${inspect(res)}`)
      });
    }
  }
};

Instrumenter.prototype.instrumentCalls = function instrumentCalls(name, fn, opts) {
  var nameWithTime = name + ".time";
  var start = this.timerStarter(nameWithTime);
  var stop = this.timerStopper(nameWithTime);
  var incr;
  var zbx;
  var _opts = opts || {};
  if(_opts.count) {
    incr = this.instrumenterFn("increment", name + ".count");
  }

  if(_opts.zabbix) {
    zbx = this.zabbixFn(this._statsd.options.prefix + nameWithTime);
  } else {
    zbx = _.identity;
  }

  return function() {
    start();
    incr && incr();
    return fn.apply(this, arguments).tap(_.flow(stop, zbx));
  };
};

Instrumenter.prototype.timerStarter = function timerStarter(name) {
  return function(x) {
    this._timers[name] = new Date();
    return x;
  }.bind(this);
};

Instrumenter.prototype.timerStopper = function timerStopper(name) {
  return function() {
    var duration = Date.now() - this._timers[name];
    this._statsd.timing(name, duration);
    return duration;
  }.bind(this);
};

exports.Instrumenter = Instrumenter;

