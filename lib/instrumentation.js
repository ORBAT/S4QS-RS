/**
 * Created by teklof on 7.9.15.
 */
var dbg = require('debug');
var _enabledOrig = dbg.enabled; // NOTE: temporarily force debug logging on
dbg.enabled = function(ns) {
  if(/s4qs/.test(ns)) return true; else return _enabledOrig(ns);
};
var debug = dbg('s4qs-rs:instr');
var error = dbg('s4qs-rs:instr:error');

var _ = require('lodash');


function Instrumenter(client, prefix) {
  var cl;

  if(client instanceof Instrumenter) {
    cl = client.statsd.getChildClient(prefix)
  } else {
    cl = client;
  }

  this.statsd = cl;
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
    this.statsd[type](fullName, val);
  }.bind(this);
};

Instrumenter.prototype.instrumenterFn = function instrumenterFn(type, name) {
  return function(val) {
    //debug(type + " " + this.statsd.options.prefix + name + ": " + val);
    this.statsd[type](name, val);
  }.bind(this);
};

Instrumenter.prototype.instrumentCalls = function instrumentCalls(name, fn, opts) {
  var start = this.timerStarter(name + ".time");
  var stop = this.timerStopper(name + ".time").bind(this);
  var incr;
  var _opts = opts || {};
  if(_opts.count) {
    incr = this.instrumenterFn("increment", name + ".count");
  }

  return function() {
    start();
    incr && incr();
    return fn.apply(this, arguments).tap(stop);
  };
};

Instrumenter.prototype.timerStarter = function timerStarter(name) {
  return function(x) {
    this._timers[name] = new Date();
    debug("Starting timer " + this.statsd.options.prefix + name);
    return x;
  }.bind(this);
};

Instrumenter.prototype.timerStopper = function timerStopper(name) {
  return function(x) {
    var duration = Date.now() - this._timers[name];
    debug("Timer " + this.statsd.options.prefix + name + " stopped: " + duration + "ms");
    this.statsd.timing(name, duration);
    return x;
  }.bind(this);
};

exports.Instrumenter = Instrumenter;

