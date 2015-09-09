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

Instrumenter.prototype.instrument = function instrument(name, fn, ctx) {
  var start = this.timerStarter(name);
  var stop = this.timerStopper(name).bind(this);
  return function() {
    start();
    return fn.apply(ctx || this, arguments).tap(stop);
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

