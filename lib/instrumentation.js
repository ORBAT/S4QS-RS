/**
 * Created by teklof on 7.9.15.
 */

var _ = require('lodash');
var dbg = require('debug');
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
  return function(val) {
    instFn(fn(val));
    return val;
  };
};

/**
 * Like instrumenterFn, but returns a function that also takes a postfix that is added to each key at call time.
 * @param {String} type Type of instrumentation to use, e.g. 'gauge', 'increment', 'counter' (see statsd client's
 * documentation for more info)
 * @param {String} key Statsd key to use
 * @return {function(postfix:String, val:Number)} A function that sends the specified key and postfix to statsd with value val
 */
Instrumenter.prototype.postfixedInstrFn = function postfixedInstrFn(type, key) {
  return function(postfix, val) {
    var fullName = key + "." + postfix;
    this._statsd[type](fullName, val);
  }.bind(this);
};

/**
 * Returns a function that calls a specific statsd client function (so 'gauge', 'increment' etc.)
 * @param {String} type Type of instrumentation to use, e.g. 'gauge', 'increment', 'counter' (see statsd client's
 * documentation for more info)
 * @param {String} key Statsd key to use
 * @return {function(val:Number)} A function that sends the specified key to statsd with value val
 */
Instrumenter.prototype.instrumenterFn = function instrumenterFn(type, key) {
  return function(val) {
    this._statsd[type](key, val);
  }.bind(this);
};

/**
 * Returns a function that sends a value to Zabbix using the specified key
 * @param {String} key Key name to use
 * @return {function(val:Number)}
 */
Instrumenter.prototype.zabbixFn = function zabbixFn(key) {
  return (val) => {
    if(this._zbx) {
      this._zbx.send({[key]: val}, err => {err && error(`Error sending ${key} to Zabbix: ${err}. Code ${err.code}`)});
    }
  }
};

/**
 * Instruments a function by returning a closure wrapped with instrumentation calls. The returned closure can be called exactly
 * like the original function, i.e. arguments and return value are not touched.
 * Execution time is always instrumented with the key name + ".time". If opts is provided and has a truthy key
 * "count", the number of times the function is called is sent with the key name + ".count".
 * @param name
 * @param {function(*):Promise} fn Function to instrument. Should return a promise
 * @param {Boolean} opts.count If true, instrument number of times function was invoked with key name + ".count"
 * @param {Boolean} opts.zabbix If true, send execution time to Zabbix as well (when configured)
 * @return {function(*):Promise}
 */
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

