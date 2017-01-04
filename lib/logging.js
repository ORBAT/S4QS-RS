/**
 * Created by teklof on 8.11.16.
 */
"use strict";
const $ = require("highland");
const _ = require("lodash");
const bunyan = require("bunyan");

let rootLogger;

const defaults = {
  name: "s4qs"
  , streams: [
    {level: "debug", stream: process.stdout}
  ]
  , serializers: {err: bunyan.stdSerializers.err}
};

function transformBunyanLevel(lvl) {
  let newLvl = Math.min(bunyan.ERROR, Math.max(bunyan.DEBUG, lvl));
  return (bunyan.nameFromLevel[newLvl] || "unknown").toUpperCase();
}

function WantedOutput(bunyanOutput) {
  this.ts = bunyanOutput.time;
  this.bunyanLevel = bunyanOutput.level;
  this.level = transformBunyanLevel(bunyanOutput.level);
  this.service = bunyanOutput.name;
  _.defaults(this, _.omit(bunyanOutput, "time", "level", "v", "hostname", "name"))
}

function confStream(opts) {
  return streamOptions[opts.stream](opts)
}

const streamOptions = {
  "stdout": (opt) => {opt.stream = process.stdout; return opt}
  , "stderr": (opt) => { opt.stream = process.stderr; return opt}
  , "massaged": (opt) => {
    const stream = $();
    stream.map(logObj => new WantedOutput(logObj)).map(row => JSON.stringify(row)+"\n").pipe(process.stdout);
    opt.stream = stream;
    opt.type = "raw";
    return opt;
  }
};

function isInited() {
  return !!rootLogger;
}

exports.initLogging = function(options) {
  const clonedOpts = _.cloneDeep(options) || {};

  if(_.isArray(clonedOpts.streams)) {
    clonedOpts.streams = clonedOpts.streams.map(confStream);
  }

  const optsWithDefaults = _.defaults(clonedOpts, defaults);

  rootLogger = bunyan.createLogger(optsWithDefaults);
  rootLogger.info({module: "logging"}, "root logger initialized");
  return exports;
};

exports.getLogger = function(fields) {
  if(!isInited()) {
    throw new Error("Logging hasn't been initialized: call initLogging first");
  }
  return rootLogger.child(fields || {});
};