/**
 * Created by teklof on 8.11.16.
 */
"use strict";
const _ = require('lodash');
const bunyan = require("bunyan");
var rootLogger;

const defaults = {
  name: "s4qs"
  , streams: [
    {level: "debug", stream: process.stdout}
    , {level: "warn", stream: process.stderr}
  ]
  , serializers: {err: bunyan.stdSerializers.err}
};

function isInited() {
  return !!rootLogger;
}

exports.initLogging = function(options) {
  let clonedOpts = _.clone(options) || {};

  if(_.isArray(clonedOpts.streams)) {
    clonedOpts.streams = _.map(clonedOpts.streams, it => {
      switch (it.stream) {
        case "stdout":
          it.stream = process.stdout;
          break;
        case "stderr":
          it.stream = process.stderr;
          break;
      }
      return it;
    });
  }

  let optsWithDefaults = _.defaults(clonedOpts, defaults);

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