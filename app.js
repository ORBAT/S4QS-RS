#!/usr/bin/env node

/**
 * Created by teklof on 27.1.15.
 */

var p = require('./lib/sqs-poller');
var aws = require('aws-sdk');
var s3rs = require('./lib/s3-to-rs');
var rest = require('./lib/rest');
var ut = require('./lib/utils');
var S3Copier = s3rs.S3Copier;
var Promise = require('bluebird');
var pg = require('pg');

Promise.promisifyAll(pg);

pg.defaults.poolSize = 3;
pg.defaults.poolIdleTimeout = 120;

var _ = require('lodash');
var config = require('config');
var dbg = require('debug');
var _enabledOrig = dbg.enabled; // NOTE: temporarily force debug logging on
dbg.enabled = function(ns) {
  if(/s4qs/.test(ns)) return true; else return _enabledOrig(ns);
};
var debug = dbg('s4qs-rs:s4qs-app');
var error = dbg('s4qs-rs:s4qs-app:error');

error.log = console.error;

var util = require('util');
var inspect = _.partialRight(util.inspect, {depth: 10});

if(!process.env.NODE_ENV) { // default to development
  process.env.NODE_ENV = "development";
}

function creds(keyId, key) {
  if(!(keyId || key)) {
    throw new Error("Missing access key ID or key");
  }
  return "aws_access_key_id=" + keyId + ";aws_secret_access_key=" + key;
}

debug("Started with env " + process.env.NODE_ENV);

var credentials = aws.config.credentials;

if(!credentials) {
  console.error("No credentials found?");
  process.exit(1);
}


var copierOpts = config.get("S3Copier");

var sqs = new aws.SQS({region: config.get('SQS.region'), params: config.get('SQS.params')});

var s3 = new aws.S3(config.get("S3Copier.S3"));

var pollerOpts = config.has('SQS.poller') ? config.get('SQS.poller') : {};

var tbl = copierOpts.copyParams.table;
var namerFn = _.isString(tbl) ? ut.tableStrToNamer(tbl) : tbl;
var knownNames = _.keys(config.get("S3Copier.timeSeries"));

pollerOpts.filter = ut.nameFilterFnFor(knownNames, namerFn);

var poller = new p.Poller(sqs, pollerOpts);

var rs = new aws.Redshift(config.get("S3Copier.Redshift"));

copierOpts.copyParams.withParams.CREDENTIALS = creds(credentials.accessKeyId, credentials.secretAccessKey);

var s3c = new S3Copier(poller, Promise.promisifyAll(pg), s3, rs, copierOpts.copyParams, copierOpts);


function cleanup(sig) {
  return function() {
    console.error("\nsignal", sig +". Exiting. This may take a while.");
    s3c.stop().done(function() {
      console.error("Cleanup done");
      process.exit(0);
    });
  };
}

_.each(['SIGTERM', 'SIGINT'], function(sig) {
  process.on(sig, cleanup(sig));
});

if(config.has("HTTPPort")) {
  debug("Starting HTTP server on port " + config.get('HTTPPort'));
  rest.app.listen(config.get("HTTPPort"));
}

s3c.start();