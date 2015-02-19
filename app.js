#!/usr/bin/env node

/**
 * Created by teklof on 27.1.15.
 */

var p = require('./lib/sqs-poller');
var aws = require('aws-sdk');
var s3rs = require('./lib/s3-to-rs');
var rest = require('./lib/rest');
var S3Copier = s3rs.S3Copier;
var pg = require('pg');
var _ = require('lodash');
var config = require('config');
var debug = require('debug')('s4qs-app');
var error = require('debug')('s4qs-app:error');

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

var sqs = new aws.SQS({region: config.get('SQS.region'), params: config.get('SQS.params')});

var s3 = new aws.S3(config.get("S3Copier.S3"));

var pollerOpts = config.has('SQS.poller') ? config.get('SQS.poller') : {};

var poller = new p.Poller(sqs, pollerOpts);

var opts = config.get("S3Copier");

var credentials = aws.config.credentials;
if(!credentials) {
  error("No credentials found?");
}

opts.copyParams.withParams.CREDENTIALS = creds(credentials.accessKeyId, credentials.secretAccessKey);

var s3c = new S3Copier(poller, pg, s3, opts.copyParams, _.omit(opts, ["copyParams", "S3"]));


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