#!/usr/bin/env node

/**
 * Created by teklof on 27.1.15.
 */

//var p = require('./lib/sqs-poller');
var _ = require('lodash');
var aws = require('aws-sdk');
var config = require('config');
var pg = require('pg');
var Promise = require('bluebird');
var util = require('util');

var s3rs = require('./lib/s3-to-rs');
var rest = require('./lib/rest');
var ut = require('./lib/utils');
var S3Copier = s3rs.S3Copier;

var inspect = _.partialRight(util.inspect, {depth: 10});

var logOpts = config.has("logging") ? config.get("logging") : {};
var logger = require("./lib/logging").initLogging(logOpts).getLogger({module: "app"});

Promise.promisifyAll(pg);

pg.defaults.poolSize = 3;
pg.defaults.poolIdleTimeout = 120;



if(!process.env.NODE_ENV) { // default to development
  process.env.NODE_ENV = "development";
}

function creds(keyId, key) {
  if(!(keyId || key)) {
    throw new Error("Missing access key ID or key");
  }
  return "aws_access_key_id=" + keyId + ";aws_secret_access_key=" + key;
}

logger.info({NODE_ENV: process.env.NODE_ENV}, "Started");

var credentials = aws.config.credentials;

if(!credentials) {
  logger.error("No AWS credentials found?");
  process.exit(1);
}


var copierOpts = config.get("S3Copier");

var s3 = new aws.S3(config.get("S3Copier.S3"));


var statsdOpts = config.has("statsd") ? config.get("statsd") : {prefix: "s4qs."};
var zabbixOpts = config.has("zabbix") ? config.get("zabbix") : null;

var rs = new aws.Redshift(config.get("S3Copier.Redshift"));

if(!_.get(copierOpts, "copyParams.withParams")) _.set(copierOpts, "copyParams.withParams", {});

copierOpts.copyParams.withParams.CREDENTIALS = creds(credentials.accessKeyId, credentials.secretAccessKey);
copierOpts.statsd = statsdOpts;
copierOpts.zabbix = zabbixOpts;
copierOpts.logger = logger;

var s3c = new S3Copier(Promise.promisifyAll(pg), s3, rs, copierOpts);


function cleanup(sig) {
  return () => {
    logger.info({signal: sig}, "Exiting. This may take a while.");
    s3c.stop().done(() => {
      logger.info("Cleanup done");
      process.exit(0);
    });
  };
}

_.each(['SIGTERM', 'SIGINT'], sig => process.on(sig, cleanup(sig)));

s3c.start();


process.on("message", (msg) => {
  if(process.connected) {
    logger.debug({msg: msg}, "Got ping");
    process.send({pong: msg.ping});
  }
});

process.on("unhandledRejection", (reason, promise) => {
  logger.fatal({err: reason}, "Exiting due to possibly unhandled rejection");
  process.exit(1);
});

s3c.errorStream.fork()
  .errors(function (err, push) {
    push(null, err);
  })
  .each(function (err) {
    logger.fatal({err: err}, "Got an error we can't recover from");
    process.exit(1);
  });

