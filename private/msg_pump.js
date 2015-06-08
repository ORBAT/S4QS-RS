/*
  This script reads messages from one SQS queue, sends them to another queue and then deletes them from the original.
  The first command line argument must be the region, the second the source queue URL and the third the destination
  queue URL.

 node private/msg_pump.js us-east-1 https://sqs.us-east-1.amazonaws.com/12398543789/from-this-queue \
 https://sqs.us-east-1.amazonaws.com/12398543789/to-this-queue
 */

var Promise = require('bluebird');
var util = require('util');
var aws = require('aws-sdk');
var _ = require('lodash');
var $ = require('highland');

var inspect = _.partialRight(util.inspect, {depth: 10});

var argv = process.argv
  , region = argv[2]
  , from = argv[3]
  , to = argv[4];

console.error("region", region, "from", from, "to", to);

var defaultParams = {WaitTimeSeconds: 20, MaxNumberOfMessages: 10, VisibilityTimeout: 200};

var sqsFrom = new aws.SQS({region: region, params: _.defaults({QueueUrl: from}, defaultParams)});

var sqsTo = new aws.SQS({region: region, params: _.defaults({QueueUrl: to}, defaultParams)});

var noMsgCount = 0
  , maxNoMsg = 3;

var msgStream = $(function (push, next) {
  var req = sqsFrom.receiveMessage();
  req.on("error", function (err) {
    push(err);
    next();
  });

  req.on("success", function (resp) {
    if (resp && resp.data) {
      var msgs = resp.data.Messages;
      if (msgs && msgs.length) {
        console.error("Received", msgs.length, "messages");
        _.each(msgs, _.partial(push, null, _));
        next();
      } else {
        console.error("no messages received");
        if(++noMsgCount >= maxNoMsg) {
          console.error("noMsgCount >= maxNoMsg", noMsgCount, maxNoMsg);
          push(null, $.nil);
        } else {
          next();
        }
      }
    } else {
      console.error("no data?");
      next();
    }

  });

  req.send();
});


function sendMsgs(msgs) {
  return $(new Promise(function (resolve, reject) {
    var msgsOut = _.map(msgs, function (msg, n) {
      return {
        Id: n.toString(), MessageBody: msg.Body
      };
    });

    var req = sqsTo.sendMessageBatch({Entries: msgsOut});

    console.error("sending", msgs.length);

    req.send(function (err) {
      if(err) {
        reject(err);
        return;
      }

      console.error("sent", msgsOut.length, "messages");
      resolve(_.pluck(msgs, "ReceiptHandle"));
    });
  }));
}


function deleteMsgs(handles) {

  if(_.contains(argv, "--nodel")) {
    return $(Promise.resolve());
  }

  return $(new Promise(function (resolve, reject) {
    var entries = _.map(handles, function (hndl, n) {
      return {
        Id: n.toString(), ReceiptHandle: hndl
      }
    });

    var req = sqsFrom.deleteMessageBatch({Entries: entries});

    console.error("deleting", handles.length);

    req.send(function (err, res) {
      if(err) {
        reject(err);
        return;
      }
      console.error("deleted", handles.length);
      resolve(res.data)
    });

    console.error("delete send called");
  }));
}


msgStream
  .on("end", function() {
    console.error("stream end");
  })
  .batch(10)
  .map(sendMsgs)
  .parallel(5)
  .map(deleteMsgs)
  .parallel(5)
  .each(function () {
    console.error("each");
  });
