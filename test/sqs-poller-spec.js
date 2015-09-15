/**
 * Created by teklof on 5.2.15.
 */

var sp = require('../lib/sqs-poller');
var tu = require('./test-utils');
var _ = require('lodash');
var chai = require('chai');
var util = require('util');
var Promise = require('bluebird');
require('mocha-sinon');
var expect = chai.expect;
var should = chai.should();

var ut = require('../lib/utils');

chai.use(require("chai-as-promised"));
chai.use(require('sinon-chai'));

var inspect = _.partialRight(util.inspect, {depth: 3});

describe("SQS poller", function() {

  var bucket = "TOOK_MAH_BUKKIT";
  var prefix = "some-prefix/table.name/";
  var namerFn = ut.tableStrToNamer("/s3:\/\/.*?\/some-prefix\/(.*?)\//i");
  var filterFn = ut.nameFilterFnFor(["table_name"], namerFn);


  function newSQSMsg(n) {
    return new tu.SQSMessage(n, bucket, prefix);
  }

  function newPoller(rcv, del) {
    return new sp.Poller(new tu.FakeSQS(rcv, del), {filter: filterFn, pollIntervalSeconds: 0.02, parallelPolls: 3});
  }


  describe("_unknownMsgs", function () {

    it("should output the S3 URIs in deleted messages", function (done) {
      var badMsgs = new tu.SQSMessage(10, bucket, "dirgle/bleuhrg/").Messages
        , sm = newSQSMsg(10).Messages
        , p = newPoller()
        ;

      var deleteMsgs = this.sinon.stub(p, "deleteMsgs", function (msgs) {
        return Promise.resolve(ut.messagesToURIs(msgs));
      });

      var rcv = this.sinon.stub(p, "_rcv");

      rcv.onFirstCall().resolves(badMsgs);
      rcv.resolves(sm);

      p.start();

      var count = 0;

      p._unknownMsgs.each(function (msg) {
        expect(msg).to.deep.equal(ut.messageToURIs(badMsgs[count % badMsgs.length])[0]);
        count++;
        if (count >= 10) {
          p.stop();
          done();
        }
      });
    });

    it("should delete messages that don't pass the filter", function (done) {
      var badMsgs = new tu.SQSMessage(10, bucket, "dirgle/bleuhrg/").Messages
        , sm = newSQSMsg(10).Messages
        , p = newPoller()
        ;

      var deleteMsgs = this.sinon.stub(p, "deleteMsgs", function (msgs) {
        return Promise.resolve(ut.messagesToURIs(msgs));
      });

      var rcv = this.sinon.stub(p, "_rcv");

      rcv.onFirstCall().resolves(badMsgs);
      rcv.resolves(sm);

      p.start();

      var count = 0;

      p._unknownMsgs.each(function (msg) {
        expect(_.flattenDeep(deleteMsgs.args)).to.deep.equal(badMsgs);
        count++;
        if (count >= 10) {
          p.stop();
          done();
        }
      });
    });
  });

  describe("messageStream", function () {
    var clock;

    afterEach(function () {
      if (clock) {
        clock.restore();
        clock = null;
      }
    });

    it("should not contain messages that don't pass the filter function", function (done) {
      var badMsgs = new tu.SQSMessage(20, bucket, "dirgle/bleuhrg/").Messages
        , sm = newSQSMsg(10).Messages
        , p = newPoller()
        ;

      this.sinon.stub(p, "deleteMsgs").resolves();
      var rcv = this.sinon.stub(p, "_rcv");

      rcv.onFirstCall().resolves(badMsgs);
      rcv.resolves(sm);

      p.start();

      var count = 0;

      p.messageStream.each(function (msg) {
        expect(msg).to.deep.equal(sm[count % sm.length]);

        count++;
        if(count >= 10) {
          p.stop();
          done();
        }
      });

    });

    it("should stream messages it gets from _rcv", function (done) {
      var sm = newSQSMsg(10).Messages
        , p = newPoller()
        ;

      this.sinon.stub(p, "_rcv").resolves(sm);

      p.start();

      var count = 0;
      p.messageStream.each(function (msg) {
        expect(msg).to.deep.equal(sm[count % sm.length]);

        count++;
        if(count >= 60) {
          p.stop();
          done();
        }
      });
    });



  });

  describe("deleteMsgs", function () {
    it("Should return a promise of an empty array when given an empty array", function () {
      return expect(newPoller().deleteMsgs([])).to.eventually.deep.equal([]);
    });

    it("Should return a rejected promise if deletion fails", function () {
      var p = newPoller(null, {event:"error", content: new Error("gler")})
        , sm = newSQSMsg(10).Messages
        , deleteMessageBatch = this.sinon.spy(p.sqs, 'deleteMessageBatch')
        ;
      return expect(p.deleteMsgs(sm)).to.be.rejected;
    });

    it("Should delete 10 messages per deleteMessageBatch request", function () {
      var p = newPoller(null, {event:"success", content: "doesn't matter"})
        , sm = newSQSMsg(25).Messages
        , deleteMessageBatch = this.sinon.spy(p.sqs, 'deleteMessageBatch')
        ;
      return p.deleteMsgs(sm).then(function () {
        expect(deleteMessageBatch).to.have.been.calledThrice;
        expect(_.pluck(deleteMessageBatch.getCall(0).args[0].Entries, "ReceiptHandle")).to.deep.equal(_.pluck(sm.slice(0, 10), "ReceiptHandle"));
        expect(_.pluck(deleteMessageBatch.getCall(1).args[0].Entries, "ReceiptHandle")).to.deep.equal(_.pluck(sm.slice(10, 20), "ReceiptHandle"));
        expect(_.pluck(deleteMessageBatch.getCall(2).args[0].Entries, "ReceiptHandle")).to.deep.equal(_.pluck(sm.slice(20), "ReceiptHandle"));
      });
    });

  });

  describe("_rcv", function () {


    it("should return a promise of messages from SQS", function() {
      var sm = newSQSMsg(10);
      var p = newPoller({event: "success", content: {data: sm}});
      return expect(p._rcv()).to.eventually.deep.equal(sm.Messages);
    });

    it("should return a promise of [] when the SQS response contains nothing", function() {
      var p = newPoller({event: "success", content: null});
      return expect(p._rcv()).to.eventually.deep.equal([]);
    });

    it("should return a promise of [] when the SQS response contains no messages", function() {
      var p = newPoller({event: "success", content: {data: {}}});
      return expect(p._rcv()).to.eventually.deep.equal([]);
    });

    it("should return a promise of [] when a request emits 'error'", function() {
      var p = newPoller({event: "error", content: new Error("I'M DYIN' HERE")});
      return expect(p._rcv()).to.be.rejectedWith("I'M DYIN' HERE");
    });
  });
});