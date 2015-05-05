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
  var prefix = "dirgle/table.name/";
  var namerFn = ut.tableStrToNamer("/s3:\/\/.*?\/dirgle\/(.*?)\//i");
  var filterFn = ut.nameFilterFnFor(["table_name"], namerFn);


  function newSQSMsg(n) {
    return new tu.SQSMessage(n, bucket, prefix);
  }

  function newPoller(rcv, del) {
    return new sp.Poller(new tu.FakeSQS(rcv, del), {filter: filterFn, pollIntervalSeconds: 0.2, repeatPoll: 3});
  }

  describe("messageStream", function () {
    var clock;

    afterEach(function () {
      if (clock) {
        clock.restore();
        clock = null;
      }
    });

    it("should call _rcv multiple times", function (done) {
      clock = this.sinon.useFakeTimers(1000);
      var sm = newSQSMsg(10).Messages
        , p = newPoller()
        ;

      this.sinon.stub(p, "_rcv").resolves(sm);

      var count = 0;
      p.messageStream.each(function (msg) {
        expect(msg).to.deep.equal(sm[count % sm.length]);

        count++;
        if(count >= 30) {
          p.messageStream.end();
          done();
        }
      });
    });

    it("should stream messages it gets from _rcv", function (done) {
      clock = this.sinon.useFakeTimers(1000);
      var sm = newSQSMsg(10).Messages
        , p = newPoller()
        ;

      this.sinon.stub(p, "_rcv").resolves(sm);

      var count = 0;
      p.messageStream.each(function (msg) {
        expect(msg).to.deep.equal(sm[count % sm.length]);

        count++;
        if(count >= 60) {
          p.messageStream.end();
          done();
        }
      });
    });



  });

  describe("deleteMsgs", function () {
    it("Should return an empty promise when given an empty array", function () {
      return expect(newPoller().deleteMsgs([])).to.eventually.equal(undefined);
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

  describe("poll", function () {

    it("should set pollPending", function () {
      var sm = newSQSMsg(10).Messages
        , p = newPoller()
        ;

      this.sinon.stub(p, "_rcv").returns(Promise.resolve(sm));
      p.poll();
      return expect(p.pollPending).to.be.fulfilled;
    });

    it("should emit [] if no messages were received", function (done) {
      var p = newPoller()
        , rcvStub = this.sinon.stub(p, "_rcv")
        ;
      rcvStub.returns([]);

      p.repeatPoll = 2;

      p.on('messages', function (msgs) {
        expect(msgs).to.deep.equal([]);
        done();
      });

      p.poll();

    });

    it("should emit messages in one array despite multiple fetches", function (done) {
      var sm = newSQSMsg(20).Messages
        , p = newPoller()
        , rcvStub = this.sinon.stub(p, "_rcv")
        ;
      rcvStub.onFirstCall().returns(Promise.resolve(sm.slice(0, 10)));
      rcvStub.onSecondCall().returns(Promise.resolve(sm.slice(10)));

      p.repeatPoll = 2;

      p.on('messages', function (msgs) {
        expect(msgs).to.deep.equal(sm);
        done();
      });

      p.poll();
    });


    it("should do repeat _rcv calls", function () {
      var sm = newSQSMsg(20).Messages
        , p = newPoller()
        , rcv = this.sinon.stub(p, "_rcv")
        ;
      rcv.onFirstCall().returns(Promise.resolve(sm.slice(0, 10)));
      rcv.onSecondCall().returns(Promise.resolve(sm.slice(10)));


      p.repeatPoll = 2;
      p.poll();
      return p.pollPending.then(function() {
        expect(rcv).to.have.been.calledTwice;
      });
    });
  });

  describe("_rcv", function () {

    it("should not return messages that don't pass the filter function", function () {
      var sm = new tu.SQSMessage(5, bucket, "dirgle/bleuhrg/")
        , p = newPoller({event: "success", content: {data: sm}})
        ;
      this.sinon.stub(p, "deleteMsgs").resolves();

      return expect(p._rcv()).to.eventually.deep.equal([]);
    });

    it("should delete messages that don't pass the filter function", function () {
      var sm = new tu.SQSMessage(5, bucket, "dirgle/bleuhrg/")
        , p = newPoller({event: "success", content: {data: sm}})
        ;
      this.sinon.stub(p, "deleteMsgs").resolves();
      return p._rcv().then(function() {
        expect(p.deleteMsgs).to.have.been.calledWithMatch(sm.Messages);
      })
    });

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