/**
 * Created by teklof on 5.2.15.
 */

var sp = require('../lib/sqs-poller');
var tu = require('./test-utils');
var _ = require('lodash');
var chai = require('chai');
var util = require('util');
var when = require('when');
require('mocha-sinon');
var expect = chai.expect;
var should = chai.should();

var ut = require('../lib/utils');

chai.use(require("chai-as-promised"));
chai.use(require('sinon-chai'));

var inspect = _.partialRight(util.inspect, {depth: 3});

describe("SQS poller", function() {

  var bucket = "TOOK_MAH_BUKKIT";
  var prefix = "dirgle/";

  function newSQSMsg(n) {
    return new tu.SQSMessage(n, bucket, prefix);
  }

  function newPoller(rcv, del) {
    return new sp.Poller(new tu.FakeSQS(rcv, del));
  }

  describe("deleteMsgs", function () {
    it("Should return an empty promise when given an empty array", function () {
      return expect(newPoller().deleteMsgs([])).to.eventually.equal(undefined);
    });

    it("Should return a rejected promise if deletion fails", function () {
      var p = newPoller(null, {event:"error", content: new Error("gler")})
        , sm = newSQSMsg(10)
        , deleteMessageBatch = this.sinon.spy(p.sqs, 'deleteMessageBatch')
        ;
      return expect(p.deleteMsgs(sm.Messages)).to.be.rejected;
    });

    it("Should delete 10 messages per deleteMessageBatch request", function () {
      var p = newPoller(null, {event:"success", content: "doesn't matter"})
        , sm = newSQSMsg(25)
        , deleteMessageBatch = this.sinon.spy(p.sqs, 'deleteMessageBatch')
        ;
      return p.deleteMsgs(sm.Messages).then(function () {
        expect(deleteMessageBatch).to.have.been.calledThrice;
        expect(_.pluck(deleteMessageBatch.getCall(0).args[0].Entries, "ReceiptHandle")).to.deep.equal(_.pluck(sm.Messages.slice(0, 10), "ReceiptHandle"));
        expect(_.pluck(deleteMessageBatch.getCall(1).args[0].Entries, "ReceiptHandle")).to.deep.equal(_.pluck(sm.Messages.slice(10, 20), "ReceiptHandle"));
        expect(_.pluck(deleteMessageBatch.getCall(2).args[0].Entries, "ReceiptHandle")).to.deep.equal(_.pluck(sm.Messages.slice(20), "ReceiptHandle"));
      });
    });

  });

  describe("poll", function () {

    it("should set pollPending", function () {
      var sm = newSQSMsg(10)
        , p = newPoller()
        ;

      this.sinon.stub(p, "_rcv").returns(when(sm.Messages));
      this.sinon.stub(p, "_markSeen");
      this.sinon.stub(p, "_dedup", when);
      p.poll();
      return expect(p.pollPending).to.be.fulfilled;
    });

    it("should not emit anything if no messages were received", function (done) {
      var p = newPoller()
        , rcvStub = this.sinon.stub(p, "_rcv")
        ;
      rcvStub.returns([]);

      this.sinon.stub(p, "_markSeen");
      this.sinon.stub(p, "_dedup", when);

      p.repeatPoll = 2;

      p.on('messages', function (msgs) {
        expect(msgs).to.deep.equal(sm.Messages);
        done(new Error("NOPE NOPE NOPE NOPE"));
      });

      p.poll();

      expect(p.pollPending).to.notify(done);
    });

    it("should emit messages in one array despite multiple fetches", function (done) {
      var sm = newSQSMsg(20)
        , p = newPoller()
        , rcvStub = this.sinon.stub(p, "_rcv")
        ;
      rcvStub.onFirstCall().returns(when(sm.Messages.slice(0, 10)));
      rcvStub.onSecondCall().returns(when(sm.Messages.slice(10)));

      this.sinon.stub(p, "_markSeen");
      this.sinon.stub(p, "_dedup", when);

      p.repeatPoll = 2;

      p.on('messages', function (msgs) {
        expect(msgs).to.deep.equal(sm.Messages);
        done();
      });

      p.poll();
    });

    it("should mark messages as seen", function () {
      var sm = newSQSMsg(20)
        , p = newPoller()
        , rcv = this.sinon.stub(p, "_rcv")
        , markSeen = this.sinon.stub(p, "_markSeen")
        ;

      rcv.onFirstCall().returns(when(sm.Messages.slice(0, 10)));
      rcv.onSecondCall().returns(when(sm.Messages.slice(10)));
      this.sinon.stub(p, "_dedup", when);

      p.repeatPoll = 2;
      p.poll();

      return p.pollPending.then(function() {
        expect(markSeen).to.have.been.calledOnce;
        expect(markSeen).to.have.been.calledWithExactly(sm.Messages);
      });
    });

    it("should deduplicate after _rcv", function () {
      var sm = newSQSMsg(20)
        , p = newPoller()
        , rcv = this.sinon.stub(p, "_rcv")
        , dedup = this.sinon.stub(p, "_dedup", when)
        ;

      rcv.onFirstCall().returns(when(sm.Messages.slice(0, 10)));
      rcv.onSecondCall().returns(when(sm.Messages.slice(10)));

      this.sinon.stub(p, "_markSeen");

      p.repeatPoll = 2;
      p.poll();
      return p.pollPending.then(function() {
        expect(dedup).to.have.been.calledOnce;
        expect(dedup).to.have.been.calledWithExactly(sm.Messages);
      });
    });

    it("should do repeat _rcv calls", function () {
      var sm = newSQSMsg(20)
        , p = newPoller()
        , rcv = this.sinon.stub(p, "_rcv")
        ;
      rcv.onFirstCall().returns(when(sm.Messages.slice(0, 10)));
      rcv.onSecondCall().returns(when(sm.Messages.slice(10)));

      this.sinon.stub(p, "_markSeen");
      this.sinon.stub(p, "_dedup", when);

      p.repeatPoll = 2;
      p.poll();
      return p.pollPending.then(function() {
        expect(rcv).to.have.been.calledTwice;
      });
    });
  });

  describe("_dedup", function () {
    it("should delete duplicate messages", function () {
      var sm = newSQSMsg(10)
        , seen = sm.Messages.slice(0, 5)
        , p = newPoller()
        , deleteMsgs = this.sinon.stub(p, "deleteMsgs").returns(when())
      ;
      _.each(_.pluck(seen, 'MessageId'), function(mid) {
        p.seenMsgs.set(mid, true);
      });



      return p._dedup(sm.Messages).then(function() {
        expect(deleteMsgs).to.have.been.calledOnce;
        expect(deleteMsgs.args[0][0]).to.deep.equal(seen);
      });
    });

    it("should return a promise of an array of messages when no duplicates are found", function () {
      var sm = newSQSMsg(10)
        , p = newPoller()
        ;
      this.sinon.stub(p, "deleteMsgs").returns(when());
      expect(p._dedup(sm.Messages)).to.eventually.deep.equal(sm.Messages);
    });

    it("should return a promise of an array of non-duplicate messages when deletion fails", function () {
      var sm = newSQSMsg(10)
        , seen = sm.Messages.slice(0, 5)
        , p = newPoller()
        ;
      _.each(_.pluck(seen, 'MessageId'), function(mid) {
        p.seenMsgs.set(mid, true);
      });
      this.sinon.stub(p, "deleteMsgs").returns(when.reject(new Error("uh oh")));

      expect(p._dedup(sm.Messages)).to.eventually.deep.equal(sm.Messages.slice(5));
    });

    it("should return a promise of an array of non-duplicate messages when deletion succeeds", function () {
      var sm = newSQSMsg(10)
        , seen = sm.Messages.slice(0, 5)
        , p = newPoller()
        ;
      _.each(_.pluck(seen, 'MessageId'), function(mid) {
        p.seenMsgs.set(mid, true);
      });
      this.sinon.stub(p, "deleteMsgs").returns(when());

      expect(p._dedup(sm.Messages)).to.eventually.deep.equal(sm.Messages.slice(5));
    });
  });

  describe("_markSeen", function () {
    it("should mark messages as seen", function () {
      var sm = newSQSMsg(10)
      , p = newPoller()
      , spy =  this.sinon.spy(p.seenMsgs, "set")
      ;
      p._markSeen(sm.Messages);
      var firstElem = ut.splat(ut.get('0'));
      expect(firstElem(spy.args)).to.deep.equal(_.pluck(sm.Messages, 'MessageId'));
    });
  });

  describe("_rcv", function () {
    it("should return a promise of messages from SQS", function() {
      var sm = newSQSMsg(10);
      var p = newPoller({event: 'success', content: {data: sm}});
      expect(p._rcv()).to.eventually.deep.equal(sm.Messages);
    });

    it("should return a promise of [] when the SQS response contains nothing", function() {
      var p = newPoller({event: 'success', content: null});
      expect(p._rcv()).to.eventually.deep.equal([]);
    });

    it("should return a promise of [] when the SQS response contains no messages", function() {
      var p = newPoller({event: 'success', content: {data: {}}});
      expect(p._rcv()).to.eventually.deep.equal([]);
    });

    it("should return a promise of [] when a request emits 'error'", function() {
      var p = newPoller({event: 'error', content: new Error("I'M DYIN' HERE")});
      p.on('error', function () {});
      expect(p._rcv()).to.eventually.deep.equal([]);
    });

    it("should make Poller emit an error when a request emits 'error'", function(done) {
      var p = newPoller({event: 'error', content: new Error("I'M DYIN' HERE")});
      p.on('error', done.bind(null,null));
      p._rcv();
    });
  });
});