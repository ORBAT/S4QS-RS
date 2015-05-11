/**
 * Created by teklof on 17.2.15.
 */


var $ = require('highland');
var s3u = require('../lib/s3-to-rs');
var ut = require('../lib/utils');
var mup = require('../lib/manifest-uploader');
var tu = require('./test-utils');
var _ = require('lodash');
var chai = require('chai');
var util = require('util');
var Promise = require('bluebird');
require('mocha-sinon');
var expect = chai.expect;
var should = chai.should();
var sinon = require('sinon');

var inspect = _.partialRight(util.inspect, {depth: 3});

function defer() {
  var resolver, rejecter;

  var p = new Promise(function (resolve, reject) {
    resolver = resolve;
    rejecter = reject;
  });

  return {reject: rejecter, resolve: resolver, promise: p};
}

describe("Manifest uploading", function () {

  var clock;

  afterEach(function () {
    if (clock) {
      clock.restore();
      clock = null;
    }
  });

  var manifBucket = "manif-bukkit"
    , manifPrefix = "manif-prefix/"
    , msgPrefix = "msg-prefix/"
    , grouper = ut.tableStrToNamer("/s3:\/\/.*?\/(.*?)\//i")
  ;

  function newSQSMsg(n, prefix) {
    return new tu.SQSMessage(n, "msg-bukkit", prefix || msgPrefix);
  }

  function newManifest(mandatory, n, put, del, table) {
    var s3 = new tu.FakeS3(put, del)
      , manifest = new mup.Manifest(s3, {mandatory:!!mandatory,
        bucket: manifBucket,
        prefix:manifPrefix,
        table:table})
      ;

    n = n || 0;

    manifest._addAll(newSQSMsg(n).Messages);

    return  manifest;
  }

  describe("Uploader", function () {
    function newUploader(minUp, mwt, put, del) {
      return new mup.Uploader(new tu.FakeS3(put, del), {mandatory: true, maxToUpload: minUp, maxWaitSeconds: mwt,
        bucket: manifBucket, prefix: manifPrefix, grouper: grouper});
    }

    it("should periodically upload manifests even if maxToUpload hasn't been reached", function (done) {
      var up = newUploader(20,0.01)
        , msgs = newSQSMsg(10, "table1/").Messages
        ;

      this.sinon.stub(mup.Manifest.prototype, "_upload", function() {
        this._resolve(this);
        return this._promise;
      });

      $(msgs).through(up.msgsToManifests.bind(up)).sequence().each(function(mf) {
        expect(mf._msgs).to.deep.equal(msgs);
        done();
      })

    });

  });


  describe("Manifest object", function () {

    it("should give itself a well-formed URI", function () {
      var m = newManifest()
        , re = new RegExp("s3://"+ manifBucket + "/"+ manifPrefix + ".*?\\.json")
        ;
      expect(m.manifestURI).to.match(re);
    });

    describe("_upload", function () {

      it("should return a rejected promise if PUT fails", function () {
        var m = newManifest(true, 1, {eventName: "error", content: new Error("eurgh")}, null, "some_table")
          ;
        return expect(m._upload()).to.be.rejectedWith("eurgh");
      });

      it("should call S3's putObject", function () {
        var m = newManifest(true, 1, {eventName: "success", content: "yay"}, null, "some_table")
          , putObject = this.sinon.spy(m._s3, "putObject")
          , json = m.toJSON()
          ;

        return expect(m._upload()).to.be.fulfilled.then(function () {
          expect(putObject).to.have.been.calledOnce;
          var arg = putObject.firstCall.args[0];
          expect(arg.Bucket).to.equal(manifBucket);
          expect(arg.Key).to.equal(m._key);
          expect(arg.ContentType).to.equal("application/json");
          expect(JSON.parse(arg.Body)).to.deep.equal(json);
        });
      });

      it("should return a promise of the Manifest", function () {
        var m = newManifest(true, 1, {eventName: "success", content: "yay"}, null, "some_table")
          ;

        return expect(m._upload()).to.be.fulfilled.and.eventually.deep.equal(m);
      });
    });

    describe("delete", function () {

      it("should return a rejected promise if deletion fails", function () {
        var m = newManifest(true, 1, null, {eventName: "error", content: new Error("eurgh")}, "some_table")
          ;
        return expect(m.delete()).to.be.rejectedWith("eurgh");
      });

      it("should return a promise of the S3 URI of the deleted manifest", function () {
        var m = newManifest(true, 1, null, {eventName: "success", content: "yay"}, "some_table")
          ;

        return expect(m.delete()).to.be.fulfilled.and.eventually.equal(m.manifestURI);
      });

      it("should call S3's deleteObject", function () {
        var m = newManifest(true, 1, null, {eventName: "success", content: "yay"}, "some_table")
          , deleteObject = this.sinon.spy(m._s3, "deleteObject")
        ;

        return expect(m.delete()).to.be.fulfilled.then(function () {
          expect(deleteObject).to.have.been.calledOnce;
          expect(deleteObject).to.have.been.calledWithMatch({Bucket: manifBucket, Key: m._key});
        });
      });
    });

    it("should return correct length", function () {
      var m = new mup.Manifest(null, {mandatory:true})
        , msgs = newSQSMsg(2).Messages
        ;
      m._push(msgs[0]);
      m._push(msgs[1]);
      expect(m.length).to.equal(2);
    });

    it("should _push new SQS messages", function () {
      var m = new mup.Manifest(null, {mandatory:true})
        , msg = newSQSMsg(1).Messages[0]
        ;

      expect(m._push(msg)).to.equal(1);
      expect(m.uris).to.deep.equal(msg._s3Event.s3URIs());
    });

    it("should return an array of SQS messages", function () {
      var m = new mup.Manifest(null, {mandatory:true})
        , msgs = newSQSMsg(5).Messages
      ;
      m._addAll(msgs);

      expect(m.msgs).to.deep.equal(msgs);
    });

    it("should add an array of SQS messages", function () {
      var m = new mup.Manifest(null, {mandatory:true})
        , msgs = newSQSMsg(5).Messages
        ;
      m._push(msgs[0]);
      expect(m._addAll(msgs.slice(1))).to.equal(5);

      expect(m.uris).to.deep.equal(ut.messagesToURIs(msgs));
    });

    it("should generate correct-looking JSON", function () {
      var m = new mup.Manifest(null, {mandatory:true})
        , msgs = newSQSMsg(2).Messages
      ;
      m._addAll(msgs);
      expect(m.toJSON()).to.deep.equal({entries: [{url: msgs[0]._s3Event.s3URIs()[0], mandatory: true}, {url: msgs[1]._s3Event.s3URIs()[0], mandatory: true}]})
    });

  });

  describe("toUris", function () {
    it("Should turn SQS messages to URIs", function () {
      var msgs = newSQSMsg(2).Messages.concat([null, ""])
        , uris = ut.messagesToURIs(msgs)
        , uri1 = msgs[0]._s3Event.s3URIs()[0]
        , uri2 = msgs[1]._s3Event.s3URIs()[0]
        ;

      expect(uris).to.have.length(2);
      expect(uris[0]).to.equal(uri1);
      expect(uris[1]).to.equal(uri2);
    });
  });
});