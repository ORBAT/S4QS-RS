/**
 * Created by teklof on 17.2.15.
 */


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
    , grouper = s3u._tableStrToNamer("/s3:\/\/.*?\/(.*?)\//i")
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
      return new mup.Uploader(new tu.FakeS3(put, del), {mandatory:true, minToUpload: minUp, maxWaitTime: mwt,
        bucket: manifBucket, prefix: manifPrefix, grouper: grouper});
    }

    it("should periodically upload manifests even if minToUpload hasn't been reached", function () {
      clock = this.sinon.useFakeTimers();
      var up = newUploader(20,1000)
        , _uploadCurrent = this.sinon.stub(up, "_uploadCurrent")
        ;
      up.start();
      up.addMessages(newSQSMsg(10).Messages);
      clock.tick(1000);
      expect(_uploadCurrent).to.have.been.calledOnce;
    });

    describe("_uploadCurrent", function () {
      it("should upload all current non-empty manifests", function (done) {
        var up = newUploader(20,1, {eventName: "success", content: {data: "dsead"}})
          , t2msgs = newSQSMsg(10, "table2/").Messages
          , t3msgs = newSQSMsg(10, "table3/").Messages
          , msgs = t2msgs.concat(t3msgs)
          ;

        up.addMessages(msgs);
        up._newManifest("table1");
        this.sinon.spy(up._manifestGroups["table1"], "_upload");
        var count = 0;
        up.on('manifest', function(mf) {
          console.error("manifest");
          expect(mf.table).to.match(/table2|table3/);
          count++;
          if(count == 1) {
            expect(up._manifestGroups["table1"]._upload).to.not.have.been.called;
            done()
          }
        });

        up._uploadCurrent();
      });
    });

    describe("_uploadGroup", function () {

      it("should upload emit 'error' on failed upload", function (done) {
        var up = newUploader(20,1)
          , msgs = newSQSMsg(15, "table2/").Messages
          , error = new Error("not today you don't")
          ;

        up.addMessages(msgs);

        var manif = up._manifestGroups["table2"]
          , manifUpl = this.sinon.stub(manif, '_upload').returns(Promise.reject(error))
          ;

        up.on('error', function (err, mf) {
          expect(mf).to.deep.equal(manif);
          expect(err).to.deep.equal(error);
          done();
        });
        up._uploadGroup("table2");
      });

      it("should upload emit 'manifest' on successful upload", function (done) {
        var up = newUploader(20,1)
          , msgs = newSQSMsg(15, "table2/").Messages
          ;

        up.addMessages(msgs);

        var manif = up._manifestGroups["table2"]
          , manifUpl = this.sinon.stub(manif, '_upload').returns(Promise.resolve(manif))
          ;

        up.on('manifest', function (mf) {
          expect(mf).to.deep.equal(manif);
          done();
        });
        up._uploadGroup("table2");
      });

      it("should upload manifests", function () {
        var up = newUploader(20,1)
          , msgs = newSQSMsg(15, "table2/").Messages
          ;

        up.addMessages(msgs);

        var manif = up._manifestGroups["table2"]
          , manifUpl = this.sinon.stub(manif, '_upload').returns(Promise.resolve(manif))
          ;

        up._uploadGroup("table2");

        expect(manifUpl).to.have.been.calledOnce;
      });

      it("should create a new manifest before staring an upload", function () {
        var up = newUploader(20,1)
          , msgs = newSQSMsg(15, "table2/").Messages
          ;
        up.addMessages(msgs);

        var manif = up._manifestGroups["table2"];

        up._uploadGroup("table2");
        expect(up._manifestGroups["table2"]).to.not.deep.equal(manif);
      });
    });

    describe("addMessages", function () {

      it("should upload a group once it has the required amount of items", function () {
        var up = newUploader(20,1)
          , t1msgs = newSQSMsg(3, "table1/").Messages
          , t2msgs = newSQSMsg(25, "table2/").Messages
          , msgs = t1msgs.concat(t2msgs)
          , _uploadGroup = this.sinon.stub(up, "_uploadGroup")
          ;
        up.addMessages(msgs);
        expect(_uploadGroup).to.have.been.calledOnce;
        expect(_uploadGroup).to.have.been.calledWithMatch("table2");
      });

      it("should group messages by their corresponding table name", function () {
        var up = newUploader(20,1)
          , t1msgs = newSQSMsg(3, "table1/").Messages
          , t2msgs = newSQSMsg(3, "table2/").Messages
          , msgs = t1msgs.concat(t2msgs)
        ;
        up.addMessages(msgs);
        expect(up._manifestGroups["table1"].msgs).to.deep.equal(t1msgs);
        expect(up._manifestGroups["table2"].msgs).to.deep.equal(t2msgs);
      });
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

    describe("deleteManifest", function () {

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

      expect(m.uris).to.deep.equal(mup._messagesToURIs(msgs));
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
        , uris = mup._messagesToURIs(msgs)
        , uri1 = msgs[0]._s3Event.s3URIs()[0]
        , uri2 = msgs[1]._s3Event.s3URIs()[0]
        ;

      expect(uris).to.have.length(2);
      expect(uris[0]).to.equal(uri1);
      expect(uris[1]).to.equal(uri2);


    });
  });


  describe("_eventToS3URIs", function () {

    beforeEach(function () {
      event = {
        "Records": [
          {
            "eventVersion": "2.0",
            "eventSource": "aws:s3",
            "awsRegion": "us-east-1",
            "eventTime": "2015-01-30T14:49:11.286Z",
            "eventName": "ObjectCreated:Copy",
            "userIdentity": {
              "principalId": "AWS:QUUX"
            },
            "requestParameters": {
              "sourceIPAddress": "10.10.10.10"
            },
            "responseElements": {
              "x-amz-request-id": "ABCDEF012345",
              "x-amz-id-2": "aGVycCBkZXJw"
            },
            "s3": {
              "s3SchemaVersion": "1.0",
              "configurationId": "SNSCreationEvent",
              "bucket": {
                "name": "some-bucket-name",
                "ownerIdentity": {
                  "principalId": "QUUX"
                },
                "arn": "arn:aws:s3:::some-bucket-name"
              },
              "object": {
                "key": "someprefix/some.stuff.here/2015-01-30/some.stuff.here-p-9-2015-01-30-0044120221.txt.gz",
                "size": 2254780
              }
            }
          }
        ]
      };
    });

    it("Should return an empty array when S3 schema version doesn't match", function () {
      event.Records[0].s3.s3SchemaVersion = "3.0";
      expect(mup._eventToS3URIs(event)).to.deep.equal([]);
    });

    it("Should return an empty array when event version doesn't match", function () {
      event.Records[0].eventVersion = "3.0";
      expect(mup._eventToS3URIs(event)).to.deep.equal([]);
    });

    it("Should return an empty array when no records are found", function () {
      expect(mup._eventToS3URIs({Records: []})).to.deep.equal([]);
    });

    it("Should convert S3 events to S3 URIs", function () {
      var uris = mup._eventToS3URIs(event);
      var wanted = "s3://some-bucket-name/someprefix/some.stuff.here/2015-01-30/some.stuff.here-p-9-2015-01-30-0044120221.txt.gz";
      expect(uris[0]).to.equal(wanted);
    });
  });

});