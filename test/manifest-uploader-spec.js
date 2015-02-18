/**
 * Created by teklof on 17.2.15.
 */


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

  var manifBucket = "bukkit"
    , manifPrefix = "my-prefix/"
  ;

  function newSQSMsg(n) {
    return new tu.SQSMessage(n, "other-bukkit", "other-prefix/");
  }

  function newManifest(mandatory, n, put, del) {
    var s3 = new tu.FakeS3(put, del)
      , manifest = new mup.Manifest(!!mandatory, manifBucket, manifPrefix, s3)
      ;

    n = n || 0;

    manifest._addAll(newSQSMsg(n).Messages);

    return  manifest;
  }

  describe("Uploader", function () {
    function newUploader(minUp, mwt, put, del) {
      return new mup.Uploader(new tu.FakeS3(put, del), {mandatory:true, minToUpload: minUp, maxWaitTime: mwt,
        bucket: manifBucket, prefix: manifPrefix});
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

    describe("addMessage", function () {

      it("should call _uploadCurrent once URI count has been reached", function () {
        var up = newUploader(20,1)
          , _uploadCurrent = this.sinon.stub(up, "_uploadCurrent")
          ;
        up.addMessages(newSQSMsg(20).Messages);
        expect(_uploadCurrent).to.have.been.calledOnce;
      });

      it("should add messages to current manifest", function () {
        var up = newUploader(20,1)
          ;
        up.addMessages(newSQSMsg(10).Messages);
        expect(up._currentManifest.length).to.equal(10);
      });
    });

    describe("_uploadCurrent", function() {

      it("should change _currentManifest immediately", function () {
        var up = newUploader(20,1)
          , firstManif = up._currentManifest
          , upload = this.sinon.stub(firstManif, "_upload").returns(Promise.resolve(firstManif))
          ;

        up.addMessages(newSQSMsg(10).Messages);

        expect(firstManif).to.have.length(10);

        up._uploadCurrent();

        expect(up._currentManifest).to.not.deep.equal(firstManif);
      });

      it("should emit 'manifest' on successful upload", function (done) {
        var up = newUploader(20,1)
          , manif = up._currentManifest
          , upload = this.sinon.stub(manif, "_upload").returns(Promise.resolve(manif))
          ;

        up.addMessages(newSQSMsg(10).Messages);
        up.on('manifest', function (mf) {
          expect(mf).to.deep.equal(manif);
          expect(upload).to.have.been.calledOnce;
          done();
        });

        up._uploadCurrent();
      });

      it("should emit 'error' on failed upload", function (done) {
        var up = newUploader(20,1)
          , manif = up._currentManifest
          , error = new Error("not today you don't")
          , upload = this.sinon.stub(manif, "_upload").returns(Promise.reject(error))
          ;
        up.addMessages(newSQSMsg(10).Messages);
        up.on('error', function (err) {
          expect(err).to.deep.equal(error);
          expect(upload).to.have.been.calledOnce;
          done();
        });

        up._uploadCurrent();
      });

    })
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
        var m = newManifest(true, 1, {eventName: "error", content: new Error("eurgh")}, null)
          ;
        return expect(m._upload()).to.be.rejectedWith("eurgh");
      });

      it("should call S3's putObject", function () {
        var m = newManifest(true, 1, {eventName: "success", content: "yay"}, null)
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
        var m = newManifest(true, 1, {eventName: "success", content: "yay"}, null)
          ;

        return expect(m._upload()).to.be.fulfilled.and.eventually.deep.equal(m);
      });
    });

    describe("deleteManifest", function () {

      it("should return a rejected promise if deletion fails", function () {
        var m = newManifest(true, 1, null, {eventName: "error", content: new Error("eurgh")})
          ;
        return expect(m.delete()).to.be.rejectedWith("eurgh");
      });

      it("should return a promise of the S3 URI of the deleted manifest", function () {
        var m = newManifest(true, 1, null, {eventName: "success", content: "yay"})
          ;

        return expect(m.delete()).to.be.fulfilled.and.eventually.equal(m.manifestURI);
      });

      it("should call S3's deleteObject", function () {
        var m = newManifest(true, 1, null, {eventName: "success", content: "yay"})
          , deleteObject = this.sinon.spy(m._s3, "deleteObject")
        ;

        return expect(m.delete()).to.be.fulfilled.then(function () {
          expect(deleteObject).to.have.been.calledOnce;
          expect(deleteObject).to.have.been.calledWithMatch({Bucket: manifBucket, Key: m._key});
        });
      });
    });

    it("should return correct length", function () {
      var m = new mup.Manifest(true)
        , msgs = newSQSMsg(2).Messages
        ;
      m._push(msgs[0]);
      m._push(msgs[1]);
      expect(m.length).to.equal(2);
    });

    it("should _push new SQS messages", function () {
      var m = new mup.Manifest(true)
        , msg = newSQSMsg(1).Messages[0]
        ;

      expect(m._push(msg)).to.equal(1);
      expect(m.uris).to.deep.equal(msg._s3Event.s3URIs());
    });

    it("should return an array of SQS messages", function () {
      var m = new mup.Manifest(true)
        , msgs = newSQSMsg(5).Messages
      ;
      m._addAll(msgs);

      expect(m.msgs).to.deep.equal(msgs);
    });

    it("should add an array of SQS messages", function () {
      var m = new mup.Manifest(true)
        , msgs = newSQSMsg(5).Messages
        ;
      m._push(msgs[0]);
      expect(m._addAll(msgs.slice(1))).to.equal(5);

      expect(m.uris).to.deep.equal(mup._toUris(msgs));
    });

    it("should generate correct-looking JSON", function () {
      var m = new mup.Manifest(true)
        , msgs = newSQSMsg(2).Messages
      ;
      m._addAll(msgs);
      expect(m.toJSON()).to.deep.equal({entries: [{url: msgs[0]._s3Event.s3URIs()[0], mandatory: true}, {url: msgs[1]._s3Event.s3URIs()[0], mandatory: true}]})
    });

  });

  describe("toUris", function () {
    it("Should turn SQS messages to URIs", function () {
      var msgs = newSQSMsg(2).Messages.concat([null, ""])
        , uris = mup._toUris(msgs)
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