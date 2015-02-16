/**
 * Created by teklof on 30.1.15.
 */
var s3t = require('../lib/s3-to-rs');
var sqsp = require('../lib/sqs-poller');
var tu = require('./test-utils');
var _ = require('lodash');
var chai = require('chai');
var util = require('util');
var Promise = require('bluebird');
require('mocha-sinon');
var expect = chai.expect;
var should = chai.should();

chai.use(require("chai-as-promised"));
chai.use(require('sinon-chai'));

var inspect = _.partialRight(util.inspect, {depth: 10});

describe("S3 to Redshift copier", function () {

  var event;

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

  describe("S3Copier", function () {
    var s3URI = "s3://bucket/derr/some.stuff.here/fsadjlkgasjkl.csv"
      , toTbl = "some.stuff.here"
      , table = "some_stuff_here"
      , copyParams = {
        "table": "/s3:\/\/.*?\/derr\/(.*?)\//i",
        "args": [
          "GZIP",
          "TRUNCATECOLUMNS"
        ]
      };

    function newCopier(pgConnErr, pgQueryErr, pgDoneCb) {
      var fakePoller = new tu.FakePoller("derr/some.stuff.here")
        , fakePg = new tu.FakePg(pgConnErr, pgQueryErr, pgDoneCb);
      return new s3t.S3Copier(fakePoller, fakePg, copyParams, {connStr: "postgres://bler", pollIntervalS: 60});
    }

    describe("_onMsgs", function () {
      it("should set _onMsgPending to a promise that is fulfilled after the function is done", function () {
        var c = newCopier(null, null, null);
        this.sinon.stub(c, "_doDelete").returns(Promise.resolve());
        this.sinon.stub(c, "_connAndCopy", function(uri) {
          return Promise(uri);
        });
        c._onMsgs([]);
        return expect(c._onMsgPending).to.be.fulfilled;
      });

      it("should schedule a new poll after completion", function () {
        this.sinon.useFakeTimers(10000);
        var c = newCopier(null, null, null);
        this.sinon.stub(c, "_doDelete").returns(Promise.resolve());
        this.sinon.stub(c, "_schedulePoll");
        this.sinon.stub(c, "_connAndCopy", function(uri) {
          return Promise.resolve(uri);
        });

        c._onMsgs([]);
        return c._onMsgPending.then(function() {
          expect(c._schedulePoll).to.have.been.calledWithExactly(10000 + c._pollIntervalS * 1000);
        });
      });

      it("should not delete messages that failed to be copied", function () {
        var c = newCopier(null, null, null);
        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.resolve());

        // return a function that returns a rejected promise on the first call and a promise of its argument on all others
        function firstFn() {
          var first = true;
          return function(uri) {
            if(first) {
              first = false;
              return Promise.reject(new Error("yoink"));
            }
            return Promise.resolve(uri);
          };
        }

        this.sinon.stub(c, "_connAndCopy", firstFn());

        var ev = new tu.SQSMessage(10, "derr", "some.stuff.here/");
        var msgs = ev.Messages;
        c._onMsgs(msgs);
        return c._onMsgPending.then(function () {
          expect(c._poller.deleteMsgs).to.have.been.calledWithMatch(_.tail(msgs));
        });
      });

      it("should delete copied messages", function () {
        var c = newCopier(null, null, null);
        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.resolve());
        this.sinon.stub(c, "_connAndCopy", function(uri) {
          return Promise.resolve(uri);
        });

        var ev = new tu.SQSMessage(10, "derr", "some.stuff.here/");
        var msgs = ev.Messages;
        c._onMsgs(msgs);
        return c._onMsgPending.then(function () {
          expect(c._poller.deleteMsgs).to.have.been.calledWithMatch(msgs);
        });
      });

      it("should call _connAndCopy for each message", function () {
        var c = newCopier(null, null, null)
          , bukkit = "derr"
          , prefix = "some.stuff.here/"
          , re = new RegExp("s3://"+ bukkit + "/"+ prefix)
        ;

        this.sinon.stub(c, "_doDelete").returns(Promise.resolve());
        this.sinon.stub(c, "_connAndCopy", function(uri) {
          expect(uri).to.match(re);
          return Promise.resolve(uri);
        });


        var ev = new tu.SQSMessage(10, bukkit, prefix);
        var msgs = ev.Messages;
        c._onMsgs(msgs);
        return c._onMsgPending.then(function () {
          expect(c._connAndCopy).to.have.callCount(msgs.length);
          _.each(ev.s3URIs, function (uri) {
            expect(c._connAndCopy).to.have.been.calledWithExactly(uri);
          });
        });
      });
    });

    describe("_doDelete", function () {

      it("should clear the _toDelete array on successful copy", function () {
        var c = newCopier(null, null, null);
        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.resolve());
        c._toDelete = new tu.SQSMessage(10, "gler", "flor").Messages;
        return c._doDelete().then(function () {
          expect(c._toDelete).to.have.length(0);
        });
      });

      it("should not clear the _toDelete array on failed copy", function () {
        var c = newCopier(null, null, null);
        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.reject(new Error("HNNNGGGGH")));
        c._toDelete = new tu.SQSMessage(10, "gler", "flor").Messages;
        return c._doDelete().then(function () {
          expect(c._toDelete).to.have.length(10);
        });
      });

      it("should call poller.deleteMsg", function (done) {
        var c = newCopier(null, null, null);
        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.resolve());
        var msgs = new tu.SQSMessage(10, "gler", "flor").Messages;
        c._toDelete = msgs;
        return expect(c._doDelete()).to.be.fulfilled.then(function () {
          expect(c._poller.deleteMsgs).to.have.been.calledOnce.and.calledWithMatch(msgs);
        }).should.notify(done);
      });

      it("should not return a rejected promise on deletion error", function () {
        var c = newCopier(null, null, null);
        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.reject(new Error("welp")));
        c._toDelete = new tu.SQSMessage(10, "gler", "flor").Messages;
        return expect(c._doDelete()).to.be.fulfilled;
      });
    });

    describe("_connAndCopy", function () {
      it("should return a promise of the S3 URI on successful copy", function () {
        var doneCb = this.sinon.spy();
        var c = newCopier(null, null, doneCb);
        return expect(c._connAndCopy(s3URI)).to.become(s3URI);
      });

      it("should connect to pg", function (done) {
        var doneCb = this.sinon.spy();
        var c = newCopier(null, null, doneCb);
        this.sinon.spy(c._pg, "connect");
        return expect(c._connAndCopy(s3URI)).to.be.fulfilled
          .then(function () {
            expect(c._pg.connect).to.have.been.calledOnce.and.calledWith("postgres://bler");
          }).should.notify(done);
      });

      it("should do queries with a table postfix", function (done) {
        var doneCb = this.sinon.spy();
        var c = newCopier(null, null, doneCb);
        c._tablePostfix = "_pahoyhoy";
        return expect(c._connAndCopy(s3URI)).to.be.fulfilled
          .then(function () {
            expect(c._pg.client.query).to.have.been.calledOnce.and.calledWithMatch(util.format("COPY %s FROM '%s' %s;",
              table + c._tablePostfix, s3URI,
              copyParams.args.join(' ')));
          }).should.notify(done);
      });

      it("should do queries", function (done) {
        var doneCb = this.sinon.spy();
        var c = newCopier(null, null, doneCb);
        return expect(c._connAndCopy(s3URI)).to.be.fulfilled
          .then(function () {
            expect(c._pg.client.query).to.have.been.calledOnce.and.calledWithMatch(util.format("COPY %s FROM '%s' %s;",
              table, s3URI,
              copyParams.args.join(' ')));
          }).should.notify(done);
      });

      it("Should return the client to the pool when a query succeeds", function (done) {
        var doneCb = this.sinon.spy();
        var c = newCopier(null, null, doneCb);
        return expect(c._connAndCopy(s3URI)).to.be.fulfilled
          .then(function () {
            expect(doneCb).to.have.been.calledOnce.and.calledWithExactly();
          }).should.notify(done);
      });

      it("Should not return the client to the pool when a query fails", function (done) {
        var doneCb = this.sinon.spy();
        var c = newCopier(null, new Error("query error"), doneCb);
        return expect(c._connAndCopy(s3URI)).to.be.rejectedWith(Error, "query error")
          .then(function () {
            expect(doneCb).to.have.been.calledOnce.and.calledWith(c._pg.client);
          }).should.notify(done);
      });

      it("should return a rejected promise on connection errors", function () {
        var doneCb = this.sinon.spy();
        var c = newCopier(new Error("connection error"), null, doneCb);
        return expect(c._connAndCopy(s3URI)).to.be.rejectedWith(Error, "connection error");
      });

      it("should return a rejected promise on query errors", function () {
        var doneCb = this.sinon.spy();
        var c = newCopier(null, new Error("query error"), doneCb);
        return expect(c._connAndCopy(s3URI)).to.be.rejectedWith(Error, "query error");
      });
    });
  });

  describe("_tableStrToNamer", function () {

    it("Should handle strings", function () {
      var fn = s3t._tableStrToNamer("qux");
      var uri = "s3://some-bucket/someprefix/herp.derp.durr/2015-02-02/herp.derp.durr-p-7-2015-02-02-0062210428.txt.gz";
      expect(fn(uri)).to.equal('qux');
    });

    it("Should handle regexen", function () {
      var fn = s3t._tableStrToNamer("/s3:\/\/.*?\/someprefix\/(.*?)\//i");
      var uri = "s3://some-bucket/someprefix/herp.derp.durr/2015-02-02/herp.derp.durr-p-7-2015-02-02-0062210428.txt.gz";
      expect(fn(uri)).to.equal('herp_derp_durr');
    });
  });

  describe("_URIToTbl", function () {
    var re = new RegExp("s3:\/\/.*?\/someprefix\/(.*?)\/");
    var fn = _.partial(s3t._URIToTbl, re);
    it("Should correctly transform S3 URI to a table name", function () {
      var uri = "s3://some-bucket/someprefix/herp.derp.durr/2015-02-02/herp.derp.durr-p-7-2015-02-02-0062210428.txt.gz";
      expect(fn(uri)).to.equal('herp_derp_durr');

      var uri2 = "s3://some-bucket/someprefix/doink/2015-02-02/doink-p-7-2015-02-02-0062210428.txt.gz";
      expect(fn(uri2)).to.equal('doink');
    });

    it("Should throw if URI can't be turned into a table name", function () {
      var uri = "s3://hurrr/derr/durr";
      expect(fn.bind(null, uri)).to.throw();
    });
  });

  describe("_copyParamsTempl", function() {
    function checkPrelude(templ) {
      expect(templ).to.match(new RegExp("^copy %s from .*", "i"));
    }

    it("Should handle ON/OFF and boolean true/false", function() {
      var copyParams = {
        withParams: {
          booltrue: true,
          strtrue: "TRUE",
          stron: "on",
          boolfalse: false,
          strfalse: "FALSE",
          stroff: "off"
        }
      };
      var templ = s3t._copyParamsTempl(copyParams);
      checkPrelude(templ, copyParams);
      expect(templ).to.match(/\bbooltrue true/i);
      expect(templ).to.match(/\bstrtrue true/i);
      expect(templ).to.match(/\bstron on/i);
      expect(templ).to.match(/\bboolfalse false/i);
      expect(templ).to.match(/\bstrfalse false/i);
      expect(templ).to.match(/\bstroff off/i);
    });

    it("Should handle ENCODING", function () {
      var copyParams = {
        withParams: {
          ENCODING: "UTF8"
        }
      };
      var templ = s3t._copyParamsTempl(copyParams);
      checkPrelude(templ, copyParams);
      expect(templ).to.match(/\bencoding utf8/i);
    });

    it("Should handle number arguments", function () {
      var copyParams = {
        withParams: {
          DURR: 666,
          AHOY: 7
        }
      };
      var templ = s3t._copyParamsTempl(copyParams);
      checkPrelude(templ, copyParams);
      expect(templ).to.match(/\bdurr 666/i);
      expect(templ).to.match(/\bahoy 7/i);
    });

    it("Should handle argumentless parameters", function () {
      var copyParams = {
        args: ["GZIP", "SSH"]
      };
      var templ = s3t._copyParamsTempl(copyParams);
      checkPrelude(templ, copyParams);
      expect(templ).to.match(/\bgzip/i);
      expect(templ).to.match(/\bssh/i);
    });

    it("Should handle both types of parameters", function () {
      var copyParams = {
        args: ["GZIP", "SSH"],
        withParams: {
          CREDENTIALS: "aws_access_key_id=wub;aws_secret_access_key=fub",
          DELIMITER: "\\t",
          REGION: "us-east-1",
          ENCODING: "UTF8"
        }
      };
      var templ = s3t._copyParamsTempl(copyParams);
      checkPrelude(templ, copyParams);
      expect(templ).to.match(/\bdelimiter '\\t'/i);
      expect(templ).to.match(/\bcredentials 'aws_access_key_id=wub;aws_secret_access_key=fub'/i);
      expect(templ).to.match(/\bregion 'us-east-1'/i);
      expect(templ).to.match(/\bencoding utf8/i);
      expect(templ).to.match(/\bgzip/i);
      expect(templ).to.match(/\bssh/i);
    });
  });

  describe("_eventToS3URIs", function () {

    it("Should return an empty array when S3 schema version doesn't match", function () {
      event.Records[0].s3.s3SchemaVersion = "3.0";
      expect(s3t._eventToS3URIs(event)).to.deep.equal([]);
    });

    it("Should return an empty array when event version doesn't match", function () {
      event.Records[0].eventVersion = "3.0";
      expect(s3t._eventToS3URIs(event)).to.deep.equal([]);
    });

    it("Should return an empty array when no records are found", function () {
      expect(s3t._eventToS3URIs({Records: []})).to.deep.equal([]);
    });

    it("Should convert S3 events to S3 URIs", function () {
      var uris = s3t._eventToS3URIs(event);
      var wanted = "s3://some-bucket-name/someprefix/some.stuff.here/2015-01-30/some.stuff.here-p-9-2015-01-30-0044120221.txt.gz";
      expect(uris[0]).to.equal(wanted);
    });
  });
});