/**
 * Created by teklof on 30.1.15.
 */
var s3t = require('../lib/s3-to-rs');
var sp = require('../lib/sqs-poller');
var mup = require('../lib/manifest-uploader');
var tu = require('./test-utils');
var ut = require('../lib/utils');
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

function defer() {
  var resolver, rejecter;

  var p = new Promise(function (resolve, reject) {
    resolver = resolve;
    rejecter = reject;
  });

  return {reject: rejecter, resolve: resolver, promise: p};
}

describe("S3 to Redshift copier", function () {

  var clock;

  afterEach(function () {
    if (clock) {
      clock.restore();
      clock = null;
    }
  });

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

    function newCopier(pgConnErr, pgQueryErr, pgDoneCb, s3Event) {
      s3Event = s3Event || {}
      var fakePoller = new tu.FakePoller("derr/some.stuff.here")
        , fakePg = new tu.FakePg(pgConnErr, pgQueryErr, pgDoneCb)
        , fakeS3 = new tu.FakeS3(s3Event.put, s3Event.del)
        ;
      var options = {connStr: "postgres://bler", pollIntervalS: 60, manifestUploader: {
        "minToUpload": 10,
        "maxWaitSeconds": 300,
        "mandatory": true,
        "bucket": "manifest-bukkit",
        "prefix": "manifest-prefix/"
      }};

      return new s3t.S3Copier(fakePoller, fakePg, fakeS3, copyParams, options);
    }


    function newSQSMsg(n) {
      return new tu.SQSMessage(n, "bucket", "prefix/");
    }

    describe("_dedup", function () {
      it("should delete duplicate messages", function () {
        var sm = newSQSMsg(10)
          , seen = sm.Messages.slice(0, 5)
          , c = newCopier(null, null, null)
          , deleteMsgs = this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.resolve())
          ;
        _.each(_.pluck(seen, 'MessageId'), function(mid) {
          c._seenMsgs.set(mid, true);
        });



        return c._dedup(sm.Messages).then(function() {
          expect(deleteMsgs).to.have.been.calledOnce;
          expect(deleteMsgs.args[0][0]).to.deep.equal(seen);
        });
      });

      it("should return a promise of an array of messages when no duplicates are found", function () {
        var sm = newSQSMsg(10)
          , c = newCopier(null, null, null)
          ;
        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.resolve());
        expect(c._dedup(sm.Messages)).to.eventually.deep.equal(sm.Messages);
      });

      it("should return a promise of an array of non-duplicate messages when deletion fails", function () {
        var sm = newSQSMsg(10)
          , seen = sm.Messages.slice(0, 5)
          , c = newCopier(null, null, null)
          ;
        _.each(_.pluck(seen, 'MessageId'), function(mid) {
          c._seenMsgs.set(mid, true);
        });
        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.reject(new Error("uh oh")));

        expect(c._dedup(sm.Messages)).to.eventually.deep.equal(sm.Messages.slice(5));
      });

      it("should return a promise of an array of non-duplicate messages when deletion succeeds", function () {
        var sm = newSQSMsg(10)
          , seen = sm.Messages.slice(0, 5)
          , c = newCopier(null, null, null)
          ;
        _.each(_.pluck(seen, 'MessageId'), function(mid) {
          c._seenMsgs.set(mid, true);
        });
        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.resolve());

        expect(c._dedup(sm.Messages)).to.eventually.deep.equal(sm.Messages.slice(5));
      });
    });

    describe("_markSeen", function () {
      it("should mark messages as seen", function () {
        var sm = newSQSMsg(10)
          , p = newCopier(null, null, null)
          , spy = this.sinon.spy(p._seenMsgs, "set")
          ;
        p._markSeen(sm.Messages);
        var firstElem = ut.splat(ut.get('0'));
        expect(firstElem(spy.args)).to.deep.equal(_.pluck(sm.Messages, 'MessageId'));
      });
    });

    describe("_onManifest", function () {

      function newManifest(mandatory, n, put, del, table) {
        var s3 = new tu.FakeS3(put, del)
          , manifest = new mup.Manifest(s3, {mandatory:!!mandatory,
            bucket: "manif-bucket",
            prefix: "manif-prefix/",
            table:table})
          ;

        n = n || 0;

        manifest._addAll(newSQSMsg(n).Messages);

        return  manifest;
      }

      it("should not join fulfilled manifest promises", function () {
        var c = newCopier(null, null, null)
          , _delete = this.sinon.stub(c, "_delete").returns(Promise.resolve())
          , mf = newManifest(true, 10, null, null, "table1")
          , mf2 = newManifest(true, 10, null, null, "table1")
          , _connAndCopy = this.sinon.stub(c, "_connAndCopy", Promise.resolve)
          ;

        this.sinon.stub(mf, "delete").returns(Promise.resolve(mf.manifestURI));
        this.sinon.stub(mf2, "delete").returns(Promise.resolve(mf2.manifestURI));

        c._onManifest(mf);

        return Promise.props(c._manifestsPending).then(function () {
          c._onManifest(mf2);
          return c._manifestsPending;
        }).then(function(pend) {
            expect(pend["table1"]).to.not.be.instanceOf(Array);
          });
      });

      it("should join pending manifest promises", function () {
        var c = newCopier(null, null, null)
          , _delete = this.sinon.stub(c, "_delete").returns(Promise.resolve())
          , mf = newManifest(true, 10, null, null, "table1")
          , mf2 = newManifest(true, 10, null, null, "table1")
          , mfDelete = this.sinon.stub(mf, "delete").returns(Promise.resolve(mf.manifestURI))
          , mf2Delete =  this.sinon.stub(mf2, "delete").returns(Promise.resolve(mf2.manifestURI))
          , _connAndCopy = this.sinon.stub(c, "_connAndCopy")
          , def = defer()
          , def2 = defer()
          ;


        _connAndCopy.onCall(0).returns(def.promise);
        _connAndCopy.onCall(1).returns(def2.promise);
        _connAndCopy.returns(Promise.resolve());

        c._onManifest(mf);
        c._onManifest(mf2);

        def.resolve(mf.manifestURI);
        setTimeout(def2.resolve.bind(def2, mf2.manifestURI), 100);

        return Promise.props(c._manifestsPending).then(function (pend) {
          expect(pend["table1"]).to.be.instanceOf(Array);
          expect(_connAndCopy).to.have.been.calledTwice;
          expect(_delete).to.have.been.calledTwice;
          expect(mfDelete).to.have.been.calledOnce;
          expect(mf2Delete).to.have.been.calledOnce;
        });
      });

      it("should set _manifestsPending", function () {
        var c = newCopier(null, null, null)
          , mf = newManifest(true, 10, null, null, "table1")
        ;

        this.sinon.stub(c, "_connAndCopy").returns(Promise.resolve(mf.manifestURI));
        this.sinon.stub(c, "_delete").returns(Promise.resolve());
        this.sinon.stub(mf, "delete").returns(Promise.resolve(mf.manifestURI));

        c._onManifest(mf);
        return Promise.props(c._manifestsPending).then(function () {
          expect(c._manifestsPending["table1"].isResolved()).to.be.true;
          expect(_.keys(c._manifestsPending)).to.deep.equal(["table1"]);
        });
      });
    });

    describe("_onMsgs", function () {
      it("should only give deduplicated messages to the uploader", function () {
        var c = newCopier(null, null, null)
          , _schedulePoll = this.sinon.stub(c, "_schedulePoll")
          , sm = newSQSMsg(20).Messages
          , seen = sm.slice(0,10)
          , notSeen = sm.slice(10)
          , addMessages = this.sinon.stub(c._uploader, "addMessages")
          ;

        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.resolve());

        _.each(_.pluck(seen, "MessageId"), function(mid) {
          c._seenMsgs.set(mid, true);
        });

        return c._onMsgs(sm).then(function() {
          expect(addMessages).to.have.been.calledWithMatch(notSeen);
          expect(addMessages).to.not.have.been.calledWithMatch(seen);
        });
      });

      it("should should schedule a new poll", function () {
        clock = this.sinon.useFakeTimers(1000);
        var c = newCopier(null, null, null)
          , _schedulePoll = this.sinon.stub(c, "_schedulePoll")
          , msgs = newSQSMsg(10).Messages
          ;

        this.sinon.stub(c._uploader, "addMessages");
        this.sinon.stub(c, "_dedup", Promise.resolve);

        return c._onMsgs(msgs).then(function() {
          expect(_schedulePoll).to.have.been.calledWithExactly(1000 + c._pollIntervalS * 1000);
        });
      });

      it("should give received messages to the manifest uploader", function () {
        var c = newCopier(null, null, null)
          , addMessages = this.sinon.stub(c._uploader, "addMessages")
          , msgs = newSQSMsg(5).Messages
        ;

        this.sinon.stub(c, "_dedup", Promise.resolve);

        return c._onMsgs(msgs).then(function() {
          expect(addMessages).to.have.been.calledWithMatch(msgs);
        });
      });
    });

    describe.skip("_onMsgs", function () {
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
        clock = this.sinon.useFakeTimers(10000);
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

      it("Should only COPY deduped messages", function () {
        var c = newCopier(null, null, null)
          , sm = newSQSMsg(20)
          , seen = sm.Messages.slice(0,10)
          , notSeen = sm.Messages.slice(10)
          , getUris = ut.splat(ut.send('s3URIs'))
          , notSeenUris = _.flatten(getUris(_.pluck(notSeen, '_s3Event')))
          , seenUris = _.flatten(getUris(_.pluck(seen, '_s3Event')))
          , connAndCopy = this.sinon.stub(c, "_connAndCopy", Promise.resolve)
          ;

        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.resolve());

        _.each(_.pluck(seen, "MessageId"), function(mid) {
          c._seenMsgs.set(mid, true);
        });

        c._onMsgs(sm.Messages);
        return c._onMsgPending.then(function () {
          expect(connAndCopy).to.have.callCount(notSeen.length);

          _.each(notSeenUris, function (uri) {
            expect(connAndCopy).to.have.been.calledWithExactly(uri);
          });

          _.each(seenUris, function (uri) {
            expect(connAndCopy).to.not.have.been.calledWithExactly(uri);
          });
        });
      });

      it("Should call _dedup before copying", function () {
        var c = newCopier(null, null, null)
          , sm = newSQSMsg(20)
          , seen = sm.Messages.slice(0,10)
          , notSeen = sm.Messages.slice(10)
          , dedup = this.sinon.spy(c, "_dedup")
        ;

        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.resolve());
        this.sinon.stub(c, "_connAndCopy", function(uri) {
          return Promise.resolve(uri);
        });

        _.each(_.pluck(seen, "MessageId"), function(mid) {
          c._seenMsgs.set(mid, true);
        });

        c._onMsgs(sm.Messages);
        return c._onMsgPending.then(function () {
          expect(dedup).to.have.been.calledOnce;
//          console.error("args", inspect(dedup.args[0]));
          expect(dedup).to.have.been.calledWithMatch(sm.Messages);
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
          _.each(ev.s3URIs(), function (uri) {
            expect(c._connAndCopy).to.have.been.calledWithExactly(uri);
          });
        });
      });
    });

    describe("_delete", function () {
      it("should call poller.deleteMsg", function () {
        var c = newCopier(null, null, null);
        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.resolve());
        var msgs = new tu.SQSMessage(10, "gler", "flor").Messages;
        return expect(c._delete(msgs)).to.be.fulfilled.then(function () {
          expect(c._poller.deleteMsgs).to.have.been.calledOnce.and.calledWithMatch(msgs);
        });
      });

      it("should not return a rejected promise on deletion error", function () {
        var c = newCopier(null, null, null);
        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.reject(new Error("welp")));
        return expect(c._delete(new tu.SQSMessage(10, "gler", "flor").Messages)).to.be.fulfilled;
      });
    });

    describe("_connAndCopy", function () {
      it("should return a promise of the S3 URI on successful copy", function () {
        var doneCb = this.sinon.spy();
        var c = newCopier(null, null, doneCb);
        return expect(c._connAndCopy(s3URI, table)).to.become(s3URI);
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
        return expect(c._connAndCopy(s3URI, table)).to.be.fulfilled
          .then(function () {
            expect(c._pg.client.query).to.have.been.calledOnce.and.calledWithMatch(util.format("COPY %s FROM '%s' %s;",
              table + c._tablePostfix, s3URI,
              copyParams.args.join(' ')));
          }).should.notify(done);
      });

      it("should do queries", function (done) {
        var doneCb = this.sinon.spy();
        var c = newCopier(null, null, doneCb);
        return expect(c._connAndCopy(s3URI, table)).to.be.fulfilled
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

});