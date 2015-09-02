/**
 * Created by teklof on 30.1.15.
 */
var s3t = require('../lib/s3-to-rs');
var sp = require('../lib/sqs-poller');
var mup = require('../lib/manifest-uploader');
var tu = require('./test-utils');
var ut = require('../lib/utils');
var _ = require('lodash');
var $ = require('highland');
var sinon = require('sinon');
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

  function stubPgResult(rows) {

    var p;

    if(_.isError(rows)) {
      p = Promise.reject(rows);
    } else {
      p = Promise.resolve({rows: rows})
    }

    var cl = Promise.resolve({queryAsync: this.sinon.stub().returns(p)});
    this.sinon.stub(ut, "getPgClient").returns(cl.disposer(function () {}));
  }

  var clock;

  afterEach(function () {
    if (clock) {
      clock.restore();
      clock = null;
    }
  });

  var event;
  var s3URI
    , toTbl
    , table
    , copyParams;

  beforeEach(function () {
    s3URI = "s3://bucket/derr/some.stuff.here/fsadjlkgasjkl.csv";
    toTbl = "some.stuff.here";
    table = "some_stuff_here";
    copyParams = {
      "schema": "myschema",
      "table": "/s3:\/\/.*?\/derr\/(.*?)\//i",
      "args": [
        "GZIP",
        "TRUNCATECOLUMNS"
      ]
    };
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

  describe("TableManager", function () {

    var tsOpts
      , schema = "myschema"
      ;

    beforeEach(function () {
      tsOpts = {};
      tsOpts[table] = {
        period: 60 * 60 * 24
        , maxTables: 5
        , tablesInView: [1, 2, 4]
        , columns: ["col_a int encode lzo primary key"
          , "col_b bigint encode bytedict not null"
          , "col_c varchar(24) encode lzo not null"]
        , tableAttrs: ["DISTKEY(col_a)", "SORTKEY(col_b,col_c)"]
      };
    });

    function newTSM(queryRes) {

      var fp = new tu.FakePg();

      stubPgResult.bind(this, queryRes)();

      var usingPg = function(fn) {
        return Promise.using(ut.getPgClient(fp, "postgres://dasjkdsa"), fn);
      };

      return new s3t.TableManager(usingPg, schema, "", tsOpts);
    }

    describe("_selectFor", function () {
      it("should return a proper SELECT when a table has missing columns", function() {
        var tsm = newTSM.bind(this)();
        expect(tsm._selectFor(table, ["a", "b", "e"], ["a", "b", "c" , "d", "e", "f"]).toLowerCase())
          .to.equal("select a, b, null as c, null as d, e, null as f from " + table);
      });
    });

    describe("_columnsForTables", function () {
      it("should return object with table names and their columns", function () {
        var tables = _.times(5, function(n) {
            return "table_" + n;
          })

          , res = _.flatten(_.times(tables.length, function (tableN) {
              return _.times(tableN + 1, function (colN) {
                return {tablename: "table_" + tableN, column: "column_" + colN}
              });
            }))
          , wanted = { table_0: [ 'column_0' ],
            table_1: [ 'column_0', 'column_1' ],
            table_2: [ 'column_0',
                       'column_1',
                       'column_2' ],
            table_3: [ 'column_0',
                       'column_1',
                       'column_2',
                       'column_3' ],
            table_4: [ 'column_0',
                       'column_1',
                       'column_2',
                       'column_3',
                       'column_4' ] }
          , tsm = newTSM.bind(this)(res);
        return expect(tsm._columnsForTables(tables)).to.eventually.deep.equal(wanted);
      });
    });

    describe("_pruneTsTables", function() {
      it("should handle drop errors", function() {
        var tsm = newTSM.bind(this)()
          , max = tsm._options[table].maxTables
          , tableNames = _.times(max + 2, function(n) {
            return table + "_" + n;
          })
          , dropTable = this.sinon.stub(tsm, '_dropTables').rejects("nope")
          ;

        return expect(tsm._pruneTsTables(table, tableNames)).to.eventually.deep.equal(_.takeRight(tableNames, max)).then(function() {
          expect(dropTable).to.have.been.calledOnce;
          expect(dropTable).to.have.been.calledWithMatch(_.take(tableNames, 2));
        });
      });

      it("should drop oldest tables when pruning", function() {
        var tsm = newTSM.bind(this)()
          , max = tsm._options[table].maxTables
          , tableNames = _.times(max + 2, function(n) {
            return table + "_" + n;
          })
          , dropTable = this.sinon.stub(tsm, '_dropTables', Promise.resolve)
          ;
        return expect(tsm._pruneTsTables(table, tableNames)).to.eventually.deep.equal(_.takeRight(tableNames, max)).then(function() {
          expect(dropTable).to.have.been.calledOnce;
          expect(dropTable).to.have.been.calledWithMatch(_.take(tableNames, 2));
        });
      });
    });

    describe("_updateViews", function () {
      it("should call _createView", function () {
        var time = 172800000; //60 * 60 * 48 * 1000. The period is 24h, so this should give us a ts table with the same timestamp
        clock = this.sinon.useFakeTimers(time);
        var tsm = newTSM.bind(this)()
          , tables = _.times(tsOpts[table].maxTables, function(n) {
            return "table_" + n;
          })
          , colMap = _.reduce(tables, function(acc, v) {
            acc[v] = _.times(5, function (n) {
              return "column_" + n;
            });
            return acc;
          }, {})
          , columnsForTables = this.sinon.stub(tsm, "_columnsForTables").resolves(colMap)
          , createView = this.sinon.stub(tsm, "_createView", function(name, tbls, colMap) {
            return Promise.resolve(tbls);
          })
        // turn tablesInView to an array of arrays of table names, corresponding to views & their tables
          , viewTbls = _.map(tsOpts[table].tablesInView, _.partial(_.takeRight, tables))
          ;
        tsm._postfix = "_postfix";
        return tsm._updateViews(table, tables)
          .then(function () {
            // should have been called with n newest tables
            expect(createView).to.have.callCount(tsOpts[table].tablesInView.length)
            _.each(viewTbls, function(viewTbl) {
              expect(createView).to.have.been.calledWithMatch(table + "_view_" + viewTbl.length, viewTbl, colMap);
            })

          })
          ;

      });

      it("should get columns for all tables that participate in views", function () {
        var time = 172800000; //60 * 60 * 48 * 1000. The period is 24h, so this should give us a ts table with the same timestamp
        clock = this.sinon.useFakeTimers(time);
        var tsm = newTSM.bind(this)()
          , tables = _.times(tsOpts[table].maxTables, function(n) {
            return "table_" + n;
          })
          , columnsForTables = this.sinon.stub(tsm, "_columnsForTables").resolves(_.reduce(tables, function(acc, v) {
            acc[v] = _.times(5, function (n) {
              return "column_" + n;
            });
            return acc;
          }, {}))
          , createView = this.sinon.stub(tsm, "_createView", function(name, tbls, colMap) {
            return Promise.resolve(tbls);
          })
        // turn tablesInView to an array of arrays of table names, corresponding to views & their tables
          , viewTbls = _.map(tsOpts[table].tablesInView, _.partial(_.takeRight, tables))
          ;

        return expect(tsm._updateViews(table, tables))
          .to.eventually.deep.equal(tables)
          .then(function () {
            // should have been called with n newest tables
            expect(columnsForTables).to.have.been.calledWithExactly(_.takeRight(tables,
              _.max(tsOpts[table].tablesInView)))
          })
          ;

      });
    });

    describe("tableFor", function () {

      it("should update view when a new TS table was not created", function () {
        var time = 172800000; //60 * 60 * 48 * 1000. The period is 24h, so this should give us a ts table with the same timestamp
        clock = this.sinon.useFakeTimers(time);

        var err = new Error("dsajlkads");
        err.code = "42P07";

        var tables = ["table1", "table2"]
          , tsm = newTSM.bind(this)(err)
          , prune = this.sinon.stub(tsm, "_pruneTsTables").resolves(tables)
          , updView = this.sinon.stub(tsm, "_updateViews").resolves(tables)
          , listTs = this.sinon.stub(tsm, "_listTsTables").resolves(tables)
          ;
        return tsm.tableFor(table).then(function() {
          expect(updView).to.have.been.calledOnce;
          expect(updView).to.have.been.calledWithMatch(table, ["table1", "table2"])
        });
      });

      it("should update view when a new TS table was created", function () {
        var time = 172800000; //60 * 60 * 48 * 1000. The period is 24h, so this should give us a ts table with the same timestamp
        clock = this.sinon.useFakeTimers(time);

        var tables = ["table1", "table2"]
          , tsm = newTSM.bind(this)()
          , prune = this.sinon.stub(tsm, "_pruneTsTables").resolves(tables)
          , updView = this.sinon.stub(tsm, "_updateViews").resolves(tables)
          , listTs = this.sinon.stub(tsm, "_listTsTables").resolves(tables)
          ;
        return tsm.tableFor(table).then(function() {
          expect(updView).to.have.been.calledOnce;
          expect(updView).to.have.been.calledWithMatch(table, ["table1", "table2"])
        });
      });

      it("should handle _pruneTsTables errors", function() {
        var time = 172800000; //60 * 60 * 48 * 1000. The period is 24h, so this should give us a ts table with the same timestamp
        clock = this.sinon.useFakeTimers(time);

        var tsm = newTSM.bind(this)()
          , tables = ["table1", "table2"]
          , prune = this.sinon.stub(tsm, "_pruneTsTables").rejects(new Error("A HURR HURR HOI"))
          , updView = this.sinon.stub(tsm, "_updateViews").resolves(tables)
          , listTs = this.sinon.stub(tsm, "_listTsTables").resolves(tables)
          ;
        return expect(tsm.tableFor(table)).to.be.fulfilled;
      });

      it("should not call _pruneTsTables when a new TS table was not created", function() {
        var time = 172800000; //60 * 60 * 48 * 1000. The period is 24h, so this should give us a ts table with the same timestamp
        clock = this.sinon.useFakeTimers(time);

        var err = new Error("dsajlkads");
        err.code = "42P07";

        var tsm = newTSM.bind(this)(err)
          , prune = this.sinon.stub(tsm, "_pruneTsTables").resolves([])
          , updView = this.sinon.stub(tsm, "_updateViews").resolves([])
          , listTs = this.sinon.stub(tsm, "_listTsTables").resolves(["table1", "table2"])
          ;
        return tsm.tableFor(table).then(function() {
          expect(prune).to.not.have.been.called;
        });
      });

      it("should call _pruneTsTables when a new TS table was created", function() {
        var time = 172800000; //60 * 60 * 48 * 1000. The period is 24h, so this should give us a ts table with the same timestamp
        clock = this.sinon.useFakeTimers(time);

        var tsm = newTSM.bind(this)()
          , prune = this.sinon.stub(tsm, "_pruneTsTables").resolves([])
          , updView = this.sinon.stub(tsm, "_updateViews").resolves([])
          , listTs = this.sinon.stub(tsm, "_listTsTables").resolves(["table1", "table2"])
          ;
        return tsm.tableFor(table).then(function() {
          expect(prune).to.have.been.calledOnce;
          expect(prune).to.have.been.calledWithMatch(table);
        });
      });

      it("should return table name when ts table doesn't exist", function() {
        var time = 172800000; //60 * 60 * 48 * 1000. The period is 24h, so this should give us a ts table with the same timestamp
        clock = this.sinon.useFakeTimers(time);

        var tsm = newTSM.bind(this)()
          , tables = ["table1", "table2"]
          , updView = this.sinon.stub(tsm, "_updateViews").resolves(tables)
          , listTs = this.sinon.stub(tsm, "_listTsTables").resolves(tables)
          ;

        tsm._postfix = "_postfix";

        this.sinon.stub(tsm, "_pruneTsTables").resolves([]);
        return expect(tsm.tableFor(table)).to.eventually.equal(schema + "." + table + "_postfix_ts_1970_01_03");
      });

      it("should return table name when ts table exists", function() {
        var time = 172800000; //60 * 60 * 48 * 1000. The period is 24h, so this should give us a ts table with the same timestamp
        clock = this.sinon.useFakeTimers(time);

        var err = new Error("dsajlkads");
        err.code = "42P07";

        var tsm = newTSM.bind(this)(err)
          , tables = ["table1", "table2"]
          , updView = this.sinon.stub(tsm, "_updateViews").resolves(tables)
          , listTs = this.sinon.stub(tsm, "_listTsTables").resolves(tables)
        ;

        tsm._postfix = "_postfix";

        this.sinon.stub(tsm, "_pruneTsTables").resolves([]);
        return expect(tsm.tableFor(table)).to.eventually.equal(schema + "." + table + "_postfix_ts_1970_01_03");
      });
    });
  });

  describe("S3Copier", function () {

    function newCopier(parameters) {
      var pgConnErr = parameters.pgConnErr;
      var pgQueryErr = parameters.pgQueryErr;
      var pgDoneCb = parameters.pgDoneCb;
      var s3Event = parameters.s3Event;
      var rsStatus = parameters.rsStatus;
      var msgs = parameters.msgs;
      s3Event = s3Event || {};
      var fakePoller = new tu.FakePoller("derr/some.stuff.here/", msgs)
        , fakePg = new tu.FakePg(pgConnErr, pgQueryErr, pgDoneCb || sinon.spy())
        , fakeS3 = new tu.FakeS3(s3Event.put, s3Event.del)
        , fakeRs = new tu.FakeRedshift("blaster", rsStatus || "available")
        , options = {
          Redshift: {
            connStr: "postgres://bler"
            , params: {
              ClusterIdentifier: "qux"
            }
          }
          , pollIntervalS: 60
          , manifestUploader: {
            "maxToUpload": 10
            , "maxWaitSeconds": 0.00001
            , "mandatory": true
            , "bucket": "manifest-bukkit"
            , "prefix": "manifest-prefix/"
          }
          , timeSeries: {
            table: {
              period: 60 * 60 * 24
              , tablesInView: 10
              , maxTables: 30
              , columns: []
              , tableAttrs: []
            }
          }
        };

      return new s3t.S3Copier(fakePoller, Promise.promisifyAll(fakePg), fakeS3, fakeRs, copyParams, options);
    }

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

    function newSQSMsg(n) {
      return new tu.SQSMessage(n, "bucket", "derr/some.stuff.here/");
    }

    describe("seenStream", function () {

    });

    describe("errorStream", function () {
      it("should contain errors from _unseenStream", function (done) {
        var c = newCopier({msgs: newSQSMsg(10).Messages})
          , error = new Error("derr")
          ;

        this.sinon.stub(mup.Manifest.prototype, "_upload", function() {
          console.log("Stubbed Manifest._upload");
          this._reject(error);
          return this._promise;
        });

        c.start(true).done(function() {
          c._unseenStream.fork().errors(function () {
            console.log("Welp, found an error");
            done(new Error("_unseenStream shouldn't have any errors"));
          });

          c.errorStream.fork().each(function(err) {
            expect(err).to.deep.equal(error);
            done();
          });
        });

      });
    });

    describe.skip("_availHandler", function () {

      it("should restart S4QS when cluster becomes available again", function () {

        clock = this.sinon.useFakeTimers();

        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null, s3Event: null, rsStatus: "available"})
          , uplStart = this.sinon.stub(c._uploader, "start")
          , uplStop = this.sinon.stub(c._uploader, "stop")
          , cStop = this.sinon.spy(c, "stop")
          , _isClusterAvail = this.sinon.stub(c, "_isClusterAvail")
          ;

        // everything's OK at startup, then the cluster goes down, and then up again
        _isClusterAvail.onCall(0).returns(Promise.resolve(true));
        _isClusterAvail.onCall(1).returns(Promise.resolve(false));
        _isClusterAvail.returns(Promise.resolve(true));

        c._availCheckInterval = 500;

        return c.start()
          .bind(this)
          .then(function () {
            expect(uplStart).to.have.been.calledOnce;
            clock.tick(c._availCheckInterval * 1000);
          })
          .then(function() {
            clock.tick(c._availCheckInterval * 1000);
            clock.restore();
            clock = null;
          }).delay(2)
          .then(function() {
            expect(poll).to.have.been.calledTwice;
            expect(uplStart).to.have.been.calledTwice;
          })
          ;
      });

      it("should stop S4QS when cluster goes unavailable", function () {

        clock = this.sinon.useFakeTimers();

        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null, s3Event: null, rsStatus: "available"})
          , uplStart = this.sinon.stub(c._uploader, "start")
          , uplStop = this.sinon.stub(c._uploader, "stop")
          , cStop = this.sinon.spy(c, "stop")
          , _isClusterAvail = this.sinon.stub(c, "_isClusterAvail")
          ;

        // everything's OK at startup but then the cluster goes down. OH NOES.
        _isClusterAvail.onCall(0).returns(Promise.resolve(true));
        _isClusterAvail.returns(Promise.resolve(false));

        c._availCheckInterval = 500;
        return c.start()
          .bind(this)
          .then(function () {
            expect(uplStart).to.have.been.called;
            expect(poll).to.have.been.called;
            clock.tick(c._availCheckInterval * 1000);
            clock.restore();
            clock = null;
          }).delay(2)
          .then(function() {
            expect(uplStop).to.have.been.called;
            expect(cStop).to.have.been.called;
          })
          ;
      });
    });

    describe("_isClusterAvail", function () {

      it("should return false if availability check fails", function () {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null, s3Event: null, rsStatus: "resizing"});
        this.sinon.stub(c._rs, "describeClustersAsync").returns(Promise.reject(new Error("nope nope nope nope")));
        return expect(c._isClusterAvail()).to.become.false;
      });

      it("should return false if cluster is not available", function () {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null, s3Event: null, rsStatus: "resizing"});
        return expect(c._isClusterAvail()).to.become.true;
      });

      it("should return true if cluster is available", function () {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null, s3Event: null});
        return expect(c._isClusterAvail()).to.become.true;
      });
    });

    describe("start", function () {

      it("should only have one _availHandler interval running", function () {

        clock = this.sinon.useFakeTimers();

        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null, s3Event: null, rsStatus: "available"})
          , uplStart = this.sinon.stub(c._uploader, "start")
          , uplStop = this.sinon.stub(c._uploader, "stop")
          , cStop = this.sinon.spy(c, "stop")
          , _isClusterAvail = this.sinon.stub(c, "_isClusterAvail").returns(Promise.resolve(false))
          , _availHandler = this.sinon.spy(c, "_availHandler")
          ;
        c._availCheckInterval = 500;

        return c.start().tap(c.start.bind(c)).then(function () {
          clock.tick(c._availCheckInterval * 1000);
          expect(_availHandler).to.have.been.calledOnce;
        });

      });

      it.skip("should not poll for messages if the redshift cluster is unavailable", function() {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null, s3Event: null, rsStatus: "rebooting"})
          , uplStart = this.sinon.stub(c._uploader, 'start')
          , poll = this.sinon.stub(c._poller, 'poll')
          , isClusterAvail = this.sinon.stub(c, "_isClusterAvail").returns(Promise.resolve(false))
        ;
        return expect(c.start()).to.be.rejected.then(function () {
          expect(uplStart).to.not.have.been.called;
          expect(poll).to.not.have.been.called;
        });
      });
    });

    describe("stop", function () {
      it("should return a promise of pending manifests", function() {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
          , mf = newManifest(true, 10, null, null, "table1")
          , def = defer()
          ;


        this.sinon.stub(c, "_connAndCopy").returns(def.promise);
        this.sinon.stub(c, "_delete").returns(Promise.resolve());
        this.sinon.stub(c._tblMgr, "tableFor").returns(Promise.resolve("table1"));
        this.sinon.stub(mf, "delete").returns(Promise.resolve(mf.manifestURI));

        c.start();

        c._onManifest(mf);
        var p = c.stop().then(function (pend) {
          expect(pend["table1"]).to.deep.equal(mf);
        });

        def.resolve(mf.manifestURI);

        return p
      });
    });


    describe.skip("_dedup", function () {
      it("should delete duplicate messages", function () {
        var sm = newSQSMsg(10)
          , seen = sm.Messages.slice(0, 5)
          , c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
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
          , c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
          ;
        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.resolve());
        expect(c._dedup(sm.Messages)).to.eventually.deep.equal(sm.Messages);
      });

      it("should return a promise of an array of non-duplicate messages when deletion fails", function () {
        var sm = newSQSMsg(10)
          , seen = sm.Messages.slice(0, 5)
          , c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
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
          , c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
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
          , p = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
          , spy = this.sinon.spy(p._seenMsgs, "set")
          ;
        p._markSeen(sm.Messages);
        var firstElem = ut.splat(ut.get('0'));
        expect(firstElem(spy.args)).to.deep.equal(ut.messagesToURIs(sm.Messages));
      });
    });

    describe("_onManifest", function () {

      it("should not join fulfilled manifest promises", function () {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
          , _delete = this.sinon.stub(c, "_delete").returns(Promise.resolve())
          , mf = newManifest(true, 10, null, null, "table1")
          , mf2 = newManifest(true, 10, null, null, "table1")
          , _connAndCopy = this.sinon.stub(c, "_connAndCopy", Promise.resolve)
          ;

        this.sinon.stub(mf, "delete").returns(Promise.resolve(mf.manifestURI));
        this.sinon.stub(mf2, "delete").returns(Promise.resolve(mf2.manifestURI));
        this.sinon.stub(c._tblMgr, "tableFor").returns(Promise.resolve("table1"));

        c._onManifest(mf);

        return Promise.props(c._manifestsPending).then(function () {
          c._onManifest(mf2);
          return c._manifestsPending;
        }).then(function(pend) {
            expect(pend["table1"]).to.not.be.instanceOf(Array);
          });
      });

      it.skip("should join pending manifest promises", function () {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
          , _delete = this.sinon.stub(c, "_delete").returns(Promise.resolve())
          , mf = newManifest(true, 10, null, null, "table1")
          , mf2 = newManifest(true, 10, null, null, "table1")
          , mfDelete = this.sinon.stub(mf, "delete").returns(Promise.resolve(mf.manifestURI))
          , mf2Delete =  this.sinon.stub(mf2, "delete").returns(Promise.resolve(mf2.manifestURI))
          , _connAndCopy = this.sinon.stub(c, "_connAndCopy")
          , def = defer()
          , def2 = defer()
          ;

        this.sinon.stub(c._tblMgr, "tableFor").returns(Promise.resolve("table1"));
        _connAndCopy.onCall(0).returns(def.promise);
        _connAndCopy.onCall(1).returns(def2.promise);
        _connAndCopy.returns(Promise.resolve());

        c._onManifest(mf);
        c._onManifest(mf2);

        def.resolve(mf.manifestURI);
        setTimeout(def2.resolve.bind(def2, mf2.manifestURI), 20);

        return Promise.props(c._manifestsPending).then(function (pend) {
          expect(pend["table1"]).to.be.instanceOf(Array);
          expect(_connAndCopy).to.have.been.calledTwice;
          expect(_delete).to.have.been.calledTwice;
          expect(mfDelete).to.have.been.calledOnce;
          expect(mf2Delete).to.have.been.calledOnce;
        });
      });

      it("should catch copy errors, and not delete the messages or the manifest", function () {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
          , mf = newManifest(true, 10, null, null, "table1")
          , _delete = this.sinon.stub(c, "_delete").returns(Promise.resolve())
          , mfDelete = this.sinon.stub(mf, "delete").returns(Promise.resolve(mf.manifestURI))
          ;

        this.sinon.stub(c._tblMgr, "tableFor").returns(Promise.resolve("table1"));
        this.sinon.stub(c, "_connAndCopy").returns(Promise.reject(new Error("not gonna happen, bub")));

        c._onManifest(mf);
        return Promise.props(c._manifestsPending).then(function () {
          expect(c._manifestsPending["table1"].isResolved()).to.be.true;
          expect(_delete).to.not.have.been.called;
          expect(mfDelete).to.not.have.been.called;
        });
      });

      it("should catch message deletion errors", function () {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
          , mf = newManifest(true, 10, null, null, "table1")
          ;

        this.sinon.stub(c, "_connAndCopy").returns(Promise.resolve(mf.manifestURI));
        this.sinon.stub(c, "_delete").returns(Promise.reject(new Error("nope")));
        this.sinon.stub(mf, "delete").resolves(mf.manifestURI);
        this.sinon.stub(c._tblMgr, "tableFor").returns(Promise.resolve("table1"));

        c._onManifest(mf);
        return Promise.props(c._manifestsPending).then(function () {
          expect(c._manifestsPending["table1"].isResolved()).to.be.true;
        });
      });

      it("should delete the manifest when tableFor fails", function () {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
          , mf = newManifest(true, 10, null, null, "table1")
          ;

        this.sinon.stub(c, "_connAndCopy").resolves(mf.manifestURI);
        this.sinon.stub(c, "_delete").resolves();
        this.sinon.stub(mf, "delete").resolves(mf.manifestURI);
        this.sinon.stub(c._tblMgr, "tableFor").rejects(new Error("DERR"));

        c._onManifest(mf);
        return Promise.props(c._manifestsPending).then(function () {
          expect(mf.delete).to.have.been.calledOnce;
        });
      });

      it("should catch tableFor errors", function () {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
          , mf = newManifest(true, 10, null, null, "table1")
          ;

        this.sinon.stub(c, "_connAndCopy").resolves(mf.manifestURI);
        this.sinon.stub(c, "_delete").resolves();
        this.sinon.stub(mf, "delete").resolves(mf.manifestURI);
        this.sinon.stub(c._tblMgr, "tableFor").rejects(new Error("DERR"));

        c._onManifest(mf);
        return expect(Promise.props(c._manifestsPending)).to.be.fulfilled.then(function () {
          expect(c._manifestsPending["table1"].isResolved()).to.be.true;
        });
      });

      it("should catch manifest deletion errors", function () {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
          , mf = newManifest(true, 10, null, null, "table1")
          ;

        this.sinon.stub(c, "_connAndCopy").returns(Promise.resolve(mf.manifestURI));
        this.sinon.stub(c, "_delete").returns(Promise.resolve());
        this.sinon.stub(mf, "delete").returns(Promise.reject(new Error("I DON'T THINK SO")));
        this.sinon.stub(c._tblMgr, "tableFor").returns(Promise.resolve("table1"));

        c._onManifest(mf);
        return Promise.props(c._manifestsPending).then(function () {
          expect(c._manifestsPending["table1"].isResolved()).to.be.true;
        });
      });

      it("should set _manifestsPending", function () {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
          , mf = newManifest(true, 10, null, null, "table1")
        ;

        this.sinon.stub(c, "_connAndCopy").returns(Promise.resolve(mf.manifestURI));
        this.sinon.stub(c, "_delete").returns(Promise.resolve());
        this.sinon.stub(mf, "delete").returns(Promise.resolve(mf.manifestURI));
        this.sinon.stub(c._tblMgr, "tableFor").returns(Promise.resolve("table1"));

        c._onManifest(mf);
        return Promise.props(c._manifestsPending).then(function () {
          expect(c._manifestsPending["table1"].isResolved()).to.be.true;
          expect(_.keys(c._manifestsPending)).to.deep.equal(["table1"]);
        });
      });
    });

    describe.skip("_onMsgs", function () {
      it("should only give deduplicated messages to the uploader", function () {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
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
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
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
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null})
          , addMessages = this.sinon.stub(c._uploader, "addMessages")
          , msgs = newSQSMsg(5).Messages
        ;

        this.sinon.stub(c, "_dedup", Promise.resolve);

        return c._onMsgs(msgs).then(function() {
          expect(addMessages).to.have.been.calledWithMatch(msgs);
        });
      });
    });

    describe("_delete", function () {
      it("should call poller.deleteMsg", function () {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null});
        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.resolve());
        var msgs = new tu.SQSMessage(10, "gler", "flor").Messages;
        return expect(c._delete(msgs)).to.be.fulfilled.then(function () {
          expect(c._poller.deleteMsgs).to.have.been.calledOnce.and.calledWithMatch(msgs);
        });
      });

      it("should return a rejected promise on deletion error", function () {
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: null});
        this.sinon.stub(c._poller, "deleteMsgs").returns(Promise.reject(new Error("welp")));
        return expect(c._delete(new tu.SQSMessage(10, "gler", "flor").Messages)).to.be.rejectedWith(Error, "welp");
      });
    });

    describe("_connAndCopy", function () {
      it("should return a promise of the S3 URI on successful copy", function () {
        var doneCb = this.sinon.spy();
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: doneCb});
        return expect(c._connAndCopy(s3URI, table)).to.become(s3URI);
      });

      it("should connect to pg", function (done) {
        var doneCb = this.sinon.spy();
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: doneCb});
        this.sinon.spy(c._pg, "connect");
        return expect(c._connAndCopy(s3URI)).to.be.fulfilled
          .then(function () {
            expect(c._pg.connect).to.have.been.calledOnce.and.calledWith("postgres://bler");
          }).should.notify(done);
      });

      it("should do queries", function (done) {
        var doneCb = this.sinon.spy();
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: doneCb});
        return expect(c._connAndCopy(s3URI, table)).to.be.fulfilled
          .then(function () {
            expect(c._pg.client.queryAsync).to.have.been.calledThrice.and.calledWithMatch(util.format("COPY %s FROM '%s' %s;",
              table, s3URI,
              copyParams.args.join(' ')));
          }).should.notify(done);
      });

      it("Should return the client to the pool when a query succeeds", function (done) {
        var doneCb = this.sinon.spy();
        var c = newCopier({pgConnErr: null, pgQueryErr: null, pgDoneCb: doneCb});
        return expect(c._connAndCopy(s3URI)).to.be.fulfilled
          .then(function () {
            expect(doneCb).to.have.been.calledOnce.and.calledWithExactly();
          }).should.notify(done);
      });

      it("Should not return the client to the pool when a query fails", function (done) {
        var doneCb = this.sinon.spy();
        var c = newCopier({pgConnErr: null, pgQueryErr: new Error("query error"), pgDoneCb: doneCb});
        return expect(c._connAndCopy(s3URI)).to.be.rejectedWith(Error, "query error")
          .then(function () {
            expect(doneCb).to.have.been.calledOnce.and.calledWith(c._pg.client);
          }).should.notify(done);
      });

      it("should return a rejected promise on connection errors", function () {
        var doneCb = this.sinon.spy();
        var c = newCopier({pgConnErr: new Error("connection error"), pgQueryErr: null, pgDoneCb: doneCb});
        return expect(c._connAndCopy(s3URI)).to.be.rejectedWith(Error, "connection error");
      });

      it("should return a rejected promise on query errors", function () {
        var doneCb = this.sinon.spy();
        var c = newCopier({pgConnErr: null, pgQueryErr: new Error("query error"), pgDoneCb: doneCb});
        return expect(c._connAndCopy(s3URI)).to.be.rejectedWith(Error, "query error");
      });
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