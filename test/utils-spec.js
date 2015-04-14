/**
 * Created by teklof on 24.3.15.
 */

var ut = require('../lib/utils');

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

describe("utils", function () {

  describe("nameFilterFnFor", function () {
    it("should return true for URIs that map to known base table names", function () {
      var namerFn = ut.tableStrToNamer("/s3:\/\/.*?\/someprefix\/(.*?)\//i")
        , filterFn = ut.nameFilterFnFor(["herp_derp_durr", "pak_chooie_unf"], namerFn)
        , uri = "s3://some-bucket/someprefix/herp.derp.durr/2015-02-02/herp.derp.durr-p-7-2015-02-02-0062210428.txt.gz"
        , uri2 = "s3://some-bucket/someprefix/pak.chooie.unf/2015-02-02/fadsjklajkl"
        ;

      expect(filterFn(uri)).to.be.true;
      expect(filterFn(uri2)).to.be.true;
    });

    it("should return false for URIs that do not map to known base table names", function () {
      var namerFn = ut.tableStrToNamer("/s3:\/\/.*?\/someprefix\/(.*?)\//i")
        , filterFn = ut.nameFilterFnFor(["herp_derp_durr", "pak_chooie_unf"], namerFn)
        , uri = "s3://some-bucket/someprefix/dsa.gfsd.gdst/2015-02-02/herp.derp.durr-p-7-2015-02-02-0062210428.txt.gz"
        ;

      expect(filterFn(uri)).to.be.false;
    });

  });

  describe("tableStrToNamer", function () {

    it("Should handle strings", function () {
      var fn = ut.tableStrToNamer("qux");
      var uri = "s3://some-bucket/someprefix/herp.derp.durr/2015-02-02/herp.derp.durr-p-7-2015-02-02-0062210428.txt.gz";
      expect(fn(uri)).to.equal('qux');
    });

    it("Should handle regexen", function () {
      var fn = ut.tableStrToNamer("/s3:\/\/.*?\/someprefix\/(.*?)\//i");
      var uri = "s3://some-bucket/someprefix/herp.derp.durr/2015-02-02/herp.derp.durr-p-7-2015-02-02-0062210428.txt.gz";
      expect(fn(uri)).to.equal('herp_derp_durr');
    });
  });

  describe("_URIToTbl", function () {
    var re = new RegExp("s3:\/\/.*?\/someprefix\/(.*?)\/");
    var fn = _.partial(ut._URIToTbl, re);
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


  describe("messagesToURIs", function () {
    it("should turn a SQS message array to an URI array", function () {
      var msgs = new tu.SQSMessage(10, "bukkit", "prefix/").Messages;
      var uris = ut.messagesToURIs();
      _.each(uris, function (uri) {
        expect(uri).to.match(/^s3:\/\/bukkit\/prefix\//);
      });
    });

    it("should drop URIs for unparseable messages from the result", function () {
      var msgs = new tu.SQSMessage(10, "bukkit", "prefix/").Messages;
      msgs[0].Body = "(/)&/(&(/&";
      var uris = ut.messagesToURIs(msgs);
      expect(uris).to.have.length(9);
      expect(uris).to.not.contain(null);
      expect(uris).to.not.contain(undefined);
    });

  });

  describe("messageToURIs", function () {
    it("should turn an SQS message to a URI", function () {
      var msg = new tu.SQSMessage(1, "bukkit", "prefix/").Messages[0];
      var uri = ut.messageToURIs(msg)[0];
      expect(uri).to.match(/^s3:\/\/bukkit\/prefix\//);
    });

    it("should return [] for a message with an unparseable body", function() {
      var msg = new tu.SQSMessage(1, "bukkit", "prefix/").Messages[0];
      msg.Body = "!%#=/%€#)";
      var uri = ut.messageToURIs(msg);
      expect(uri).to.deep.equal([]);
    })
  });

  describe("_eventToS3URIs", function () {

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

    it("Should return an empty array when S3 schema version doesn't match", function () {
      event.Records[0].s3.s3SchemaVersion = "3.0";
      expect(ut.eventToS3URIs(event)).to.deep.equal([]);
    });

    it("Should return an empty array when event version doesn't match", function () {
      event.Records[0].eventVersion = "3.0";
      expect(ut.eventToS3URIs(event)).to.deep.equal([]);
    });

    it("Should return an empty array when no records are found", function () {
      expect(ut.eventToS3URIs({Records: []})).to.deep.equal([]);
    });

    it("Should convert S3 events to S3 URIs", function () {
      var uris = ut.eventToS3URIs(event);
      var wanted = "s3://some-bucket-name/someprefix/some.stuff.here/2015-01-30/some.stuff.here-p-9-2015-01-30-0044120221.txt.gz";
      expect(uris[0]).to.equal(wanted);
    });
  });
});