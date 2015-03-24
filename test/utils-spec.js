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