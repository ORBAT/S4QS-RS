/**
 * Created by teklof on 17.2.15.
 */

var mup = require('../lib/manifest-uploader');
var tu = require('./test-utils');
var _ = require('lodash');
var chai = require('chai');
var util = require('util');
var Promise = require('bluebird');
require('mocha-sinon');
var expect = chai.expect;
var should = chai.should();

describe("Manifest uploader", function () {

  var manifBucket = "bukkit"
    , manifPrefix = "my-prefix/"
  ;

  function newManifest(mandatory, n, put, del) {
    var s3 = new tu.FakeS3(put, del)
      , manifest = new mup.Manifest(!!mandatory, manifBucket, manifPrefix, s3)
      ;

    n = n || 0;

    manifest.addAll(_.times(n, function () {
      return "s3://some-bucket/" + tu.randomString(8) + ".txt";
    }));

    return  manifest;
  }


  describe("Manifest object", function () {

    it("should give itself a well-formed URI", function () {
      var m = newManifest()
        , re = new RegExp("s3://"+ manifBucket + "/"+ manifPrefix + ".*?\\.json")
        ;
      console.error("manifestURI", m.manifestURI);
      expect(m.manifestURI).to.match(re);
    });

    describe("deleteManifest", function () {

      it("should return a promise of the S3 URI of the deleted manifest", function () {
        var m = newManifest(true, 1, null, {eventName: "success", data: "yay"})
          , deleteObject = this.sinon.spy(m._s3, "deleteObject")
          ;

        return expect(m.delete()).to.eventually.equal(m.manifestURI);
      });

      it("should call S3's deleteObject", function () {
        var m = newManifest(true, 1, null, {eventName: "success", data: "yay"})
          , deleteObject = this.sinon.spy(m._s3, "deleteObject")
        ;

        return expect(m.delete()).to.be.fulfilled.then(function () {
          expect(deleteObject).to.have.been.calledOnce;
          expect(deleteObject).to.have.been.calledWithMatch({Bucket: manifBucket, Key: m._prefix});
        });
      });
    });

    it("should return correct length", function () {
      var m = new mup.Manifest(true);
      m.push("uri1");
      m.push("uri2");
      expect(m.length).to.equal(2);
    });

    it("should push new URIs", function () {
      var m = new mup.Manifest(true);
      expect(m.push("uri1")).to.equal(1);
      expect(m.uris).to.deep.equal(["uri1"]);
    });

    it("should add an array of URIs", function () {
      var m = new mup.Manifest(true);
      m.push("uri0");
      expect(m.addAll(["uri1","uri2", "uri3"])).to.equal(4);
      expect(m.uris).to.deep.equal(["uri0", "uri1", "uri2", "uri3"]);
    });

    it("should generate correct-looking JSON", function () {
      var m = new mup.Manifest(true);
      m.push("uri1");
      m.push("uri2");
      expect(m.toJSON()).to.deep.equal({entries: [{url: "uri1", mandatory: true}, {url: "uri2", mandatory: true}]})
    });

  });


});