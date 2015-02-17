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

  var bucket = "bukkit"
    , prefix = "my-prefix/"
  ;

  function newManifest(mandatory, n) {

    var manifest = new mup.Manifest(mandatory);

    manifest.addAll(_.times(n, function () {
      return "s3://" + bucket + "/" + prefix + tu.randomString(8) + ".txt";
    }));

    return  manifest;
  }


  describe("Manifest object", function () {
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
      expect(m.addAll(["uri1","uri2", "uri3"])).to.equal(3);
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