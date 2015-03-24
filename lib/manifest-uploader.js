/**
 * Created by teklof on 17.2.15.
 */
var ut = require('./utils');
var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');
var util = require('util');
var Promise = require('bluebird');
var debug = require('debug')('s4qs-rs:manifest');
var error = require('debug')('s4qs-rs:manifest:error');
var $ = require('highland');
error.log = console.error;

var inspect = _.partialRight(util.inspect, {depth: 10});


/**
 * Bundles SQS messages containing S3 object creation events into manifest files. Emits 'manifest' events with
 * uploaded manifests, represented by Manifest instances.
 *
 * Manifest URIs will be of the form s3://[bucket]/[prefix][Redshift table name]-[timestamp]-[5 random letters].json
 *
 * where [prefix] can be anything that's kosher for an S3 key.
 *
 * @param {Object} s3 An initialized AWS S3 object. The params passed to its constructor <i>must</i> include
 * ACL
 * @param {Number} options.minToUpload minimum amount of files to include in each manifest
 * @param {Number} options.maxWaitSeconds send a manifest after maxWaitSeconds seconds, even if the number
 * of files is smaller than options.minToUpload
 * @param {Boolean} options.mandatory The value of the "mandatory" property of each manifest entry. See Redshift
 * COPY documentation http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
 * @param {String} options.bucket S3 bucket to upload manifests to
 * @param {String} options.prefix Key prefix to use for manifests
 * @param {Function} options.grouper Function that takes an S3 URI and turns it into an
 * array of table names (S3 events have a Records array which can ostensibly contain more than one record).
 * Used for grouping SQS messages into the correct manifest.
 * @constructor
 * @type {Uploader}
 */
var Uploader = exports.Uploader = function Uploader(s3, options) {
  EventEmitter.call(this);

  if (!s3) {
    throw new Error("s3 required");
  }

  if (!options) {
    throw new Error("No options provided");
  }

  if (!options.grouper) {
    throw new Error("options.grouper required");
  }

  if (!options.minToUpload) {
    throw new Error("options.minToUpload required");
  }

  if (!options.maxWaitSeconds) {
    throw new Error("options.maxWaitSeconds required");
  }

  if (!options.bucket) {
    throw new Error("no bucket specified");
  }

  if (!options.prefix) {
    throw new Error("no key prefix specified");
  }

  if (_.isUndefined(options.mandatory)) {
    throw new Error("options.mandatory required");
  }

  this._grouper = options.grouper;
  this._mandatory = options.mandatory;
  this._minToUpload = options.minToUpload;
  this._maxWaitSeconds = options.maxWaitSeconds;
  this._bucket = options.bucket;
  this._prefix = options.prefix;
  this._s3 = s3;
  this._manifestGroups = {};

  // TODO: add property with in-progress upload promises, return it from stop()

};

util.inherits(Uploader, EventEmitter);

Uploader.prototype.stop = function stop() {
  if(this._interval) {
    debug("Stopping");
    clearInterval(this._interval);
  }
};

Uploader.prototype.start = function start() {
  // TODO: implement per-manifest maxWaitSeconds instead of having a "global" interval
  this._interval = setInterval(this._uploadCurrent.bind(this), this._maxWaitSeconds * 1000);
  this._interval.unref();
};

Uploader.prototype._newManifest = function _newManifest(group) {
  var manifest = new Manifest(this._s3,
    {mandatory: this._mandatory, bucket: this._bucket, prefix: this._prefix, table: group});
  debug("Created new manifest for " + group);
  this._manifestGroups[group] = manifest;
};

Uploader.prototype._addToGroup = function _addToGroup(msgs, grp) {
  if (!this._manifestGroups[grp]) {
    this._newManifest(grp);
  }
  var manifestGroup = this._manifestGroups[grp];

  manifestGroup._addAll(msgs);
  if(manifestGroup.length >= this._minToUpload) {
    debug("Group " + grp + " should be uploaded. URI count " + manifestGroup.length + "/" + this._minToUpload);
    this._uploadGroup(grp);
  }
};

Uploader.prototype.addMessages = function addMessages(msgs) {
  var self = this;

  var grouped = _.reduce(msgs, function (acc, msg) {
    var uris = _messageToURIs(msg)
      , groups = _.map(uris, self._grouper)
      ;
    _.each(groups, function (group) {
      acc[group] = (acc[group] || []).concat(msg)
    });
    return acc;
  }, {});

  _.each(grouped, function (msgs, grp) {
    self._addToGroup(msgs, grp);
  });

};

Uploader.prototype._uploadGroup = function _uploadGroup(group) {
  var manif = this._manifestGroups[group]
    ;

  if(!manif) {
    error("_uploadGroup for nonexistent group " + group);
    return Promise.resolve();
  }

  if(EventEmitter.listenerCount(this, 'manifest') < 1) {
    error("_uploadGroup but no listeners on 'manifest'?");
    return Promise.resolve();
  }


  // a new manifest needs to be created immediately since we don't want addMessages to keep adding messages to
  // a group that's already uploading
  this._newManifest(group);

  if(manif.length > 0) {
    debug("Uploading manifest for group " + group + ", URI " + manif.manifestURI);
    return manif._upload()
      .bind(this)
      .then(function (mf) {
        // TODO: check if 'manifest' has listeners and if it doesn't, then delete the uploaded manifest immediately
        if(EventEmitter.listenerCount(this, 'manifest') > 0) {
          this.emit('manifest', mf);
          return null;
        } else {
          error("Uploaded manifest " + mf.manifestURI + " but nobody's listening to 'manifest'? Deleting it");
          return mf.delete();
        }
      })
      .catch(function (err) {
        this.emit('error', err, manif);
      });
  }
  return Promise.resolve();
};

/**
 * Will upload all non-empty manifests. Will emit successfully uploaded manifests with
 * event type 'manifest', and errors with type 'error'.
 * @private
 */
Uploader.prototype._uploadCurrent = function _uploadCurrent() {
  _.each(this._manifestGroups, function (manif, grp) {
    if(manif.length > 0) {
      debug("_uploadCurrent should upload " + grp);
      this._uploadGroup(grp);
    }
  }.bind(this));
};

/**
 * A Manifest contains S3 URIs/SQS messages and can be turned into a manifest file for Redshift's COPY.
 * The manifestURI and table properties are meant for users of Manifest, all other properties are intended to be
 * private.
 * @constructor
 * @param {Object} s3 Initialized S3 client
 * @param {Boolean} options.mandatory if true, all URIs will have mandatory:true in the manifest
 * @param {String} options.bucket Bucket to upload to
 * @param {String} options.prefix Key prefix to use
 * @type {Manifest}
 */
var Manifest = exports.Manifest = function Manifest(s3, options) {
  this._mandatory = !!options.mandatory;
  this._uris = [];
  this._msgs = [];
  this._s3 = s3;
  this._bucket = options.bucket;
  // the table this manifest is meant for
  this.table = options.table;

  this._key = options.prefix + this.table + '-' + Date.now().toString() + '-' + ut.randomString(5) + ".json";
  // this is the S3 URI of this manifest
  this.manifestURI = "s3://" + this._bucket + "/" + this._key
};

/**
 * Uploads the Manifest to S3, returns a promise that will be fulfilled with either the Manifest instance once the
 * upload completes, or rejected with whatever error S3 gave.
 *
 * @return {Promise} Promise of Manifest or S3 error
 * @private
 */
Manifest.prototype._upload = function _upload() {
  var self = this;
  return new Promise(function (resolve, reject) {
    var req = self._s3.putObject({ContentType: "application/json",
      Bucket: self._bucket, Key: self._key, Body: JSON.stringify(self.toJSON())});

    req.on('success', function () {
      debug("Uploaded manifest " + self.manifestURI);
      resolve(self);
    });
    req.on('error', function(err) {
      error("Error uploading manifest " + self.manifestURI +": " + err);
      reject(err);
    });
    req.send();
  });
};

/**
 * Deletes this manifest from S3 and returns a promise that'll be fulfilled with the deleted manifest's URI, or rejected
 * with S3's error in case of deletion error.
 * result.
 * @return {Promise} Promise of deletion result
 */
Manifest.prototype.delete = function delete_() {
  var self = this;
  return new Promise(function (resolve, reject) {
    debug("Deleting manifest " + self.manifestURI);
    var req = self._s3.deleteObject({Bucket: self._bucket, Key: self._key});
    req.on('success', function () {
      debug("Deleted manifest " + self.manifestURI);
      resolve(self.manifestURI);
    });
    req.on('error', function(err) {
      error("Error deleting manifest " + self.manifestURI + ": " + err);
      reject(err, self);
    });
    req.send();
  });
};

/**
 * Number of URIs in the Manifest
 */
Object.defineProperty(Manifest.prototype, "length", {
  get: function () {return this._uris.length}, set: function (n) {this._uris.length = n}
});

/**
 * Array of SQS messages in the Manifest
 */
Object.defineProperty(Manifest.prototype, "msgs", {
  get: function () {return _.clone(this._msgs)}
});

/**
 * Array of URIs in the Manifest
 */
Object.defineProperty(Manifest.prototype, "uris", {
  get: function () {return _.clone(this._uris)}
});

/**
 * Turn a Manifest into Redshift COPY manifest JSON
 * @return {{entries: *}}
 */
Manifest.prototype.toJSON = function toJSON() {
  var self = this;
  return {entries: _.reduce(this._uris, function (acc, uri) {
    return acc.concat({url: uri, mandatory: self._mandatory});
  }, [])};
};

/**
 * Adds SQS messages containing S3 events to the Manifest
 * @param {Array} msgs Messages to add
 * @return {Number} new amount of URIs in the Manifest
 * @private
 */
Manifest.prototype._addAll = function _addAll(msgs) {
  if (!msgs || msgs.length == 0) {
    return this._uris.length;
  }
  this._msgs = this._msgs.concat(msgs);
  this._uris = this._uris.concat(_messagesToURIs(msgs));
  return this._uris.length;
};

/**
 * Add SQS message to the Manifest
 * @param {String} msg S3 URI to add
 * @return {Number} new amount of URIs in the Manifest
 * @private
 */
Manifest.prototype._push = function _push(msg) {
  this._msgs.push(msg);
  this._uris = this._uris.concat(_messagesToURIs([msg]));
  return this._uris.length;
};

var _messageToURIs = ut.messageToURIs;

var _messagesToURIs = ut.messagesToURIs;