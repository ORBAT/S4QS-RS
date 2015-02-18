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
 * @param {Number} options.maxWaitTime send a manifest after maxWaitTime milliseconds, even if the number
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

  if (!options.maxWaitTime) {
    throw new Error("options.minToUpload required");
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
  this._maxWaitTime = options.maxWaitTime;
  this._bucket = options.bucket;
  this._prefix = options.prefix;
  this._s3 = s3;
  this._manifestGroups = {};
};

util.inherits(Uploader, EventEmitter);

Uploader.prototype.start = function start() {
  this._interval = setInterval(this._uploadCurrent.bind(this), this._maxWaitTime);
  this._interval.unref();
};

Uploader.prototype._newManifest = function _newManifest(group) {
  var manifest = new Manifest(this._s3,
    {mandatory: this._mandatory, bucket: this._bucket, prefix: this._prefix, table: group});
  debug("Created new manifest with URI " + manifest.manifestURI);
  return  manifest;
};

Uploader.prototype._addToGroup = function _addToGroup(msgs, grp) {
  if (!this._manifestGroups[grp]) {
    this._manifestGroups[grp] = this._newManifest(grp);
  }
  var manifestGroup = this._manifestGroups[grp];

  manifestGroup._addAll(msgs);
  if(manifestGroup.length >= this._minToUpload) {
    debug("Group " + grp + " should be uploaded");
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
    , self = this
    ;
  if(!manif) {
    error("_uploadGroup for nonexistent group " + group);
    return;
  }

  // a new manifest needs to be created immediately since we don't want addMessages to keep adding messages to
  // a group that's already uploading
  this._manifestGroups[group] = this._newManifest(group);

  if(manif.length > 0) {
    debug("Uploading manifest for group " + group + ", URI " + manif.manifestURI);
    manif._upload()
      .then(function (mf) {
        debug("Upload of " + mf.manifestURI + " done");
        self.emit('manifest', mf);
      })
      .catch(function (err) {
        error("Error uploading manifest with URI " + manif.manifestURI + ": " + err);
        self.emit('error', err, manif);
      });
  }
};

/**
 * Will upload all non-empty manifests. Will emit successfully uploaded manifests with
 * event type 'manifest', and errors with type 'error'.
 * @private
 */
Uploader.prototype._uploadCurrent = function _uploadCurrent() {
  _.each(this._manifestGroups, function (manif, grp) {
    throw new Error("not done yet");
  });

  /*  if(this._currentManifest.length == 0) {
   return;
   }

   var self = this
   , manif = this._currentManifest
   ;

   debug("Starting upload of manifest " + manif.manifestURI);

   this._currentManifest = this._newManifest();

   manif._upload()
   .then(function(mf) {
   debug("Upload of " + mf.manifestURI + " done");
   self.emit('manifest', mf);
   })
   .catch(function (err) {
   error("Error uploading manifest with URI " + self._currentManifest.manifestURI + ": " + err);
   self.emit('error', err);
   })
   */
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
      resolve(self);
    });
    req.on('error', reject);
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
    var req = self._s3.deleteObject({Bucket: self._bucket, Key: self._key});
    req.on('success', function () {
      resolve(self.manifestURI);
    });
    req.on('error', reject);
    req.send();
  });
};

/**
 * Number of URIs in the Manifest
 */
Object.defineProperty(Manifest.prototype, 'length', {
  get: function () {return this._uris.length}, set: function (n) {this._uris.length = n}
});

/**
 * Array of SQS messages in the Manifest
 */
Object.defineProperty(Manifest.prototype, 'msgs', {
  get: function () {return _.clone(this._msgs)}
});

/**
 * Array of URIs in the Manifest
 */
Object.defineProperty(Manifest.prototype, 'uris', {
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

/**
 * Takes an S3 event and returns an array of S3 URIs contained in the Records field.
 * @param {Object} evt S3 event
 * @return {Array} S3 URI strings
 */
var _eventToS3URIs = exports._eventToS3URIs = function _eventToS3URIs(evt) {
  if (!evt) {
    return [];
  }

  if (evt.Type === "Notification") {
    error("Suspicious SQS message. Are you sure the SNS message delivery is set to raw?");
    return [];
  }

  if (_.isArray(evt.Records) && evt.Records.length > 0) {
    return _.reduce(evt.Records, function (acc, record) {
      var s3 = record.s3;
      if (record.eventVersion == "2.0" && s3 && s3.s3SchemaVersion == "1.0") {
        acc.push("s3://" + s3.bucket.name + "/" + s3.object.key)
      } else {
        error("Unknown event (" + record.eventVersion + ") or S3 event version (" +
              ((s3 && s3.s3SchemaVersion) || "N/A") + ")");
      }
      return acc;
    }, []);
  }
  return [];
};


function tryParse(msg) {
  try {
    return JSON.parse(msg.Body);
  } catch (e) {
    error('Error parsing message "' + msg + '": ' + e);
    return null;
  }
}

/**
 * A function that takes an SQS message and turns it into an array of S3 URIs.
 * @type {Function}
 * @private
 */
var _messageToURIs = exports._messageToURIs = _.flow(tryParse, _eventToS3URIs);

/**
 * A function that takes an array of SQS messages, compacts it (removes falsey values), tries to parse the message
 * body of each message, extracts the S3 URI from each message and then finally flattens the resulting array
 * @type {Function}
 * @private
 */
var _messagesToURIs = exports._messagesToURIs = _.flow(_.compact, ut.splat(_messageToURIs), _.flatten);