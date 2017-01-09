# S4QS-RS
S4QS-RS reads S3 object creation events from SQS queues and copies data from S3 to Redshift using COPY with S3 manifests (see Redshift's [COPY](http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) documentation for more information.)

See the [S3 documentation](http://docs.aws.amazon.com/AmazonS3/latest/UG/SettingBucketNotifications.html) for more information about setting up the object creation events. Note that if you publish the events to SNS and then subscribe your SQS queue to the SNS topic, you **must** set the "Raw Message Delivery" subscription attribute to "**True**".

Data is copied into monolithic tables. Retention and cleanup of these tables is left up to external tools; S4QS does **not** remove old rows, or vacuum/analyze the tables.

More details about how S4QS-RS works in the [configuration section](#configuration).

## Installation

`npm install -g s4qs-rs`

## Configuration
S4QS-RS is configurable with JSON files located in either the `./config` subdirectory of the current working directory, or at `$NODE_CONFIG_DIR`. `./config/default.json` is always loaded and used for default settings, and `$NODE_ENV` is used to determine environment-specific configuration files: For example `NODE_ENV=production` would cause `./config/production.json` to be loaded and merged with `./config/default.json`.

`NODE_ENV`-specific configuration files will always override the default configuration. If `./config/default.json` is, for example

```json
  {
    "SQS": {
      "params": {
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/12345/prod-queue",
        "MaxNumberOfMessages": 10
      }
    },
    "S3Copier": {
      "other": "stuff here"
    }
  }
```

and `./config/development.json` is

```json
  {
    "SQS": {
      "params": {
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/12345/dev-queue"
      }
    }
  }
```

only `SQS.params.QueueUrl` would be overridden.

You can also use Javascript files as configuration files. See the [config](https://www.npmjs.com/package/config) NPM package documentation for more information.

S4QS-RS defaults to `NODE_ENV=development` if `NODE_ENV` is omitted.

AWS credentials are loaded by the Node AWS SDK. See [the SDK's documentation](http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html) for more information. S4QS-RS needs IAM permissions to perform `redshift:DescribeClusters`.

### Configuration structure as a JSON file

```javascript
{
  // Logging options
  // Optional.
  "logging": {
    // you can have multiple streams, but currently only the stderr and stdout output streams are supported.
    // Optional. Defaults to a minimum level of "trace" and "stdout" output
    "streams": [
          {
            // minimum level.
            "level": "trace",
            // where to output; "stdout" or "stderr"
            "stream": "stdout"
          }
        ]
  },

  // SQS poller options.
  // Can be overridden in tableConfig (see below)
  // Optional.
  "SQS": {
    "poller": {
      // Do this many parallel SQS polls when polling for new messages.
      // Optional, defaults to 1.
      "parallelPolls": 5,

      // Each parallel poll will be done at intervals of pollIntervalSeconds seconds. The interval starts from the
      // finish of the previous poll.
      "pollIntervalSeconds": 2,

      // update SQS message visibility timeout while a message is being processed.
      // Configuration is optional, but updating is always done with default values (see below)
      "visibilityTimeoutUpdater": {
        // visibility timeout in seconds. Defaults to 300 seconds.
        // Optional.
        "visibilityTimeoutSeconds": 300,

        // how often the visibility timeout should be updated. Defaults to 100 seconds.
        // Optional.
        "visibilityUpdateIntervalSeconds": 100,

        // how many failures to allow when updating a message's visibility timeout. After this many tries, a message's
        // timeout will no longer be updated. Defaults to 3.
        "maxFailures": 3
      }
    },

    // SQS region.
    // Required.
    "region": "us-east-1",

    // parameters to pass to SQS. See http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/SQS.html.
    // Optional.
    "params": {
      "WaitTimeSeconds": 20,
      "MaxNumberOfMessages": 10
    }
  },

  "S3Copier": {
    // options for etcd-based pipeline health checker.
    // If the specified etcd key doesn't contain the value in matchAgainst, data finalization time will not be
    // be updated (see the deduplication settings in tableConfig for more information.)
    // Optional.
    "healthCheck": {
      // array of etcd servers to use.
      // Required.
      "etcdServers": ["etcd:4001"],

      // etcd key to use.
      // Required.
      "key": "/s4qs/all_ok",

      // String to match the etcd key against (CASE SENSITIVE.) If the key contains anything but this **or** is not present,
      // S4QS will stop updating the finalization time for all tables.
      // Required.
      "matchAgainst": "OK"
    },
    // AWS Redshift options.
    // Required.

    "Redshift": {
      /*if the Redshift cluster's status goes to anything but "available",
       pause all operation, and try checking its status at intervals of
       clusterAvailCheckInterval. When cluster goes back up, resume
       everything.

       Set this to -1 to exit if the cluster becomes unavailable.

       Optional. Defaults to -1. */
      "clusterAvailCheckInterval": 300,

      // Redshift connection string.
      // Required.
      "connStr": "postgres://username:password@example.com:5439/schema",

      // cluster region.
      // Required.
      "region": "us-east-1",

      // bound parameters. ClusterIdentifier is required, others are optional
      "params": {
        // cluster ID.
        // Required.
        "ClusterIdentifier": "mycluster"
      }
    },

    // AWS S3 constructor options, used by the manifest uploader.
    // Required. ACL must be defined here, other parameters are optional.
    "S3": {
      "params": {
        "ACL": "bucket-owner-full-control"
      }
    },

    // Manifest uploader options.
    // Required.
    "manifestUploader": {
      // Maximum number of URIs to have in each manifest (see maxWaitSeconds)
      // Required.
      "maxToUpload": 25,

      // Upload manifests at intervals of maxWaitSeconds seconds, as long as they have over 0 messages.
      // Required.
      "maxWaitSeconds": 600,

      // Value of "mandatory" property of manifest items.
      // Required.
      "mandatory": true,

      // Bucket to upload manifests to.
      // Required.
      "bucket": "manifest-bucket",

      // Prefix for manifest keys. "In-flight" manifests are stored in bucket/prefix/inflight/YYYY-MM-DD/manifestFileName.json.
      // If the Redshift COPY is successful, manifests are moved to bucket/prefix/successful/, and if the COPY fails they are
      // moved to bucket/prefix/failed/
      // Required.
      "prefix": "s4qs/manifests/",

      // How many times uploads should be retried. Retries have a backoff of 1.5^nRetries * 1000ms, so the first
      // retry will happen after 1000ms, the 2nd after 1500ms, the 3rd after 2250ms, 4th after 3375ms etc.
      // S4QS will throw an exception and exit if the upload fails despite the retries.
      // Optional, will default to 0, meaning S4QS will exit immediately on the first upload error.
      "retries": 5
    },

    // this will be added to the end of the table name when doing COPYs.
    // Useful for having different tables for different NODE_ENVs.
    // Optional.
    "tablePostfix": "_devel",

    // parameters for Redshift's COPY query.
    // Can be overridden in tableConfig (see below)
    // Required.
    "copyParams": {
      // Redshift schema to use.
      // Required
      "schema": "myschema",

      /*
       the "table" property is used to build the Redshift table name. "table" can be either a string
       like "my_table_name" (if you only have one table) or a regular expression (which must start and end with a /).
       The regular expression is given an S3 URI (s3://bucket-name/some/key), and the first capture group
       will be used as the table name.

       When using a regex, periods in the S3 URI are converted to underscores but that's it as far as sanitization for
       Redshift goes. Feed it weird URIs and weird stuff will probably happen.

       The regex, tablePostfix and tableConfig settings in this example would turn URIs like
       s3://bucketname/whatevs/some.table.name/somefilename.csv.gz
       to a table with the name "some_table_name_devel".

       If you use Javascript configuration files, you can specify a function
       for "table". The function must take an S3 URI and output a valid
       Redshift table name.
       */
      "table": "/s3:\/\/.*?\/whatevs\/(.*?)\//i",

      // parameterless arguments to COPY. See http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html.
      // Optional.
      "args": [
        "GZIP",
        "TRUNCATECOLUMNS"
      ],

      // parameters with arguments.
      // Boolean arguments can be either true/false, "true"/"false" or "on"/"off".
      // Can be overridden easily in NODE_ENV-specific configuration.
      // Technically optional, although you'll probably want some.
      "withParams": {
        "REGION": "us-east-1",
        "MAXERROR": 100,
        "TIMEFORMAT": "auto"
      }
    },

    // "Base" table configuration. Table postfixes are added to the base table name.
    // The keys of tableConfig should match table names produced by copyParams.table. The keys are used to match
    // table configurations to incoming S3 files.
    // Required.
    "tableConfig": {

       // if this is set to true, S4QS-RS will check existing tables and their columns against what's in the
       // configuration, and either drop columns if they've been removed from the config, or add columns if they are
       // in the configuration but not in the database.
      "enableSchemaEvolution": true,

      // table configuration for the base table some_table_name.
      // The base table name is extracted from S3 URIs by the regex in the "table" property above
      "some_table_name": {
        // SQS queue parameters for this specific table.
        // The queue **must** contain only events for this specific table.
        // You can also override SQS options like WaitTimeSeconds per base table.
        // Required.
        "SQS": {
          "params": {
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/queue-name"
          }
        },

        // table-specific copyParams overrides.
        // Optional
        "copyParams": {
          "withParams": {
            // use this specific JSONPaths file for this table
            "JSON": "s3://bukkit/jsonpaths_for_some_table_name.json"
          }
        },

        // which column to use as time stamp when e.g. dropping rows that arrived too late, or when deleting old rows
        "timestampCol": "ts",

        // deduplication configuration. If these settings are present, S4QS-RS will deduplicate data
        // using a staging table before copying it to the main table. Deduplication is done based on the PRIMARY KEY
        // column, which must be defined in the "columns" array (see below) and NOT in the tableAttrs.
        // The metadata tables have the prefixes _errors (for either duplicate rows or rows that were too old),
        // _dedup (for primary keys to dedup against. See below), and _tracking (for keeping track of the newest
        // timestamp and latest finalization time. See below.)
        // Optional.
        "deduplication": {
          // only accept rows that have a timestamp that is at most this much older than the newest previous row.
          // Old rows will be moved to the table some_table_name_errors.
          // In practice, this is the finalization time of the data: after dropOlderThan, the data in the table
          // won't change.
          // Required
          "dropOlderThan": "45 minutes",

          // how long IDs should be kept in the deduplication table.
          // Duplicate rows will be moved to the table some_table_name_errors.
          // Required
          "dedupSetAge": "2 hours"
        },

        // Array of column definitions.
        // Required.
        "columns": ["SOURCE INT NOT NULL ENCODE BYTEDICT",  "ID CHAR(24) PRIMARY KEY ENCODE LZO", "..."],

        // Array of table attributes.
        // Required.
        "tableAttrs": ["DISTKEY(ID)", "SORTKEY(ID)"]
      },

      // another base table
      "other_table_name": {
        "SQS": {
          "params": {
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/another-queue-name",
          }
        },

        // table-specific copyParams overrides.
        // Optional
        "copyParams": {
          "withParams": {
            // use this specific JSONPaths file for this table
            "JSON": "s3://bukkit/jsonpaths_for_other_table_name.json"
          }
        },

        "deduplication": {
          "dropOlderThan": "30 minutes",
          "dedupSetAge": "1 hours"
        },

        "columns": ["AHOY INT PRIMARY KEY NOT NULL ENCODE LZO",  "DERR INT ENCODE DELTA", "..."],
        "tableAttrs": ["DISTKEY(AHOY)", "SORTKEY(DERR)"]
      }
    }
  },

  // send metrics to statsd using given prefix.
  // Optional.
  "statsd": {"prefix": "s4qs.", "host": "statsd-server", "port": 1234},

  // send copy metrics to Zabbix. zabbix_sender must be installed. Uses the zabbix-sender NPM package,
  // and this key is passed to the constructor.
  // The keys use the exact same format as statsd keys, including the prefix.
  // With the configuration in this file, keys would look like
  //
  // s4qs.s3-to-rs.connAndCopy.base_table_name.total.time
  //
  // with each base table having its own key. The time reported is the total time in milliseconds taken
  // to COPY and (if enabled) deduplicate data.
  //
  // Optional.
  "zabbix": {"server": "zabbix.example.com", "hostname": "s4qs-rs"}
}
```

## Environment variables
You can [map environment variables to configuration parameters](https://github.com/lorenwest/node-config/wiki/Environment-Variables#custom-environment-variables) by creating a file with the name `./config/custom-environment-variables.json`. For example

```json
{
  "S3Copier": {
    "Redshift": {
      "connStr": "RS_CONN"
    }
  }
}
```

would map `$RS_CONN` to `S3Copier.Redshift.connStr`. Values set with environment variables always override configuration file values.

## Logging
S4QS-RS uses [bunyan](https://github.com/trentm/node-bunyan) for logging. The bundled pretty-printer is good for looking at log output:

```
> npm i -g bunyan
> s4qs-rs|bunyan -oshort
```

See example config for information on how to configure logging.

## Running S4QS-RS
After setting up your configuration, you can run S4QS-RS with `s4qs-rs`. Installing bunyan's pretty-printer is recommended (see [above](#logging))

S4QS-RS traps `SIGTERM` and `SIGINT` and aborts in-flight transactions when terminated.

## Errata

If you `require('s4qs-rs')`, you can use the different components of S4QS-RS separately from the command line tool. See the source code for the documentation, plus have a look at [app.js](app.js).
