# S4QS-RS
S4QS-RS reads S3 object creation events from SQS and copies data from S3 to Redshift using COPY with S3 manifests (see Redshift's [COPY](http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) documentation for more information.)

See the [S3 documentation](http://docs.aws.amazon.com/AmazonS3/latest/UG/SettingBucketNotifications.html) for more information about setting up the object creation events. Note that if you publish the events to SNS and then subscribe your SQS queue to the SNS topic, you **must** set the "Raw Message Delivery" subscription attribute to "**True**".

Data is copied into time series tables with configurable rotation periods and retention, and multiple rolling views with different periods can be created. View creation is done so that adding or removing columns from time series tables doesn't break the view; missing columns are filled in with `NULL as [missing_col_name]` in the view queries.

More details about how S4QS-RS works in the [configuration section](#configuration).

## Installation

`npm install -g s4qs-rs`

## Configuration
S4QS-RS is configurable with JSON files located in either the `./config` subdirectory of the current working directory, or at `$NODE_CONFIG_DIR`. `./config/default.json` is always loaded and used for default settings, and `$NODE_ENV` is used to determine environment-specific configuration files: For example `NODE_ENV=production` would cause `./config/production.json` to be loaded.

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
  // S4QS-RS will start a HTTP server at this port, with the route /check
  // which returns the string OK. Omit to disable.
  "HTTPPort": 9999,
  // SQS poller options. 
  // Optional.
  "SQS": {
    "poller": {
      // repeat the poll this many times when fetching new messages from SQS.
      // You'll get at most repeatPoll * MaxNumberOfMessages messages at once.
      // Optional, defaults to 1.
      "repeatPoll": 5
    },
    // SQS region.
    // Required.
    "region": "us-east-1",
    // parameters to pass to SQS. See http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/SQS.html.
    // Required, and at least QueueUrl must be present.
    "params": {
      "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/some-queue-name",
      "WaitTimeSeconds": 20,
      "MaxNumberOfMessages": 10
    }
  },

  "S3Copier": {
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
      // Prefix for manifest keys.
      // Required.
      "prefix": "s4qs/manifests/"
    },
    // LRU cache options. Used for message deduplication.
    // Optional. See https://github.com/isaacs/node-lru-cache for possible options.
    "LRU": {
      "max": 1000
    },
    // how often to poll for new messages from SQS.
    // Required.
    "pollIntervalSeconds": 300,
    // this will be added to the end of the table name when doing COPYs.
    // Useful for having different tables for different NODE_ENVs.
    // Optional.
    "tablePostfix": "_devel",

    // parameters for Redshift's COPY query.
    // Required.
    "copyParams": {
      // Redshift schema to use
      "schema": "myschema",
      /* 
       the "table" property is used to build the "base" of the Redshift table name. table can be either a string
       like "my_table_name" or a regular expression (which must start and end with a /).
       The regular expression is given an S3 URI (s3://bucket-name/some/key), and the first capture group 
       will be used as the table name.

       When using a regex, periods in the S3 URI are converted to underscores but that's it as far as sanitization goes.
       Feed it weird URIs and weird stuff will probably happen.

       The regex, tablePostfix and timeSeries settings in this example would turn URIs like
       s3://bucketname/whatevs/some.table.name/somefilename.csv.gz
       to time series tables that have names like "some_table_name_devel_ts_1428883200",
       and the rolling view would have the name "some_table_name_view_devel".

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
      // parameters with arguments to COPY. 
      // Boolean arguments can be either true/false, "true"/"false" or "on"/"off".
      // Can be overridden easily in NODE_ENV-specific configuration.
      // Technically optional, although you'll probably want some.
      "withParams": {
        "DELIMITER": "\\t",
        "REGION": "us-east-1",
        "MAXERROR": 100,
        "NULL": "null",
        "TIMEFORMAT": "auto"
      }
    },
    /* 
     S4QS-RS copies data into time series tables with a configurable time period.
     A UNION ALL + SELECT view of the time series tables is created, and it is updated every time a new time
     series table is created. A configurable amount of old tables are retained, and old tables are dropped 
     when needed.

     The keys of timeSeries should match table names produced by copyParams.table. The keys are used to match 
     time series table configuration to incoming S3 files.

     See e.g. http://docs.aws.amazon.com/redshift/latest/dg/vacuum-time-series-tables.html for more information
     on the concept.

     Required. Sub-object properties are required unless otherwise noted.
     */
    "timeSeries": {
      // time series table configuration for the table some_table_name (extracted from S3 URI by regex in the
      // "table" property above)
      "some_table_name": {
        // Time series table period in seconds. A value of e.g. 86400 would mean that a new time series table is 
        // created per every day
        "period": 86400,
        // Keep a maximum of maxTables time series tables. Oldest tables will be deleted first
        "maxTables": 30,
        // How many rolling views to create, and how many tables each view
        // should contain.
        // The example would create 4 views with lengths 1, 5, 15 and 30.
        // Can be either an array or a single number.
        // Optional. Will default to maxTables if omitted.
        "tablesInView": [1, 5, 15, 30],
        // Array of column definitions
        "columns": ["SOURCE INT NOT NULL ENCODE BYTEDICT",  "ID CHAR(24) ENCODE LZO DISTKEY", "..."],
        // Array of table attributes
        "tableAttrs": ["DISTKEY(ID)", "SORTKEY(ID)"]
      },
      "other_table_name": {
        "period": 86400,
        "maxTables": 30,
        "tablesInView": [1, 5, 15, 30],
        "columns": ["AHOY INT NOT NULL ENCODE LZO",  "DERR INT ENCODE DELTA DISTKEY", "..."],
        "tableAttrs": ["DISTKEY(AHOY)", "SORTKEY(DERR)"]
      }
    }
  }
}
```

## Environment variables
S4QS-RS uses the [debug NPM package](npmjs.org/package/debug) for outputting debug messages to stdout and error messages to stderr. To see **any** error messages, you **must** run S4QS-RS with the env `DEBUG=s4qs-rs:*:error`. To see debug messages, run with `DEBUG=s4qs-rs:*` See debug's documentation for more information.

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

## Running S4QS-RS
After setting up your configuration, you can run S4QS-RS with `DEBUG=*:error s4qs-rs` or `DEBUG=sq4s-rs:* s4qs-rs` to see debugging information.

S4QS-RS traps `SIGTERM` and `SIGINT` to make sure in-flight copies complete and the corresponding SQS messages and S3 manifests are deleted. This may take several minutes depending on what you're copying, so don't be alarmed if nothing seems to happen.

## Errata

If you `require('s4qs-rs')`, you can use the different components of S4QS-RS separately from the command line tool. See the source code for the documentation, plus have a look at [app.js](app.js).
