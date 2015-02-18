# S4QS-RS
S4QS-RS reads S3 object creation events from SQS and copies data from S3 to Redshift using COPY. S4QS-RS gathers files
into S3 manifests, which are then used for the COPY. See Redshift's [COPY](http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html)
documentation for more information.

See the [S3 documentation](http://docs.aws.amazon.com/AmazonS3/latest/UG/SettingBucketNotifications.html) for more information about setting up the events. Note that if you publish the events to SNS and then subscribe your SQS queue to the SNS topic, you **must** set the "Raw Message Delivery" subscription attribute to "**True**".

## Installation

`npm install -g s4qs-rs`

## Configuration
S4QS-RS is configurable with JSON files located in either the `./config` subdirectory of the current working directory, or at `$NODE_CONFIG_DIR`. `./config/default.json` is always loaded and used for default settings, and `$NODE_ENV` is used to determine environment-specific configuration files: for example `NODE_ENV=production` would cause `./config/production.json` to be loaded.

You can also use Javascript files as configuration files. See the [config](https://www.npmjs.com/package/config) NPM package documentation for more information.

S4QS-RS defaults to `NODE_ENV=development` if `NODE_ENV` is omitted.

AWS credentials are loaded by the Node AWS SDK. See [the SDK's documentation](http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html) for more information.

### Configuration structure as a JSON file

```javascript
{
  // S4QS-RS will start a HTTP server at this port, with the route /check
  // which returns the string OK. Omit to disable.
  "HTTPPort": 30117,
  "SQS": {
    // SQS poller options. 
    // Optional.
    "poller": {
      // repeat the poll this many times when fetching new messages from SQS.
      // Optional, defaults to 1.
      "repeatPoll": 5
    },

    // how often to poll for new messages from SQS.
    // Required.
    "pollIntervalSeconds": 30,
     // SQS region.
     // Required.
    "region": "us-east-1",
    // parameters to pass to SQS. See http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/SQS.html.
    // Required.
    "params": {
      "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789/some-queue-name",
      "WaitTimeSeconds": 20, 
      "MaxNumberOfMessages": 10
    }
  },

  "S3Copier": {
    // AWS S3 constructor options.
    // Required.
    "S3": {
      "region": "us-west-2",
      "params": {
        "ACL": "bucket-owner-full-control"
      }
    },
    // Manifest uploader options.
    // Required.
    "manifestUploader": {
      // Minimum number of URIs to have in each manifest (see maxWaitTime)
      // Required.
      "minToUpload": 16,
      // Upload manifest at intervals of maxWaitTime milliseconds, regardless of the amount of messages in them.
      // Required.
      "maxWaitTime": 300000,
      // Value of "mandatory" property of manifest items.
      // Required.
      "mandatory": true,
      // Bucket to upload manifests to.
      // Required.
      "bucket": "manifest-bucket",
      // Prefix for manifest keys.
      // Required.
      "prefix": "some-prefix/"
    },

    // LRU cache options. Used for message deduplication.
    // Optional. See https://github.com/isaacs/node-lru-cache for possible options.
    "LRU": {
      "max": 1000
    },
      // Redshift connection string.
      // Required.
    "connStr": "postgres://username:password@example.com:5439/schema",
    
    // this will be added to the end of the table name when doing COPYs.
    // Useful for having different tables for different NODE_ENVs.
    // Optional.
    "tablePostfix": "_devel",

    "copyParams": {
      /* 
        the "table" property is used to set the Redshift table name. This can be either a string
        like "my_table_name" or a regular expression (which must start and end with a /).
        When using a regex, periods are converted to underscores but that's it.
        Feed S4QS-RS weird URIs and weird stuff will probably happen. You have been warned.
        
        The example regex and tablePostfix combination would turn an URI like
        s3://bucketname/whatevs/this.will.be.the.table.name/qwerasdf.csv.gz
        to this_will_be_the_table_name_devel.

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
      // Technically optional, although you'll probably want some.
      "withParams": {
        "DELIMITER": "\\t",
        "REGION": "us-east-1",
        "MAXERROR": 100,
        "NULL": "null",
        "TIMEFORMAT": "auto"
      }
    }
  }
}
```

## Environment variables
S4QS-RS uses the [debug NPM package](npmjs.org/package/debug) for outputting debug messages to stdout and error messages to stderr. To see **any** error messages, you **must** run S4QS-RS with the env `DEBUG=*:error`. To see debug messages, run with `DEBUG=s3-to-rs*,sqs-poller*,s4qs-app*` (or a combination thereof.) See debug's documentation for more information.

You can [map environment variables to configuration parameters](https://github.com/lorenwest/node-config/wiki/Environment-Variables#custom-environment-variables) by creating a file with the name `./config/custom-environment-variables.json`. For example

```json
{
  "S3Copier": {
    "connStr": "RS_CONN"
  }
}
```

would map `$RS_CONN` to `S3Copier.connStr`. Values set with environment variables always override configuration file values.

## Running S4QS-RS
After setting up your configuration, you can run S4QS-RS with `DEBUG=*:error s4qs-rs`.

S4QS-RS traps `SIGTERM` and `SIGINT` to make sure in-flight copies complete and the corresponding SQS messages are deleted. This may take several minutes depending on what you're copying, so don't be alarmed if nothing seems to happen.

## Errata

If you `require('s4qs-rs')`, you can use the two components of S4QS-RS, the S3 -> Redshift copier [`S3Copier`](lib/s3-to-rs.js) and the SQS poller [`Poller`](/lib/sqs-poller.js) separately from the command line tool. See the source code for the documentation, plus have a look at [app.js](app.js).