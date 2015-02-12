/**
 * Created by teklof on 11.2.15.
 */
var express = require('express');

var app = express();

app.get("/check", function (req, res) {
  res.send("OK");
});

exports.app = app;