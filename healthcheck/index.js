'use strict';

var toobusy = require('toobusy-js');
var debug = require('debug')('gateway:healthcheck');

const HEALTHCHECK_URL = '/healthcheck';

module.exports.init = function(config, logger, stats) {
  return {
   onrequest: function(req, res, next) {
     var healthcheck_url = config['healthcheck_url'] ||  HEALTHCHECK_URL
      if(healthcheck_url == req.url) {
        var statusCode = (this.toobusy() ? 503 : 200)
        debug(statusCode)
        var healthInfo = {
          memoryUsage: process.memoryUsage(),
          cpuUsage: process.cpuUsage(),
          uptime: process.uptime(),
          pid: process.pid
        }
        res.writeHead(statusCode, { 'Content-Type': 'application/json' })
        res.write(JSON.stringify(healthInfo))
        res.end()
      }
      else {
        next()
      }
    }
  }
}
