'use strict';

var debug = require('debug')('plugin:analytics');
var volos = require('volos-analytics-apigee');
module.exports.init = function(config, logger, stats) {

  config.finalizeRecord = function finalizeRecord(req, res, record, cb) {
    if (res.proxy) {
      record.apiproxy = res.proxy.name;
      record.apiproxy_revision = res.proxy.revision;
    }

    if(config.mask_request_uri) {
      record.request_uri = config.mask_request_uri;  
    }

    if(config.mask_request_path) {
      record.request_path = config.mask_request_path;
    }
    

    var xffHeader = req.headers['x-forwarded-for'];
    if(xffHeader) {
      record.client_ip = xffHeader;
    }

    cb(null, record);
  };

  var analytics = volos.create(config);
  var middleware = analytics.expressMiddleWare().apply();

  return {

    testprobe: function() { return analytics },

    onrequest: function(req, res, next) {
      middleware(req, res, next);
    }

  };

}
