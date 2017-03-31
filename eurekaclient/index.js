'use strict';
/**
 * This plugin integrate with Eureka and gets the target
 * endpoint dynamically. 
 */

var debug = require('debug')('plugin:eurekeclient');
var util = require('util');
var os = require('os');
var config = require('./eureka-client.json');

const lookup = require('./servicelookup.json');
const port = 8000;
const Eureka = require('eureka-js-client').Eureka;


function getIPAddr() {
  var interfaces = os.networkInterfaces();
  var addresses = [];
  for (var k in interfaces) {
      for (var k2 in interfaces[k]) {
          var address = interfaces[k][k2];
          if (address.family === 'IPv4' && !address.internal) {
              addresses.push(address.address);
          }
      }
  }
  return addresses[0];  
}

config.instance.hostName = os.hostname();
config.instance.ipAddr = getIPAddr();
config.instance.port["$"] = port;
config.instance.port["@enabled"] = true;
config.instance.dataCenterInfo["@class"] = "com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo";

const client = new Eureka(config);

client.start();

module.exports.init = function (config, logger, stats) {

  function getAppName(url) {
    for (var index in lookup) {
      if (url.includes(lookup[index].uri) || url == lookup[index].uri) {
        return {
                  app: lookup[index].app,
                  secure: lookup[index].secure
               };
      }
    }
    return "";
  }

  function getTarget(app, secure) {
    var instances = client.getInstancesByAppId(app);

    for (var index in instances) {
      if (instances[index].status == "UP") {
        return (secure == true) ? {"hostName": instances[index].hostName, "port": instances[index].securePort["$"]} : {"hostName": instances[index].hostName, "port":instances[index].port["$"]};
      }
    }
    return "";
  }

  return {
    onrequest: function(req, res, next) {
      var appInfo = getAppName(req.url);
      var endpoint = getTarget(appInfo.app, appInfo.secure);

      req.targetHostname = endpoint.hostName;
      req.targetPort = endpoint.port;
      req.targetPath = req.url;
      if (appInfo.secure) {
        req.targetSecure = true;
      } else {
        req.targetSecure = false;
      }
      next();
    }   
  };
}
