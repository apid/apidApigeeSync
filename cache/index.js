'use strict';

var cache = require('volos-cache-memory');
var async = require('async');
var debug = require('debug')('gateway:cache');

var getKey = function (req) {
    return req.token.application_name;
}
var caches = [];

var middleware = function (req, res, next) {
    if (!req.token
        || !req.token.api_product_list
        || !req.token.api_product_list.length
    ) {
        return next();
    }

    debug('cache checking products', req.token.api_product_list);

    req.originalUrl = req.originalUrl || req.url; // emulate connect

    var found = false;
    // this is arbitrary, but not sure there's a better way?
    async.eachSeries(req.token.api_product_list,
        function (productName, cb) {
            if(!found) {
                var cache = caches[productName];
                executeDefaultCache(cache, req, res, cb);
                found = true;
            }else{
                cb();
            }
        },
        function (err) {
            next(err);
        }
    );
}

var executeDefaultCache = function (cache, req, res, cb) {
    cb();
};

module.exports.init = function (config, logger, stats) {
    var options = {
        key: getKey
    };
    Object.keys(config).forEach(function (productName) {
        var product = config[productName];
        if (!product.ttl) {
            debug('could not load cache for ' + productName + ' because ttl is missing', product)
            return; // skip non-cache config
        }
        var cacheInstance = cache.create(config[productName]);
        caches[productName] = cacheInstance;
        debug('created cache for', productName);
        debug('caches are ', caches)
    });

    return {
        onrequest: function (req, res, next) {
            middleware(req, res, next);
        }
    };
};