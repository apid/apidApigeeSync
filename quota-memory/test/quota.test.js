/// <reference path="../../../typings/node/node.d.ts" />
/// <reference path="../../../typings/mocha/mocha.d.ts" />
/// <reference path="../../../typings/chai/chai.d.ts" />

'use strict';

var helper = require('../../test-helper');
var config = helper.config();

var request = require('supertest');
var chai = require('chai');
var should = chai.should();

var pem = require('pem');
var jwt = require('jsonwebtoken');

chai.config.includeStack = true;
chai.config.showDiff = true;

describe('quota-memory', function() {

  var privateKey, publicKey;
  var servers, proxy;

  before(function(done) {
    var options = {
      selfSigned: true,
      days: 1
    };
    pem.createCertificate(options, function(err, keys) {
      if (err) { return done(err); }

      privateKey = keys.serviceKey;
      publicKey = keys.certificate;

      config.oauth.public_key = publicKey;

      done();
    });
  });

  before(function(done) {
    config['quota-memory'] = { // config section must match plugin name
      Test2: { // product name
        allow: 1,
        interval: 2,
        timeUnit: 'month',
        bufferSize: 10000
      },
      Test: {
        allow: 1,
        interval: 1,
        timeUnit: 'minute',
        bufferSize: 10000
      }
    };

    config.edgemicro.plugins.sequence = ['oauth', 'quota-memory'];
    helper.startServers(config, function(err, s) {
      if (err) { return done(err); }
      servers = s;
      proxy = servers.proxy;
      done();
    });
  });

  after(function() {
    servers.close();
  });


  it('should succeed before limit is hit', function(done) {

    var options = { algorithm: 'RS256' };
    var payload = { test: 'test', api_product_list: [ 'Test' ] };
    var token = jwt.sign(payload, privateKey, options);

    request(proxy)
      .get('/')
      .set('Authorization', 'Bearer ' + token)
      .expect(200)
      .end(function(err, res) {
        if (err) { return done(err); }
        should.exist(res.body.fromTarget);
        done();
      });
  });

  it('should fail once limit is hit', function(done) {

    var options = { algorithm: 'RS256' };
    var payload = { test: 'test', api_product_list: [ 'Test' ] };
    var token = jwt.sign(payload, privateKey, options);

    request(proxy)
      .get('/')
      .set('Authorization', 'Bearer ' + token)
      .expect(403)
      .end(function(err, res) {
        if (err) { return done(err); }
        should.not.exist(res.body.fromTarget);
        done();
      });
  });

  it('should never fail if no token', function(done) {

    request(proxy)
      .get('/')
      .expect(200)
      .end(function(err, res) {
        if (err) { return done(err); }
        should.exist(res.body.fromTarget);
        done();
      });
  });

  it('should apply to all products', function(done) {

    // we should have 1 more hit left on Test2, eat it then check for error

    var options = { algorithm: 'RS256' };
    var payload = { test: 'test', api_product_list: [ 'Test2' ] };
    var token = jwt.sign(payload, privateKey, options);

    request(proxy)
      .get('/microgateway_test2')
      .set('Authorization', 'Bearer ' + token)
      .expect(200)
      .end(function(err, res) {
        if (err) { return done(err); }
        should.exist(res.body.fromTarget);

        request(proxy)
          .get('/microgateway_test2')
          .set('Authorization', 'Bearer ' + token)
          .expect(403)
          .end(function(err, res) {
            if (err) { return done(err); }
            should.not.exist(res.body.fromTarget);
            done();
          });
      });
  });
});
