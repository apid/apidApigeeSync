/// <reference path="../../../typings/node/node.d.ts" />
/// <reference path="../../../typings/mocha/mocha.d.ts" />
/// <reference path="../../../typings/chai/chai.d.ts" />

'use strict';

var helper = require('../../test-helper');
var config = helper.config();

var request = require('supertest');
var chai = require('chai');
var should = chai.should();

chai.config.includeStack = true;
chai.config.showDiff = true;

var expectedRecordKeys = [
  'apiproxy',
  'apiproxy_revision',
  'client_ip',
  'client_received_start_timestamp',
  'client_sent_end_timestamp',
  'recordType',
  'request_path',
  'request_uri',
  'request_verb',
  'response_status_code',
  'useragent'
];

describe('analytics', function() {

  var servers, proxy;

  beforeEach(function() {
    config.analytics = {
      uri: 'x',
      key: 'x',
      proxy: 'x',
      bufferSize: 5,
      batchSize: 5,
      flushInterval: 10000
    };
  });

  afterEach(function() {
    servers.close();
  });

  it('should log forwarded requests', function(done) {

    config.edgemicro.plugins.sequence = ['analytics'];
    helper.startServers(config, function(err, s) {
      if (err) { return done(err); }
      servers = s;
      proxy = servers.proxy;

      var buffer = helper.analytics.buffer;
      var startLen = helper.analytics.buffer.length;

      request(proxy)
        .get('/')
        .expect(200)
        .end(function(err, res) {
          if (err) { return done(err); }
          should.exist(res.body.fromTarget);
          buffer.length.should.equal(startLen + 1);
          var record = buffer[buffer.length - 1];
          record.should.have.keys(expectedRecordKeys);
          record.response_status_code.should.equal(200);

          record.apiproxy.should.eql('microgateway_test');
          record.apiproxy_revision.should.eql('1');

          done();
        });
    });
  });

  it('should log quota failures', function(done) {

    var pem = require('pem');
    var jwt = require('jsonwebtoken');
    var privateKey, publicKey;

    config['quota-memory'] = { // config section should match plugin name
      Test: {
        allow: 1,
        timeUnit: 'minute'
      }
    };

    config.edgemicro.plugins.sequence = ['analytics', 'oauth', 'quota-memory'];
    helper.startServers(config, function(err, s) {
      if (err) { return done(err); }
      servers = s;
      proxy = servers.proxy;

      var buffer = helper.analytics.buffer;
      var startLen = helper.analytics.buffer.length;

        var options = {
          selfSigned: true,
          days: 1
        };
        pem.createCertificate(options, function(err, keys) {
          if (err) { return done(err); }

          privateKey = keys.serviceKey;
          publicKey = keys.certificate;

          config.oauth.public_key = publicKey;

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

              request(proxy)
                .get('/')
                .set('Authorization', 'Bearer ' + token)
                .expect(403)
                .end(function(err, res) {
                  if (err) { return done(err); }
                  should.not.exist(res.body.fromTarget);
                  buffer.length.should.equal(startLen + 2);
                  var record = buffer[buffer.length - 1];
                  record.should.have.keys(expectedRecordKeys);
                  record.response_status_code.should.equal(403);

                  done();
                });
            });
        });
    });
  });

  it('should log spikearrest failures', function(done) {

    config.spikearrest = {
      timeUnit: 'minute',
      bufferSize: 0,
      allow: 1
    };

    config.edgemicro.plugins.sequence = ['analytics', 'spikearrest'];
    helper.startServers(config, function(err, s) {
      if (err) { return done(err); }
      servers = s;
      proxy = servers.proxy;

      var buffer = helper.analytics.buffer;
      var startLen = helper.analytics.buffer.length;

      request(proxy)
        .get('/')
        .expect(200)
        .end(function(err, res) {
          if (err) { return done(err); }
          should.exist(res.body.fromTarget);

          request(proxy)
            .get('/')
            .expect(503)
            .end(function(err, res) {
              if (err) { return done(err); }
              should.not.exist(res.body.fromTarget);
              buffer.length.should.equal(startLen + 2);
              var record = buffer[buffer.length - 1];
              record.should.have.keys(expectedRecordKeys);
              record.response_status_code.should.equal(503);

              done();
            });
        });
    });
  });

  it('should log oauth failures', function(done) {

    config.oauth = {
      allowNoAuthorization: false,
      allowInvalidAuthorization: false,
      publicKey: 'x'
    };

    config.edgemicro.plugins.sequence = ['analytics', 'oauth'];
    helper.startServers(config, function(err, s) {
      if (err) { return done(err); }
      servers = s;
      proxy = servers.proxy;

      var buffer = helper.analytics.buffer;
      var startLen = helper.analytics.buffer.length;

      request(proxy)
        .get('/')
        .expect(401)
        .end(function(err, res) {
          if (err) { return done(err); }
          should.not.exist(res.body.fromTarget);
          buffer.length.should.equal(startLen + 1);
          var record = buffer[buffer.length - 1];
          record.should.have.keys(expectedRecordKeys);
          record.response_status_code.should.equal(401);

          done();
        });
    });
  });

});
