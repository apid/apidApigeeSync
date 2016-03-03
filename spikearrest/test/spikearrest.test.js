/// <reference path="../../../typings/node/node.d.ts" />
/// <reference path="../../../typings/mocha/mocha.d.ts" />
/// <reference path="../../../typings/chai/chai.d.ts" />

'use strict';

var helper = require('../../test-helper');
var config = helper.config();

var request = require('supertest');
var chai = require('chai');
var should = chai.should();

describe('spike arrest', function() {

  var servers, proxy;

  before(function(done) {
    config.spikearrest = {
      timeUnit: 'minute',
      bufferSize: 0,
      allow: 1
    };

    config.edgemicro.plugins.sequence = ['spikearrest'];
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

  describe('quota', function() {

    it('should succeed before limit is hit', function(done) {

      request(proxy)
        .get('/')
        .expect(200)
        .end(function(err, res) {
          if (err) { return done(err); }
          should.exist(res.body.fromTarget);
          done();
        });
    });

    it('should fail once limit is hit', function(done) {

      request(proxy)
        .get('/')
        .expect(503)
        .end(function(err, res) {
          if (err) { return done(err); }
          should.not.exist(res.body.fromTarget);
          res.text.should.eql('{"message":"SpikeArrest engaged","status":503}');
          done();
        });
    });

  });
});
