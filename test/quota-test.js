const quota = require('../quota/index');
const assert = require('assert');

var exampleConfig = { 
  EdgeMicroTestProduct: {
    allow: '3',
    interval: '1',
    timeUnit: 'minute',
    bufferSize: 10000,
    uri: 'https://edgemicroservices-us-east-1.apigee.net/edgemicro/quotas/organization/ws-poc3/environment/test',
    key: 'b7b69b6bd79e778782af9a653ae9c4d91e65440d8acbc7e969437a8a8dc13612',
    secret: 'c273ec588e22f29f3165fda450fee27f2c0703a08904ebecdbf379b5ac71090a' 
  }
}

describe('quota plugin', () => {
  var plugin = null;
  
  beforeEach(() => {
    var logger = {};
    var stats = {};

    plugin = quota.init.apply(null, [exampleConfig, logger, stats]);

  });
 
  it('exposes an onrequest handler', () => {
    assert.ok(plugin.onrequest);
  });

  it('will quota limit after 3 API calls', (done) => {
    var count = 0;
    var onrequest_cb = (err) => {
      count++;
      if(count == 4) {
        assert.equal(count, 4);
        assert.equal(err.message, 'exceeded quota');
        done();
      } 
    };

    var req = {
      token: {
        application_name: '0e7762f4-ea67-4cc1-ae4a-21598c35b18f',
        api_product_list: ['EdgeMicroTestProduct']       
      }
    }

    var res = {
      headers: {},
      setHeader: (key, val) => {
        res.headers[key] = val;
      }
    }

    plugin.onrequest.apply(null, [req, res, onrequest_cb]);
    plugin.onrequest.apply(null, [req, res, onrequest_cb]);
    plugin.onrequest.apply(null, [req, res, onrequest_cb]);
    plugin.onrequest.apply(null, [req, res, onrequest_cb]);
  });

  it('will not quota limit before 3 API calls', (done) => {
    var count = 0;
    var onrequest_cb = (err) => {
      count++;
      if(count == 3) {
        assert.equal(count, 3);
        assert.ok(!(err instanceof Error));
        done();
      } 
    };

    var req = {
      token: {
        application_name: '0e7762f4-ea67-4cc1-ae4a-21598c35b18f',
        api_product_list: ['EdgeMicroTestProduct']       
      }
    }

    var res = {
      headers: {},
      setHeader: (key, val) => {
        res.headers[key] = val;
      }
    }

    plugin.onrequest.apply(null, [req, res, onrequest_cb]);
    plugin.onrequest.apply(null, [req, res, onrequest_cb]);
    plugin.onrequest.apply(null, [req, res, onrequest_cb]);
  });
});


