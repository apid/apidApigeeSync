const spikeArrest = require('../spikearrest/index');
const assert = require('assert');

describe('spike arrest plugin', () => {
  var plugin = null;

  beforeEach(() => {
    var config = {
      timeUnit: 'minute',
      bufferSize: 0,
      allow: 1
    };
    var logger = {};
    var stats = {};

    plugin = spikeArrest.init.apply(null, [config, logger, stats]);
  });

  it('exposes an onrequest handler', () => {
    assert.ok(plugin.onrequest);
  });

  it('will accept a request', (done) => {
    var onrequest_cb = () => {
      done();
    };

    var req = {};
    var res = {};
    plugin.onrequest.apply(null, [req, res, onrequest_cb]);
  });

  it('will reject a requests that come in too fast', (done) => {
    var onrequest_cb = () => {
    };
    
    var onrequest_cb_second = (req, res, next) => {
      assert.ok(req instanceof Error);
      assert.equal(req.message, 'SpikeArrest engaged');
      done();
    };

    var req = {};
    var res = {};
    plugin.onrequest.apply(null, [req, res, onrequest_cb]);
    plugin.onrequest.apply(null, [req, res, onrequest_cb_second]);
  });
});
