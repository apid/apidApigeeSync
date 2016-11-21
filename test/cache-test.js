const cache = require('../cache/index');
const assert = require('assert');

describe('cache plugin', () => {
  var plugin = null;

  beforeEach(() => {
    var config = {};
    var logger = {};
    var stats = {};

    plugin = cache.init.apply(null, [config, logger, stats]);
  });

  it('exposes an onrequest handler', () => {
    assert.ok(plugin.onrequest);
  });

  it('will call next if there is no token attached to the request object', (done) => {
    const onrequest_cb = () => {
      done();
    };

    plugin.onrequest.apply(null, [{}, {}, onrequest_cb]);

  });
});

