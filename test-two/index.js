'use strict';

module.exports.init = function(config, logger, stats) {

  var id = 'two';

  return {

    ondata_request: function(req, res, data, next) {
      console.log(id, 'ondata request');
      next(null, append(id, 'request', data));
    },

    ondata_response: function(req, res, data, next) {
      console.log(id, 'ondata response');
      next(null, append(id, 'response', data));
    },

    onend_request: function(req, res, data, next) {
      console.log(id, 'onend request');
      next(null, append(id, 'request_end', data));
    },

    onend_response: function(req, res, data, next) {
      console.log(id, 'onend response');
      next(null, append(id, 'response_end', data));
    },

  };

  function append(id, name, data) {
    if (data.length === 0) return data;
    var body = JSON.parse(data.toString());
    var property = 'sequence' + '_' + name;
    if (!body[property]) body[property] = [];
    body[property].push(id);
    return JSON.stringify(body);
  }
}
