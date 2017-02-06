'use strict'
//export all modules
module.exports = {
  'accumulate-request':require('./accumulate-request'),
  'accumulate-response':require('./accumulate-response'),
  analytics:require('./analytics'),
  'header-uppercase':require('./header-uppercase'),
  oauth:require('./oauth'),
  quota:require('./quota'),
  'quota-memory':require('./quota-memory'),
  spikearrest:require('./spikearrest'),
  'transform-uppercase':require('./transform-uppercase'),
  json2xml: require('./json2xml'),
  healthcheck: require('./healthcheck')
}
