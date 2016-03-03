var should = require('should');
var assert = require('assert');
var plugin = require('../index');
describe('construct',function(){
   it('should work',function(done){
       var req = plugin.init();
       req.onrequest(null,null,function(err){
           should(err).be.null
           done();
       })
   }) ;
});