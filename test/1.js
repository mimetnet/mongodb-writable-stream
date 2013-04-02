var tap = require('tap')
    , test = tap.test
    , mis;

test('require', function(t) {
    mis = require('../index.js');

    t.ok(mis, 'mongodb-writable-stream exists');
    t.type(mis, 'function', 'require returns a function');
    t.equal(0, Object.keys(mis).length, 'no hidden exports');
    t.end();
});

test('octor', function(t) {
    t.throws(function() {
        mis();
    }, {name:'TypeError', message:'First argument isnt an object'}, 'throws TypeError');

    t.throws(function() {
        mis({});
    }, {name:'TypeError', message:'First argument doesnt have an insert function'}, '1st Arg TypeError');

    t.throws(function() {
        mis({insert:function(){}});
    }, {name:'TypeError', message:'First argument.insert expects three parameters'}, '1st Arg.insert TypeError');

    t.throws(function() {
        mis({insert:function(a,b,c){}}, 'string');
    }, {name:'TypeError', message:'Second argument isnt a MongoDB Insert options'}, '2nd Arg TypeError - string');

    t.throws(function() {
        mis({insert:function(a,b,c){}}, 1);
    }, {name:'TypeError', message:'Second argument isnt a MongoDB Insert options'}, '2ns Arg TypeError - number');

    t.doesNotThrow(function() {
        mis({insert:function(a,b,c){}}, {});
    }, 'Mock arguments');

    t.end();
});
