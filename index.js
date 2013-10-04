var util = require('util');
var Duplex = require('stream').Duplex;

if (!Duplex) {
    Duplex = require('readable-stream/duplex');
}

require('setimmediate');

function InsertStream(collection, options) {
    if (!(this instanceof(InsertStream)))
        return new InsertStream(collection, options);

    if ('object' !== typeof(collection))
        throw new TypeError('First argument isnt an object');
    if ('function' !== typeof(collection.insert))
        throw new TypeError('First argument doesnt have an insert function');
    if (3 > collection.insert.length)
        throw new TypeError('First argument.insert expects three parameters');

    if (options && 'object' != typeof(options))
        throw new TypeError('Second argument isnt a MongoDB Insert options');

    Duplex.call(this, {objectMode:true});

    this._state = {
        queue: [],
        ending: false,
        collection: collection,
        options: options,
        highWaterMark: (~~options.highWaterMark || 50)
    };
}

InsertStream.prototype = Object.create(Duplex.prototype, {constructor: {value: InsertStream}});

InsertStream.prototype._read = function(bytes) {
    //we push() from insert callback so nothing to do here
};

InsertStream.prototype._write = function(chunk, encoding, done) {
    if ('function' === typeof(this._transform))
        chunk = this._transform(chunk);

    this._state.queue.push(chunk);

    if (this._state.highWaterMark != this._state.queue.length) {
        done();
    } else {
        setImmediate(this._dequeue.bind(this, done));
    }
};

InsertStream.prototype._dequeue = function(done) {
    var self = this, chunk = this._state.queue.splice(0);

    if (0 === chunk.length)
        return;

    this._state.collection.insert(chunk, this._state.options, function(error, results) {
        if ('function' === typeof(done))
            done(error);

        if (error) {
            self.emit('error', error);
        } else {
            results.forEach(self.push.bind(self));

            self._checkFinish();
        }
    });
};

InsertStream.prototype.end = function(chunk, encoding, done) {
    if (chunk)
        Duplex.prototype.write.call(this, chunk, encoding, done);

    if (false === this._state.ending) {
        this._state.ending = true;
        this._checkFinish();
    }
};

InsertStream.prototype.transform = function(method) {
    if ('function' !== typeof(method))
        throw 'transform(val) only accepts functions';

    this._transform = method;
    return this;
};

InsertStream.prototype._checkFinish = function() {
    if (false === this._state.ending)
        return;

    if (0 < this._state.queue.length) {
        setImmediate(this._dequeue.bind(this));
    } else {
        setImmediate(Duplex.prototype.end.bind(this));
    }
};

module.exports = function(collection, options) {
    return new InsertStream(collection, options);
};
