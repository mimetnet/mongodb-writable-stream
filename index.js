var util = require('util');
var Stream = require('stream');

require('setimmediate');

function StreamingInsert(collection, options, bufferLength) {
    if (!(this instanceof(StreamingInsert)))
        return new StreamingInsert(collection, options, bufferLength);

    if ('number' !== typeof(bufferLength))
        bufferLength = 50;
    if (options && 'object' != typeof(options))
        throw 'Second argument isnt a MongoDB Insert options';

    if ('object' !== typeof(collection))
        throw 'First argument isnt an object';
    if ('function' !== typeof(collection.insert))
        throw 'First argument doesnt have an insert function';
    if (3 > collection.insert.length)
        throw 'First collection.insert expects three arguments';

    Stream.call(this);

    this.writable = true;

    this.collection = collection;
    this.bufferLength = bufferLength;
    this.options = options;
    this.error = false;
    this.queue = [];
    this.inserted = 0;
}

StreamingInsert.prototype = Object.create(Stream.prototype, {constructor: {value: StreamingInsert}});

StreamingInsert.prototype.write = function(chunk) {
    if (this.writable) {
        if ('function' === typeof(this._transform))
            chunk = this._transform(chunk);

        if (this.bufferLength == this.queue.push(chunk))
            setImmediate(this._dequeue.bind(this));

        return true;
    }

    this.emit('error', 'StreamingInsert has been closed');

    return false;

};

StreamingInsert.prototype.transform = function(method) {
    if ('function' !== typeof(method))
        throw 'transform(val) only accepts functions';

    this._transform = method;
    return this;
};

StreamingInsert.prototype.end = function(chunk) {
    if ('undefined' !== typeof(chunk) && null !== chunk) {
        this.write(chunk);
    }

    this.writable = false;
    this._checkFinish();
};

StreamingInsert.prototype._dequeue = function() {
    var q = this.queue.splice(0, this.queue.length);

    if (0 === q.length)
        return;

    this.collection.insert(q, this.options, this._handleInsert.bind(this));
};

StreamingInsert.prototype._handleInsert = function(error, result) {
    if (util.isArray(result) && result.length) {
        this.inserted += result.length;
    }

    if (error) {
        this.emit('error', error);
        this.end();
    } else {
        this._checkFinish();
    }
};

StreamingInsert.prototype._checkFinish = function() {
    if (false === this.writable && false === this.error) {
        if (0 < this.queue.length) {
            this._dequeue();
        } else {
            this.emit('finish');
        }
    }
};

module.exports = function(collection, options, bufferLength) {
    return new StreamingInsert(collection, options, bufferLength);
};

module.exports.StreamingInsert = StreamingInsert;
