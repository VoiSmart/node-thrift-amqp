var util = require('util');
var amqp = require('amqplib');
var thrift = require('thrift');


var BufferUnderrunError = function(message) {
  Error.call(this);
  Error.captureStackTrace(this, this.constructor);
  this.name = this.constructor.name;
  this.message = message;
};
util.inherits(BufferUnderrunError, Error);


var Transport = function() {
    Transport.super_.apply(this, arguments);
};
util.inherits(Transport, thrift.TBufferedTransport);


Transport.prototype.ensureAvailable = function(len) {
    if (this.readCursor + len > this.writeCursor) {
        throw new BufferUnderrunError();
    }
};


Transport.receiver = function(callback, seqid) {
  var reader = new Transport();

  return function(data) {
    if (reader.writeCursor + data.length > reader.inBuf.length) {
      var buf = new Buffer(reader.writeCursor + data.length);
      reader.inBuf.copy(buf, 0, 0, reader.writeCursor);
      reader.inBuf = buf;
    }
    data.copy(reader.inBuf, reader.writeCursor, 0);
    reader.writeCursor += data.length;

    callback(reader, seqid);
  };
};


module.exports.Transport = Transport;
module.exports.BufferUnderrunError = BufferUnderrunError;
