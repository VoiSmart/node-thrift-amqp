var util = require('util');
var EventEmitter = require("events").EventEmitter;

var amqp = require('amqplib');
var thrift = require('thrift');

var transport = require('./transport');


var Connection = function(url, options) {
    EventEmitter.call(this);
    this.options = options || {};
    this.url = url;
    this.transport = this.options.transport || transport.Transport;
    this.protocol = this.options.protocol || thrift.TBinaryProtocol;
    this._reset();
    this.send_pending = [];
    this._currentTry = 0;
    this._stopTrying = false;
    this.connectionCallback = undefined;
    this.logger = options.logger;
};
util.inherits(Connection, EventEmitter);


Connection.prototype._log = function() {
    if (arguments.length === 0) {
        return;
    }
    var args = [];
    var level = arguments[0];
    for (var i=0; i<arguments.length; i++) {
        args.push(arguments[i]);
    }
    if (this.logger && this.logger[level]) {
        this.logger[level].apply(this, args);
    }
};


Connection.prototype._reset = function() {
    this.connected = false;
    this.connection = null;
    this.channel = null;
    this.seqId2Service = {};
};


Connection.prototype.close = function() {
    this._stopTrying = true;
    if (this.isOpen()) {
        this.connection.close();
        this._reset();
    }
};


Connection.prototype.connect = function(callback) {
    this.connectionCallback = callback;
    var me = this;
    this._currentTry += 1;
    this._stopTrying = false;
    var connected = amqp.connect(this.url);
    connected.then(function(connection) {
        me.connected = true;
        me.connection = connection;
        me.connection.on('close', me._onConnectionClose.bind(me));
        return connection.createChannel();
    }).then(function(ch) {
        me.channel = ch;
        ch.on('error', me._onChannelError.bind(me));
        ch.on('return', me._onReturn.bind(me));
        return me.channel.assertExchange(me.options.servicesExchange, 'direct', {durable: false});
    }).then(function() {
        return me.channel.assertExchange(me.options.responsesExchange, 'direct', {durable: false});
    }).then(function() {
        var qargs = {
            durable: false,
            exclusive: true,
            autoDelete: true,
            "arguments": {
                'x-message-ttl': 0
            }
        };
        return me.channel.assertQueue(null, qargs);
    }).then(function(q) {
        me.response_queue = q.queue;
        return me.channel.bindQueue(me.response_queue, me.options.responsesExchange, me.response_queue);
    }).then(function() {
        return me.channel.consume(me.response_queue, me._onMessage.bind(me), {noAck: true});
    }).then(function() {
        me.connected = true;
        me._currentTry = 0;
        me._onConnected();
        if (callback) {
            callback(null, me);
        }
    }).catch(function(err) {
        me.connected = false;
        if (callback) {
            callback(err, me);
        }
    });
};


Connection.prototype._onConnectionClose = function(err) {
    if (this._stopTrying) {
        this._log('info', 'connection closed: ', err);
        return;
    }
    this._log('error', 'connection closed: ', err);
    var attempt = this.currentTry ? (this.currentTry > 0) : 0;
    var random_delay = Math.floor(Math.random() * (30 - 1)) + 1;
    var max_delay = 120;
    var delay = Math.min(random_delay + Math.pow(2, attempt-1), max_delay);
    this._log('warn', "disconnected. Attempt " + attempt + ". Trying to reconnect in " + delay + "s");
    var me = this;
    setTimeout(function() { me.connect.call(me, me.connectionCallback);}, delay * 1000);
};


Connection.prototype._onChannelError = function(err) {
    this._log('error', 'channel closed: ', err);
    this.connection.close();  // will reconnect
};


Connection.prototype._onReturn = function(data) {
    if (Object.prototype.toString.call(data) === "[object ArrayBuffer]") {
        data = new Uint8Array(data.content);
    }
    var buf = new Buffer(data.content);
    var me = this;
    var callback = function(tdata) {
        me.__basicReturnCallback.call(me, tdata, data);
    };
    this.transport.receiver(callback)(buf);
};


Connection.prototype._onMessage = function(data) {
    this._log('log', 'Received message', data);
    if (Object.prototype.toString.call(data) === "[object ArrayBuffer]") {
        data = new Uint8Array(data.content);
    }
    var buf = new Buffer(data.content);
    this.transport.receiver(this.__decodeCallback.bind(this))(buf);
};


Connection.prototype._onConnected = function() {
    if (this.send_pending.length > 0) {
        var me = this;
        this.send_pending.forEach(function(data) {
            me.write(data);
        });
        this.send_pending = [];
    }
};


Connection.prototype.isOpen = function() {
    return this.connected;
};


Connection.prototype.write = function(data, seqId) {
    if (this.isOpen()) {
        // send data to amqp server
        var opts = {mandatory: true, deliveryMode: 1, replyTo: this.response_queue};
        this.channel.publish(this.options.servicesExchange, this.options.routingKey, data, opts);
    } else {
        // queue data to send when connection is established
        this.send_pending.push(data);
    }
};


Connection.prototype.__basicReturnCallback = function(transport_with_data, err) {
    var proto = new this.protocol(transport_with_data);
    try {
        while (true) {
            var header = proto.readMessageBegin();
            delete this.client._reqs[header.rseqid];
        }
    } catch (e) {
        if (e instanceof transport.BufferUnderrunError) {
            transport_with_data.rollbackPosition();
        } else {
            throw e;
        }
    }
    this._onChannelError(err);
};


Connection.prototype.__decodeCallback = function(transport_with_data) {
    // this is adapted from thrift sources
    var proto = new this.protocol(transport_with_data);
    try {
        while (true) {
            var header = proto.readMessageBegin();
            var dummy_seqid = header.rseqid * -1;
            var client = this.client;
            var service_name = this.seqId2Service[header.rseqid];
            if (service_name) {
                client = this.client[service_name];
                delete this.seqId2Service[header.rseqid];
            }
            client._reqs[dummy_seqid] = function(err, success) {
                transport_with_data.commitPosition();
                var clientCallback = client._reqs[header.rseqid];
                delete client._reqs[header.rseqid];
                if (clientCallback) {
                    clientCallback(err, success);
                }
            };
            if (client['recv_' + header.fname]) {
                client['recv_' + header.fname](proto, header.mtype, dummy_seqid);
            } else {
                delete client._reqs[dummy_seqid];
                this.emit("error",
                        new Error("Received a response to an unknown RPC function"));
            }
        }
    } catch (e) {
        if (e instanceof transport.BufferUnderrunError) {
            transport_with_data.rollbackPosition();
        } else {
            throw e;
        }
    }
};


module.exports = Connection;
