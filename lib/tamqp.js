var Connection = require('./connection.js');


var createClient = function(ServiceClient, connection) {
    if (ServiceClient.Client) {
        ServiceClient = ServiceClient.Client;
    }
    // Wrap the write method
    var writeCb = function(buf, seqid) {
        connection.write(buf, seqid);
    };
    var transport = new connection.transport(undefined, writeCb);
    var client = new ServiceClient(transport, connection.protocol);
    transport.client = client;
    connection.client = client;
    return client;
};


var createConnection = function(url, options) {
    var logger = options.logger;
    var connection = new Connection(url, options);
    return connection;
};


module.exports = {
    createConnection: createConnection,
    createClient: createClient
};
