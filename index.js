var readConfig = require('read-config');
    config = readConfig('./config.json');
var reader = require('./reader.js');

reader.start(config);
