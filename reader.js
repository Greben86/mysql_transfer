var mysql  = require('mysql');
	mysqlUtilities = require('mysql-utilities');
var saver = require('./saver.js');
var connection;

function start(config) {	
	connection = mysql.createConnection(config.database.mysql);

	connection.connect();
	readUsers(config, function(){
		connection.end();
	});
}

function readUsers(config, callback) {	
	connection.query('SELECT `id`, `username`, `fullname`, `email`, `active` FROM `sn_user`', [], function(err, result) {
		
		if (err)
			throw err;

		console.log();
		console.log(' (i) Read users is ok. Count users: ' + result.length);
		saver.insertUsers(config, result, function(){
			readMarks(config, callback);
		});		
	});
}

function readMarks(config, callback) {
	connection.query('SELECT `id`, `value` FROM `sn_type_mark`', [], function(err, result) {
		
		if (err)
			throw err;
		
		console.log();
		console.log(' (i) Read marks is ok. Count types: ' + result.length);
		saver.insertMarks(config, result, function(){
			readTypes(config, callback);
		});
	});
}

function readTypes(config, callback) {
	connection.query('SELECT `id`, `value` FROM `sn_type_org`', [], function(err, result) {
		
		if (err)
			throw err;
		
		console.log();
		console.log(' (i) Read types is ok. Count types: ' + result.length);
		saver.insertTypes(config, result, function(){
			readClients(config, callback);
		});
	});
}

function readClients(config, callback){
	connection.query(
		'SELECT c.`id`, c.`org`, c.`id_region`, c.`comment`, c.`manager`, c.`site`, c.`inn`, c.`city`, t.`value` AS `typeorg`, m.`value` AS typemark, 0 AS `region` '+
		'FROM `sn_client` AS c '+
		'  LEFT JOIN `sn_type_org` AS t ON (t.`id`=c.`id_typeorg`) '+
		'  LEFT JOIN `sn_type_mark` AS m ON (m.`id`=c.`mark`) limit 1000', [], function(err, result) {
		
		if (err)
			throw err;

		console.log();
		console.log(' (i) Read clients is ok. Count clients: ' + result.length);
		saver.insertClients(config, result, function(){
			readContacts(config, callback);
		});		
	});
}

function readContacts(config, callback){
	connection.query('SELECT `id`, `id_client`, `name`, `email`, `phone` FROM `sn_contact` limit 1000', [], function(err, result) {
	
		if (err)
			throw err;

		console.log();
		console.log(' (i) Read contacts is ok. Count contacts: ' + result.length);
		saver.insertContacts(config, result, function(){
			readTasks(config, callback);
		});		
	});
}

function readTasks(config, callback){
	connection.query('SELECT `date`, `type`, `text`, `status`, `user`, `id_client` FROM `sn_task` limit 1000', [], function(err, result) {

		if (err)
			throw err;

		console.log();
		console.log(' (i) Read tasks is ok. Count tasks: ' + result.length);
		saver.insertTasks(config, result, function(){
			callback();
		});
	});
}

exports.start = start;