var Firebird = require('node-firebird');

function ActiveToSwitchOff(value) {
	switch (value) {
		case '0':
			return 'T';
			break;
		case '1': 
			return 'F';
			break;
		default:
			return 'F';
	}
}

function recursiveInsertUsers(db, users, index, callback){
	if (index < 0){
		callback();
		return;
	}

	db.transaction(Firebird.ISOLATION_READ_COMMITED, function(err, transaction) {								
		transaction.query(
		"UPDATE OR INSERT INTO users (id_ext, login, name, workemail, switchoff, pass, rights, dismiss) "+
		"VALUES (?, ?, ?, ?, ?, ?, 3, 'F') "+
		"MATCHING (id_ext)", 
		[users[index].id, 
		(users[index].username+"").substr(0, 15), 
		(users[index].fullname+"").substr(0, 30), 
		(users[index].email+"").substr(0, 30),
		ActiveToSwitchOff(users[index].active),
		"D41D8CD98F00B204E9800998ECF8427E"], 
		function(err, result) {
			if (err) {
				console.error(err);
				transaction.rollback();
			} else {						
				transaction.commit(function(err) {
					if (err) {
						console.error(err);
						transaction.rollback();
					} else {
						console.log("[" + index + "] " + users[index].username + ' -> save user is Ok');
					}											
					recursiveInsertUsers(db, users, index-1, callback);
				});
			}
		});
	});
}

function recursiveInsertMarks(config, db, marks, index, callback){
	if (index < 0){
		callback();
		return;
	}

	db.transaction(Firebird.ISOLATION_READ_COMMITED, function(err, transaction) {								
		transaction.query(
		"UPDATE OR INSERT INTO "+config.tables.marks+" (name, folder, owner) VALUES (?, 1, 0) MATCHING (name)", 
		[marks[index].value], 
		function(err, result) {
			if (err) {
				console.error(err);
				transaction.rollback();
			} else {						
				transaction.commit(function(err) {
					if (err) {
						console.error(err);
						transaction.rollback();
					} else {
						console.log("[" + index + "] " + marks[index].value + ' -> save mark is Ok');
					}											
					recursiveInsertMarks(config, db, marks, index-1, callback);
				});
			}
		});
	});
}

function recursiveInsertTypes(config, db, types, index, callback){
	if (index < 0){
		callback();
		return;
	}

	db.transaction(Firebird.ISOLATION_READ_COMMITED, function(err, transaction) {								
		transaction.query(
		"UPDATE OR INSERT INTO "+config.tables.types+" (name, folder, owner) VALUES (?, 1, 0) MATCHING (name)", 
		[types[index].value], 
		function(err, result) {
			if (err) {
				console.error(err);
				transaction.rollback();
			} else {						
				transaction.commit(function(err) {
					if (err) {
						console.error(err);
						transaction.rollback();
					} else {
						console.log("[" + index + "] " + types[index].value + ' -> save type is Ok');
					}											
					recursiveInsertTypes(config, db, types, index-1, callback);
				});
			}
		});
	});
}

function recursiveInsertClients(config, db, clients, index, callback){
	if (index < 0) {
		callback();
		return;
	}

	db.transaction(Firebird.ISOLATION_READ_COMMITED, function(err, transaction) {								
		transaction.query(
		"UPDATE OR INSERT INTO customer ("+
		"  id_ext, name, fullname, prim, website, inn, curator, specialist, region, city, "+config.tables.types+", "+config.tables.marks+", address, "+
		"  custtype, folder, owner, regdate, actual_time, privateclient"+
		") VALUES ("+
		"  ?, ?, ?, ?, ?, ?, (select first(1) id from users where id_ext=?), (select first(1) id from users where id_ext=?), "+
		"  (select first(1) id from places where name containing ?), (select first(1) id from places where name=?), "+
		"  (select first(1) id from "+config.tables.types+" where name=?), (select first(1) id from "+config.tables.marks+" where name=?), "+
		"  ?, 1, 1, 0, current_timestamp, current_timestamp, 'F'"+
		") MATCHING (id_ext)", 
		[clients[index].id,
		(clients[index].org+"").substr(0, 200),
		(clients[index].org+"").substr(0, 200),
		clients[index].comment,
		(clients[index].site+"").substr(0, 50),
		(clients[index].inn+"").substr(0, 30),
		clients[index].manager,
		clients[index].manager,
		clients[index].region,
		clients[index].city,
		clients[index].typeorg,
		clients[index].typemark,
		clients[index].region + " " + clients[index].city],
		function(err, result) {
			if (err) {
				console.error(err);
				transaction.rollback();
			} else {						
				transaction.commit(function(err) {
					if (err) {
						console.error(err);
						transaction.rollback();
					} else {
						console.log("[" + index + "] " + clients[index].org + ' -> save client is Ok');						
					}											
					recursiveInsertClients(config, db, clients, index-1, callback);
				});
			}
		});
	});
}

function recursiveInsertContacts(db, contacts, index, callback){
	if (index < 0){
		callback();
		return;
	}

	db.transaction(Firebird.ISOLATION_READ_COMMITED, function(err, transaction) {								
		transaction.query(
		"UPDATE OR INSERT INTO contactpersons (id_ext, name, email, phone, custno) "+
		"VALUES (?, ?, ?, ?, (select first(1) id from customer where id_ext=?)) "+
		"MATCHING (id_ext)", 
		[contacts[index].id, 
		(contacts[index].name+"").substr(0, 30), 
		(contacts[index].email+"").substr(0, 50),
		(contacts[index].phone+"").substr(0, 50), 
		contacts[index].id_client], 
		function(err, result) {
			if (err) {
				console.error(err);
				transaction.rollback();
			} else {						
				transaction.commit(function(err) {
					if (err) {
						console.error(err);
						transaction.rollback();
					} else {
						console.log("[" + index + "] " + contacts[index].name + ' -> save contact is Ok');						
					}
					recursiveInsertContacts(db, contacts, index-1, callback);
				});
			}
		});
	});
}

function StatusTask(value) {
	switch (value) {
		case '1':
			return '1';
			break;
		case '2': 
			return '3';
			break;
		default:
			return value;
	}
}

function DoneTask(value) {
	switch (value) {
		case '1':
			return 'F';
			break;
		case '2': 
			return 'T';
			break;
		default:
			return 'F';
	}
}

function recursiveInsertTasks(db, tasks, index, callback){
	if (index < 0){
		callback();
		return;
	}

	db.transaction(Firebird.ISOLATION_READ_COMMITED, function(err, transaction) {								
		transaction.query(
		"INSERT INTO task ("+
		"  dateini, datestart, dateexp, description, taskstatus, maker, executor, custno, "+
		"  hint, showntask, rush, control, done, remake, rem_need, rem_before, rem_time"+
		") VALUES ("+
		"  CAST(? AS timestamp), CAST(? AS timestamp), DATEADD(15 minute to CAST(? AS timestamp)), ?, ?, "+
		"  (select first(1) id from users where id_ext=?), (select first(1) id from users where id_ext=?), "+
		"  (select first(1) id from customer where id_ext=?), 'F', 'F', 3, 'F', ?, 'F', 'T', 15, DATEADD(-15 minute to CAST(? AS timestamp))) ", 
		[tasks[index].date, 
		tasks[index].date, 
		tasks[index].date,
		"[" + tasks[index].type + "] " + tasks[index].text,
		StatusTask(tasks[index].status),
		tasks[index].user,
		tasks[index].user,
		tasks[index].id_client,
		DoneTask(tasks[index].status), 
		tasks[index].date], 
		function(err, result) {
			if (err) {
				console.error(err);
				transaction.rollback();
			} else {						
				transaction.commit(function(err) {
					if (err) {
						console.error(err);
						transaction.rollback();
					} else {
						console.log("[" + index + "] " + tasks[index].date + ' -> save task is Ok');
					}
					recursiveInsertTasks(db, tasks, index-1, callback);
				});
			}
		});
	});
}

function insertUsers(config, users, callback){
	Firebird.attach(config.database.firebird, function(err, db) {
		if (err)
			throw err;

		recursiveInsertUsers(db, users, users.length-1, function(){
			console.log(' (i) Users inserted.');
			db.detach();
			callback();
		});	
	});	
}

function insertMarks(config, marks, callback){
	Firebird.attach(config.database.firebird, function(err, db) {
		if (err)
			throw err;

		recursiveInsertMarks(config, db, marks, marks.length-1, function(){
			console.log(' (i) Marks inserted.');
			db.detach();
			callback();
		});	
	});	
}

function insertTypes(config, types, callback){
	Firebird.attach(config.database.firebird, function(err, db) {
		if (err)
			throw err;

		recursiveInsertTypes(config, db, types, types.length-1, function(){
			console.log(' (i) Types inserted.');
			db.detach();
			callback();
		});	
	});
}

function insertClients(config, clients, callback){
	Firebird.attach(config.database.firebird, function(err, db) {
		if (err)
			throw err;

		recursiveInsertClients(config, db, clients, clients.length-1, function(){
			console.log(' (i) Clients inserted.');
			db.detach();
			callback();
		});	
	});
}

function insertContacts(config, contacts, callback){
	Firebird.attach(config.database.firebird, function(err, db) {
		if (err)
			throw err;

		recursiveInsertContacts(db, contacts, contacts.length-1, function(){
			console.log(' (i) Contacts inserted.');
			db.detach();
			callback();
		});	
	});
}

function insertTasks(config, tasks, callback){
	Firebird.attach(config.database.firebird, function(err, db) {
		if (err)
			throw err;

		recursiveInsertTasks(db, tasks, tasks.length-1, function(){
			console.log(' (i) Tasks inserted.');
			db.detach();
			callback();
		});	
	});
}

exports.insertUsers = insertUsers;
exports.insertMarks = insertMarks;
exports.insertTypes = insertTypes;
exports.insertClients = insertClients;
exports.insertContacts = insertContacts;
exports.insertTasks = insertTasks;