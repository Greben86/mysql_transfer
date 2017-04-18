var readConfig = require('read-config');
    config = readConfig('./config.json');
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

function recursiveInsertUsers(users, index, callback){
	if (index === 0){
		callback();
		return;
	}

	Firebird.attach(config.database.firebird, function(err, db) {
		if (err)
			throw err;

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
			"930023ae91342d9ac1dd8d75eeebb844"], 
			function(err, result) {
				if (err) {
					console.error(err);
					transaction.rollback();
					db.detach();
				} else {						
					transaction.commit(function(err) {
						if (err) {
							console.error(err);
							transaction.rollback();
						} else {
							console.log(users[index].username + ' -> save user is Ok');
							recursiveInsertUsers(users, index-1, callback);
						}											
						db.detach();
					});
				}
			});
		});
	});
}

function recursiveInsertMarks(marks, index, callback){
	if (index === 0){
		callback();
		return;
	}

	Firebird.attach(config.database.firebird, function(err, db) {
		if (err)
			throw err;

		db.transaction(Firebird.ISOLATION_READ_COMMITED, function(err, transaction) {								
			transaction.query(
			"UPDATE OR INSERT INTO "+config.tables.marks+" (name, folder, owner) VALUES (?, 1, 0) MATCHING (name)", 
			[marks[index].value], 
			function(err, result) {
				if (err) {
					console.error(err);
					transaction.rollback();
					db.detach();
				} else {						
					transaction.commit(function(err) {
						if (err) {
							console.error(err);
							transaction.rollback();
						} else {
							console.log(marks[index].value + ' -> save mark is Ok');
							recursiveInsertMarks(marks, index-1, callback);
						}											
						db.detach();
					});
				}
			});
		});
	});
}

function recursiveInsertTypes(types, index, callback){
	if (index === 0){
		callback();
		return;
	}

	Firebird.attach(config.database.firebird, function(err, db) {
		if (err)
			throw err;

		db.transaction(Firebird.ISOLATION_READ_COMMITED, function(err, transaction) {								
			transaction.query(
			"UPDATE OR INSERT INTO "+config.tables.types+" (name, folder, owner) VALUES (?, 1, 0) MATCHING (name)", 
			[types[index].value], 
			function(err, result) {
				if (err) {
					console.error(err);
					transaction.rollback();
					db.detach();
				} else {						
					transaction.commit(function(err) {
						if (err) {
							console.error(err);
							transaction.rollback();
						} else {
							console.log(types[index].value + ' -> save type is Ok');
							recursiveInsertTypes(types, index-1, callback);
						}											
						db.detach();
					});
				}
			});
		});
	});
}

function recursiveInsertClients(clients, index, callback){
	if (index === 0) {
		callback();
		return;
	}

	Firebird.attach(config.database.firebird, function(err, db) {
		if (err)
			throw err;

		db.transaction(Firebird.ISOLATION_READ_COMMITED, function(err, transaction) {								
			transaction.query(
			"UPDATE OR INSERT INTO customer ("+
			"  id_ext, name, fullname, prim, website, inn, curator, specialist, region, city, "+config.tables.types+", "+config.tables.marks+", "+
			"  custtype, folder, owner, regdate, actual_time, privateclient"+
			") VALUES ("+
			"  ?, ?, ?, ?, ?, ?, (select first(1) id from users where id_ext=?), "+
			"  (select first(1) id from users where id_ext=?), (select first(1) id from places where name=?), "+
			"  (select first(1) id from places where name=?), "+
			"  (select first(1) id from "+config.tables.types+" where name=?), (select first(1) id from "+config.tables.marks+" where name=?), "+
			"  1, 1, 0, current_timestamp, current_timestamp, 'F'"+
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
			clients[index].typemark],
			function(err, result) {
				if (err) {
					console.error(err);
					transaction.rollback();
					db.detach();
				} else {						
					transaction.commit(function(err) {
						if (err) {
							console.error(err);
							transaction.rollback();
						} else {
							console.log(clients[index].org + ' -> save client is Ok');
							recursiveInsertClients(clients, index-1, callback);
						}											
						db.detach();
					});
				}
			});
		});
	});
}

function recursiveInsertContacts(contacts, index, callback){
	if (index === 0){
		callback();
		return;
	}

	Firebird.attach(config.database.firebird, function(err, db) {
		if (err)
			throw err;

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
					db.detach();
				} else {						
					transaction.commit(function(err) {
						if (err) {
							console.error(err);
							transaction.rollback();
						} else {
							console.log(contacts[index].name + ' -> save contact is Ok');
							recursiveInsertContacts(contacts, index-1, callback);
						}											
						db.detach();
					});
				}
			});
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

function TypeTask(value) {
	switch (value) {
		case 'call':
			return '[Звонок] ';
			break;
		case 'call_repeat': 
			return '[Повторный звонок] ';
			break;
		case 'kp': 
			return '[КП] ';
			break;
		case 'monitor': 
			return '[Мониторинг] ';
			break;
		default: 
			return "[" + value + "] ";
	}
}

function recursiveInsertTasks(tasks, index, callback){
	if (index === 0){
		callback();
		return;
	}

	Firebird.attach(config.database.firebird, function(err, db) {
		if (err)
			throw err;

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
			TypeTask(tasks[index].type) + tasks[index].text,
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
					db.detach();
				} else {						
					transaction.commit(function(err) {
						if (err) {
							console.error(err);
							transaction.rollback();
						} else {
							console.log(tasks[index].date + ' -> save task is Ok');
							recursiveInsertTasks(tasks, index-1, callback);
						}											
						db.detach();
					});
				}
			});
		});
	});
}

function insertUsers(users, callback){
	recursiveInsertUsers(users, users.length-1, function(){
		console.log(' (i) Users inserted.');
		callback();
	});
}

function insertClients(clients, callback){
	recursiveInsertClients(clients, clients.length-1, function(){
		console.log(' (i) Clients inserted.');
		callback();
	});
}

function insertMarks(marks, callback){
	recursiveInsertMarks(marks, marks.length-1, function(){
		console.log(' (i) Marks inserted.');
		callback();
	});
}

function insertTypes(types, callback){
	recursiveInsertTypes(types, types.length-1, function(){
		console.log(' (i) Types inserted.');
		callback();
	});
}

function insertContacts(contacts, callback){
	recursiveInsertContacts(contacts, contacts.length-1, function(){
		console.log(' (i) Contacts inserted.');
		callback();
	});
}

function insertTasks(tasks, callback){
	recursiveInsertTasks(tasks, tasks.length-1, function(){
		console.log(' (i) Tasks inserted.');
		callback();
	});
}

exports.insertUsers = insertUsers;
exports.insertMarks = insertMarks;
exports.insertTypes = insertTypes;
exports.insertClients = insertClients;
exports.insertContacts = insertContacts;
exports.insertTasks = insertTasks;