mariadb-transact
================

A transaction manager for the node.js MariaSQL module that uses promises. This module is essentially a convenience wrapper for [node-mariasql](https://github.com/mscdex/node-mariasql) with some extended functionality.

Install
=======

	npm install mariadb-transact

Example
=======

**CoffeeScript**

	TransactionManager = require "mariadb-transact"

	sqlcfg =
	  user: "USER"
	  password: "PASSWORD"
	  host: "HOST"
	  db: "DATABASE"
	  metadata: true
	
	transact = new TransactionManager(sqlcfg)
	
	transact.init().then ->
	  transact.basic().then (sql) ->
	    sql.fetchArray("SHOW DATABASES")
	    .then (res) ->
	      console.log res
	      
	  .then ->
	    transact.begin()
	    
	  .then (sql) ->
	    sql.command("DELETE FROM invoice_status WHERE name='test'")
	    
	    .then (res) ->
	      console.log res
	      sql.commit()
	      
	    .then ->
	      transact.close()
	      
	.catch (err) ->
	  console.error err.stack


**JavaScript**

	var TransactionManager = require("mariadb-transact");
	
	var sqlcfg = {
	user: "USER",
	password: "PASSWORD",
	host: "HOST",
	db: "DATABASE",
	metadata: true
	};
	
	var transact = new TransactionManager(sqlcfg);
	
	transact.init().then(function() {
	  return transact.basic().then(function(sql) {
	    return sql.fetchArray("SHOW DATABASES").then(function(res) {
	      return console.log(res);
	    });
	  }).then(function() {
	    return transact.begin();
	  }).then(function(sql) {
	    return sql.command("DELETE FROM mytable WHERE id=3").then(function(res) {
	      console.log(res);
	      return sql.commit();
	    }).then(function() {
	      return transact.close();
	    });
	  });
	})["catch"](function(err) {
	  return console.error(err.stack);
	});

API
===

`require('mariadb-transact')` returns a **_TransactionManager_** object.


TransactionManager properties
-----------------------------

* **log** - < _Logger_ > - A Winston-style logging object. Default is a stub.


TransactionManager events
-------------------------

* **error**(< _Error_ >err) - An error occurred.

* **init**() - The manager has been initialized.


TransactionManager methods
--------------------------

* **contructor**(< _object_ >config) - Create and return a new TransactionManager instance. The *config* object is equivalent to the Client config object in the node-mariasql library, but takes an additional parameter and extends on the **metadata** parameter:

	* **metadata** - < _boolean_ > - Automatically convert result data into appropriate types. **Default:** false

	* **poolsize** - < _integer_ > - Size of the connection pool. **Default:** 20


* **init**() - < _Promise_ >() - Initializes the database connection and returns a promise that fulfills when it is connected.

* **basic**() - < _Promise_>(< _Client_ >client) - Returns a promise that fulfills with a non-transaction-safe **_Client_** object.

* **begin**() - < _Promise_>(< _Client_ >client) - Returns a promise that fulfills with a transaction-safe **_Client_** object from the connection pool. Transaction-safe **_Client_** objects must always be completed using their *commit* or *rollback* method, otherwise they will not complete or return to the pool.

* **begin**() - < _Promise_ >() - Closes connection and returns a promise that fulfills when complete.


Client methods
--------------

* **command**(< _string_ >query, < _object_ >params) - < _Promise_ >(< _object_ >result) - Executes SQL query, returns promise that fulfills with a result object containing the following elements:

	* **affectedRows** - < _integer_ > - Number of rows affected by the command.

	* **insertId** - < _integer_ > - Last auto-increment ID of insert command.

	* **numRows** - < _integer_ > - Number of returned rows.

* **commit**() - < _Promise_ >() - Commits a transaction. Promise fulfulls when done.

* **fetchArray**(< _string_ >sql, < _object_ >params) - < _Promise_ >(< _Array_ >results) - Executes SQL query, returns promise that fulfills with a result array.

* **fetchOne**(< _string_ >sql, < _object_ >params) - < _Promise_ >(< _object_ >result) - Executes SQL query, returns promise that fulfills with a single result object. This command always returns the first result row of the query.

* **rollback**() - < _Promise_ >() - Rolls back a transaction. Promise fulfulls when done.