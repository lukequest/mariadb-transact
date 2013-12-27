mariadb-transact
================

A transaction manager for the node.js MariaSQL module.


## Usage

    mysqlcfg = {
      user: "USERNAME",
      password: "PASSWORD",
      host: "HOST",
      db: "DATABASE"
    };
    TransactionManager = require("mariadb-transact");
    transact = new TransactionManager({connection:mysqlcfg});