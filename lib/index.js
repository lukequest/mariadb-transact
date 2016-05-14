(function() {
  var MariaSQL, Promise, TransactionManager;

  MariaSQL = require("mariasql");

  Promise = require("bluebird");

  TransactionManager = (function() {

    /*
      Handles a queue of transactions and a pool of MariaSQL connections.
     */
    function TransactionManager(opts) {
      if (!opts) {
        opts = {};
      }
      this.conn = {
        connected: false
      };
      this.autoconvert = typeof opts.metadata === "undefined" ? true : !!opts.metadata;
      this._pool = [];
      this._queue = [];
      this._poolsize = typeof opts.poolsize === "number" ? opts.poolsize : 3;
      this.conncfg = opts;
    }

    TransactionManager.prototype._createConnection = function() {

      /*
        Create a new connection object.
       */
      return new Promise((function(_this) {
        return function(resolve, reject) {
          var conn;
          conn = new MariaSQL();
          conn.connect(_this.conncfg);
          conn.command = conn.cmd = _this.command.bind(_this, conn);
          conn.commit = _this.commit.bind(_this, conn);
          conn.fetchArray = _this.fetchArray.bind(_this, conn);
          conn.fetchOne = _this.fetchOne.bind(_this, conn);
          conn.rollback = _this.rollback.bind(_this, conn);
          conn._queryAsync = Promise.promisify(conn.query, {
            context: conn
          });
          conn.on("ready", function() {
            return resolve(conn);
          });
          return conn.on("error", function(err) {
            return reject(err);
          });
        };
      })(this));
    };

    TransactionManager.prototype.init = function() {

      /*
        Initialize all connections.
       */
      return Promise["try"]((function(_this) {
        return function() {
          return _this._createConnection();
        };
      })(this)).then((function(_this) {
        return function(conn) {
          var funcs, i, j, ref;
          _this.conn = conn;
          funcs = [];
          if (_this._poolsize > 0) {
            for (i = j = 1, ref = _this._poolsize; 1 <= ref ? j <= ref : j >= ref; i = 1 <= ref ? ++j : --j) {
              funcs.push(_this._createConnection());
            }
          }
          return Promise.all(funcs);
        };
      })(this)).then((function(_this) {
        return function(tconns) {
          var funcs, j, len, tc;
          funcs = [];
          for (j = 0, len = tconns.length; j < len; j++) {
            tc = tconns[j];
            _this._pool.push(tc);
            funcs.push(tc._queryAsync("SET autocommit = 0"));
          }
          return Promise.all(funcs);
        };
      })(this));
    };

    TransactionManager.prototype.basic = function() {

      /*
        Get a basic, non-transactional connection. (Only for simple queries.)
       */
      return new Promise((function(_this) {
        return function(resolve, reject) {
          if (_this.conn.connected) {
            return resolve(_this.conn);
          } else {
            return reject(new Error("The transaction manager is not connected to a database."));
          }
        };
      })(this));
    };

    TransactionManager.prototype.close = function() {

      /*
        Close all connections.
       */
      return new Promise((function(_this) {
        return function(resolve) {
          var c, j, len, ref;
          _this.conn.end();
          ref = _this._pool;
          for (j = 0, len = ref.length; j < len; j++) {
            c = ref[j];
            c.end();
          }
          return resolve();
        };
      })(this));
    };

    TransactionManager.prototype._checkQueue = function() {

      /*
        Check the queue for waiting transaction initializations.
       */
      var resolve;
      if (this._queue.length > 0 && this._pool.length > 0) {
        resolve = this._queue.shift();
        return resolve(this._pool.pop());
      }
    };

    TransactionManager.prototype._finalCmd = function(cmd, conn) {

      /*
        Execute rollback or commit.
       */
      return Promise["try"]((function(_this) {
        return function() {
          return conn._queryAsync(cmd);
        };
      })(this))["finally"]((function(_this) {
        return function() {
          _this._pool.push(conn);
          return _this._checkQueue();
        };
      })(this));
    };

    TransactionManager.prototype.commit = function(conn) {

      /*
        Commit a transaction.
       */
      return this._finalCmd("COMMIT", conn);
    };

    TransactionManager.prototype.rollback = function(conn) {

      /*
        Roll back a transaction.
       */
      return this._finalCmd("ROLLBACK", conn);
    };

    TransactionManager.prototype.command = function(conn, sql, params) {

      /*
        Perform an SQL command (no result returned, use for INSERT/UPDATE queries).
       */
      return Promise["try"]((function(_this) {
        return function() {
          return conn._queryAsync(sql, params || {});
        };
      })(this)).then((function(_this) {
        return function(res) {
          if (res.info) {
            res.info.numRows = parseInt(res.info.numRows);
            res.info.affectedRows = parseInt(res.info.affectedRows);
            res.info.insertId = parseInt(res.info.insertId);
          }
          return res.info;
        };
      })(this));
    };

    TransactionManager.prototype.convert = function(row, metadata) {

      /*
        Convert row elements based on type info.
       */
      var key, results, t;
      results = [];
      for (key in row) {
        t = metadata[key].type;
        if (t === "DATE" || t === "DATETIME" || t === "TIMESTAMP") {
          row[key] = new Date(row[key]);
        }
        if (t === "DECIMAL" || t === "DOUBLE" || t === "FLOAT") {
          row[key] = parseFloat(row[key]);
        }
        if (t === "INTEGER" || t === "TINYINT" || t === "SMALLINT" || t === "MEDIUMINT" || t === "BIGINT") {
          results.push(row[key] = parseInt(row[key]));
        } else {
          results.push(void 0);
        }
      }
      return results;
    };

    TransactionManager.prototype.fetchArray = function(conn, sql, params) {

      /*
        Fetch an array of SQL result rows.
       */
      return Promise["try"]((function(_this) {
        return function() {
          return conn._queryAsync(sql, params || {});
        };
      })(this)).then((function(_this) {
        return function(res) {
          var j, len, row;
          for (j = 0, len = res.length; j < len; j++) {
            row = res[j];
            if (res.info && res.info.metadata && _this.autoconvert) {
              _this.convert(row, res.info.metadata);
            }
          }
          return res;
        };
      })(this));
    };

    TransactionManager.prototype.fetchOne = function(conn, sql, params) {

      /*
        Fetch a single SQL result row.
       */
      return Promise["try"]((function(_this) {
        return function() {
          return conn._queryAsync(sql, params || {});
        };
      })(this)).then((function(_this) {
        return function(res) {
          var row;
          if (res && res.length) {
            row = res[0];
            if (res.info && res.info.metadata && _this.autoconvert) {
              _this.convert(row, res.info.metadata);
            }
            return row;
          } else {
            return null;
          }
        };
      })(this));
    };

    TransactionManager.prototype.begin = function() {

      /*
        Attempt to begin a transaction. Add to promise to queue if connection pool is empty.
       */
      return new Promise((function(_this) {
        return function(resolve) {
          if (_this._pool.length > 0) {
            return resolve(_this._pool.pop());
          } else {
            return _this._queue.push(resolve);
          }
        };
      })(this));
    };

    return TransactionManager;

  })();

  module.exports = TransactionManager;

}).call(this);
