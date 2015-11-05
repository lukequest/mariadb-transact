(function() {
  var EventEmitter, MariaSQL, Q, TransactionManager,
    extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
    hasProp = {}.hasOwnProperty;

  EventEmitter = require("events").EventEmitter;

  MariaSQL = require("mariasql");

  Q = require("q");

  TransactionManager = (function(superClass) {
    extend(TransactionManager, superClass);


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
      this.pool = [];
      this.queue = [];
      this.poolsize = typeof opts.poolsize === "number" ? opts.poolsize : 3;
      this.conncfg = opts;
      this.log = opts.log || {
        info: function() {},
        warn: function() {},
        error: function() {}
      };
    }

    TransactionManager.prototype.createConnection = function() {

      /*
        Create a new connection object.
       */
      var conn;
      conn = new MariaSQL();
      conn.connect(this.conncfg);
      conn.command = conn.cmd = this.command.bind(this, conn);
      conn.commit = this.commit.bind(this, conn);
      conn.fetchArray = this.fetchArray.bind(this, conn);
      conn.fetchOne = this.fetchOne.bind(this, conn);
      conn.rollback = this.rollback.bind(this, conn);
      return conn;
    };

    TransactionManager.prototype.init = function() {

      /*
        Initialize all connections.
       */
      var deferred;
      deferred = Q.defer();
      this.conn = this.createConnection();
      this.conn.on("error", (function(_this) {
        return function(err) {
          _this.emit("error", err);
          return deferred.reject(err);
        };
      })(this));
      this.conn.on("ready", (function(_this) {
        return function() {
          var i, j, ref, results, waiting, wrapper;
          waiting = 0;
          if (_this.poolsize > 0) {
            wrapper = function() {
              var conn;
              waiting++;
              conn = _this.createConnection();
              return conn.on("ready", function() {
                return Q.ninvoke(conn, "query", "SET autocommit = 0")["catch"](function(err) {
                  return _this.emit("error", err);
                })["finally"](function() {
                  _this.pool.push(conn);
                  waiting--;
                  if (waiting <= 0) {
                    _this.emit("init");
                    _this.log.info("TransactionManager initialized.");
                    return deferred.resolve();
                  }
                });
              });
            };
            results = [];
            for (i = j = 1, ref = _this.poolsize; 1 <= ref ? j <= ref : j >= ref; i = 1 <= ref ? ++j : --j) {
              results.push(wrapper());
            }
            return results;
          } else {
            _this.emit("init");
            return deferred.resolve();
          }
        };
      })(this));
      return deferred.promise;
    };

    TransactionManager.prototype.basic = function() {

      /*
        Get a basic, non-transactional connection. (Only for simple queries.)
       */
      var deferred;
      deferred = Q.defer();
      setImmediate((function(_this) {
        return function() {
          if (_this.conn.connected) {
            return deferred.resolve(_this.conn);
          } else {
            return deferred.reject("The transaction manager is not connected to a database.");
          }
        };
      })(this));
      return deferred.promise;
    };

    TransactionManager.prototype.close = function() {

      /*
        Close all connections.
       */
      var deferred;
      deferred = Q.defer();
      setImmediate((function(_this) {
        return function() {
          var c, j, len, ref;
          _this.conn.end();
          ref = _this.pool;
          for (j = 0, len = ref.length; j < len; j++) {
            c = ref[j];
            c.end();
          }
          return deferred.resolve();
        };
      })(this));
      return deferred.promise;
    };

    TransactionManager.prototype.checkQueue = function() {

      /*
        Check the queue for waiting transaction initializations.
       */
      var deferred;
      if (this.queue.length > 0 && this.pool.length > 0) {
        this.log.info("TransactionManager starting queued transaction.");
        deferred = this.queue.shift();
        return deferred.resolve(this.pool.shift());
      }
    };

    TransactionManager.prototype.finalCmd = function(cmd, conn) {

      /*
        Execute rollback or commit.
       */
      var deferred;
      deferred = Q.defer();
      Q.ninvoke(conn, "query", cmd).then((function(_this) {
        return function() {
          return deferred.resolve();
        };
      })(this))["catch"]((function(_this) {
        return function(err) {
          return deferred.reject(err);
        };
      })(this))["finally"]((function(_this) {
        return function() {
          return setImmediate(function() {
            _this.pool.push(conn);
            return _this.checkQueue();
          });
        };
      })(this));
      return deferred.promise;
    };

    TransactionManager.prototype.commit = function(conn) {

      /*
        Commit a transaction.
       */
      return this.finalCmd("COMMIT", conn);
    };

    TransactionManager.prototype.rollback = function(conn) {

      /*
        Roll back a transaction.
       */
      return this.finalCmd("ROLLBACK", conn);
    };

    TransactionManager.prototype.command = function(conn, sql, params) {

      /*
        Perform an SQL command (no result returned, use for INSERT/UPDATE queries).
       */
      var deferred;
      deferred = Q.defer();
      Q.ninvoke(conn, "query", sql, params || {}).then((function(_this) {
        return function(res) {
          if (res.info) {
            res.info.numRows = parseInt(res.info.numRows);
            res.info.affectedRows = parseInt(res.info.affectedRows);
            res.info.insertId = parseInt(res.info.insertId);
          }
          return deferred.resolve(res.info);
        };
      })(this))["catch"]((function(_this) {
        return function(err) {
          return deferred.reject(err);
        };
      })(this));
      return deferred.promise;
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
      var deferred;
      deferred = Q.defer();
      Q.ninvoke(conn, "query", sql, params || {}).then((function(_this) {
        return function(res) {
          var j, len, row;
          for (j = 0, len = res.length; j < len; j++) {
            row = res[j];
            if (res.info && res.info.metadata && _this.autoconvert) {
              _this.convert(row, res.info.metadata);
            }
          }
          return deferred.resolve(res);
        };
      })(this))["catch"]((function(_this) {
        return function(err) {
          return deferred.reject(err);
        };
      })(this));
      return deferred.promise;
    };

    TransactionManager.prototype.fetchOne = function(conn, sql, params) {

      /*
        Fetch a single SQL result row.
       */
      var deferred;
      deferred = Q.defer();
      Q.ninvoke(conn, "query", sql, params || {}).then((function(_this) {
        return function(res) {
          var row;
          if (res && res.length) {
            row = res[0];
            if (res.info && res.info.metadata && _this.autoconvert) {
              _this.convert(row, res.info.metadata);
            }
            return deferred.resolve(row);
          } else {
            return deferred.rssolve(null);
          }
        };
      })(this))["catch"]((function(_this) {
        return function(err) {
          return deferred.reject(err);
        };
      })(this));
      return deferred.promise;
    };

    TransactionManager.prototype.begin = function() {

      /*
        Attempt to begin a transaction. Add to promise to queue if connection pool is empty.
       */
      var deferred;
      deferred = Q.defer();
      setImmediate((function(_this) {
        return function() {
          if (_this.pool.length > 0) {
            return deferred.resolve(_this.pool.shift());
          } else {
            _this.queue.push(deferred);
            return _this.log.info("TransactionManager added transaction to queue. (Pool is empty.)");
          }
        };
      })(this));
      return deferred.promise;
    };

    return TransactionManager;

  })(EventEmitter);

  module.exports = TransactionManager;

}).call(this);
