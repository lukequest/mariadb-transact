{EventEmitter} = require "events"

MariaSQL = require "mariasql"
Q = require "q"

class TransactionManager extends EventEmitter
  ###
    Handles a queue of transactions and a pool of MariaSQL connections.
  ###

  constructor: (opts) ->
    if !opts then opts = {}
    @conn = connected:false
    @autoconvert = if typeof opts.metadata == "undefined" then true else !!opts.metadata
    @pool = []
    @queue = []
    @poolsize = if typeof opts.poolsize == "number" then opts.poolsize else 3
    @conncfg = opts
    @log = opts.log ||
      info:->
      warn:->
      error:->


  createConnection: ->
    ###
      Create a new connection object.
    ###
    conn = new MariaSQL()
    conn.connect @conncfg
    conn.command = conn.cmd = @command.bind(@, conn)
    conn.commit = @commit.bind(@, conn)
    conn.fetchArray = @fetchArray.bind(@, conn)
    conn.fetchOne = @fetchOne.bind(@, conn)
    conn.rollback = @rollback.bind(@, conn)
    return conn


  init: ->
    ###
      Initialize all connections.
    ###
    # Helper function for creating connections.
    deferred = Q.defer()

    # First, initiate the basic "non-transactional" connection.
    @conn = @createConnection()
    @conn.on "error", (err) =>
      @emit "error", err
      deferred.reject(err)
    @conn.on "ready", =>
      # Now, initialize the transaction connections.
      waiting = 0
      if @poolsize > 0
        wrapper = =>
          waiting++
          conn = @createConnection()
          conn.on "ready", =>
            Q.ninvoke(conn, "query", "SET autocommit = 0")
            .catch (err) =>
              @emit "error", err
            .finally =>
              @pool.push conn
              waiting--
              if waiting <= 0
                @emit "init"
                @log.info "TransactionManager initialized."
                deferred.resolve()
        for i in [1..@poolsize]
          wrapper()
      else
        @emit "init"
        deferred.resolve()
    return deferred.promise


  basic: ->
    ###
      Get a basic, non-transactional connection. (Only for simple queries.)
    ###
    deferred = Q.defer()
    setImmediate =>
      if @conn.connected
        deferred.resolve(@conn)
      else
        deferred.reject("The transaction manager is not connected to a database.")
    return deferred.promise


  close: ->
    ###
      Close all connections.
    ###
    deferred = Q.defer()
    setImmediate =>
      @conn.end()
      for c in @pool
        c.end()
      deferred.resolve()
    return deferred.promise


  checkQueue: ->
    ###
      Check the queue for waiting transaction initializations.
    ###
    if @queue.length > 0 && @pool.length > 0
      @log.info "TransactionManager starting queued transaction."
      deferred = @queue.shift()
      deferred.resolve(@pool.shift())


  finalCmd: (cmd, conn) ->
    ###
      Execute rollback or commit.
    ###
    deferred = Q.defer()
    Q.ninvoke(conn, "query", cmd)
    .then =>
      deferred.resolve()
    .catch (err) =>
      deferred.reject(err)
    .finally =>
      setImmediate =>
        # Push this connection back into the pool and check the queue for unresolved promises.
        @pool.push(conn)
        @checkQueue()
    return deferred.promise


  commit: (conn) ->
    ###
      Commit a transaction.
    ###
    @finalCmd "COMMIT", conn


  rollback: (conn) ->
    ###
      Roll back a transaction.
    ###
    @finalCmd "ROLLBACK", conn


  command: (conn, sql, params) ->
    ###
      Perform an SQL command (no result returned, use for INSERT/UPDATE queries).
    ###
    deferred = Q.defer()
    Q.ninvoke(conn, "query", sql, params || {})
    .then (res) =>
      if res.info
        res.info.numRows = parseInt(res.info.numRows)
        res.info.affectedRows = parseInt(res.info.affectedRows)
        res.info.insertId = parseInt(res.info.insertId)
      deferred.resolve res.info
    .catch (err) =>
      deferred.reject(err)
    return deferred.promise


  convert: (row, metadata) ->
    ###
      Convert row elements based on type info.
    ###
    for key of row
      t = metadata[key].type
      if t=="DATE" || t=="DATETIME" || t=="TIMESTAMP" then row[key] = new Date(row[key])
      if t=="DECIMAL" || t=="DOUBLE" || t=="FLOAT" then row[key] = parseFloat(row[key])
      if t=="INTEGER" || t=="TINYINT" || t=="SMALLINT" || t=="MEDIUMINT" || t=="BIGINT" then row[key] = parseInt(row[key])


  fetchArray: (conn, sql, params) ->
    ###
      Fetch an array of SQL result rows.
    ###
    deferred = Q.defer()
    Q.ninvoke(conn, "query", sql, params || {})
    .then (res) =>
      for row in res
        if res.info && res.info.metadata && @autoconvert then @convert(row, res.info.metadata)
      deferred.resolve(res)
    .catch (err) =>
      deferred.reject(err)
    return deferred.promise


  fetchOne: (conn, sql, params) ->
    ###
      Fetch a single SQL result row.
    ###
    deferred = Q.defer()
    Q.ninvoke(conn, "query", sql, params || {})
    .then (res) =>
      if res && res.length
        row = res[0]
        if res.info && res.info.metadata && @autoconvert then @convert(row, res.info.metadata)
        deferred.resolve(row)
      else
        deferred.resolve(null)
    .catch (err) =>
      deferred.reject(err)
    return deferred.promise


  begin: ->
    ###
      Attempt to begin a transaction. Add to promise to queue if connection pool is empty.
    ###
    deferred = Q.defer()

    setImmediate =>
      if @pool.length > 0
        # There is an available connection. Start immediately.
        deferred.resolve(@pool.shift())
      else
        # No connection available. Add promise to queue.
        @queue.push(deferred)
        @log.info "TransactionManager added transaction to queue. (Pool is empty.)"
    return deferred.promise

module.exports = TransactionManager