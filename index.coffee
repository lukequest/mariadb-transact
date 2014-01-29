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
    @pool = []
    @queue = []
    @poolsize = if typeof opts.poolsize == "number" then opts.poolsize else 20
    @conncfg = opts.connection
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
    @conn.on "connect", =>
      # Now, initialize the transaction connections.
      waiting = 0
      if @poolsize > 0
        wrapper = =>
          waiting++
          conn = @createConnection()
          conn.on "connect", =>
            q = conn.query "SET autocommit = 0"
            q.on "result", =>
            q.on "error", (err) => @emit "error", err
            q.on "end", =>
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
    q = conn.query cmd
    reterr = null
    q.on "result", (res) =>
      res.on "error", (err) => reterr = err
    q.on "end", =>
      if reterr == null
        deferred.resolve()
      else
        deferred.reject(reterr)
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
    if !params
      params = {}
    ret = null
    rerr = null
    q = conn.query(sql, params)
    q.on "result", (res) ->
      res.on "end", (info) -> ret = info
      res.on "error", (err) -> rerr = err
    q.on "end", ->
      if rerr == null
        deferred.resolve(ret)
      else
        deferred.reject(rerr)
    return deferred.promise


  fetchArray: (conn, sql, params) ->
    ###
      Fetch an array of SQL result rows.
    ###
    deferred = Q.defer()
    if !params
      params = {}
    rows = []
    rerr = null
    q = conn.query(sql, params)
    q.on "result", (res) ->
      res.on "row", (row) -> rows.push(row)
      res.on "error", (err) -> rerr = err
    q.on "end", ->
      if rerr == null
        deferred.resolve(rows)
      else
        deferred.reject(rerr)
    return deferred.promise


  fetchOne: (conn, sql, params) ->
    ###
      Fetch a single SQL result row.
    ###
    deferred = Q.defer()
    if !params
      params = {}
    resrow = null
    rerr = null
    q = conn.query(sql, params)
    q.on "result", (res) ->
      res.on "row", (row) -> if resrow==null then resrow = row
      res.on "error", (err) -> rerr = err
    q.on "end", ->
      if rerr == null
        deferred.resolve(resrow)
      else
        deferred.reject(rerr)
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