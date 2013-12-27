{EventEmitter} = require "events"
MariaSQL = require "mariasql"

class TransactionManager extends EventEmitter
  ###
    Handles a queue of transactions and a pool of MariaSQL connections.
  ###

  constructor: (opts) ->
    if !opts then opts = {}
    @pool = []
    @queue = []
    @poolsize = opts.poolsize || 20

    # Helper function for creating connections.
    createConnection = ->
      conn = new MariaSQL()
      conn.connect opts.connection
      conn.command = conn.cmd = @command.bind(@, conn)
      conn.commit = @commit.bind(@, conn)
      conn.fetchArray = @fetchArray.bind(@, conn)
      conn.fetchOne = @fetchOne.bind(@, conn)
      conn.rollback = @rollback.bind(@, conn)
      return conn

    # First, initiate the basic "non-transactional" connection.
    @conn = createConnection.call(@)
    @conn.on "error", (err) =>
      @emit "error", err
    @conn.on "connect", =>
      # Now, initialize the transaction connections.
      waiting = 0
      for i in [1..@poolsize]
        waiting++
        conn = createConnection.call(@)
        conn.on "connect", =>
          q = conn.query "SET autocommit = 0"
          q.on "result", =>
          q.on "error", (err) => @emit "error", err
          q.on "end", =>
            @pool.push conn
            waiting--
            if waiting <= 0
              @emit "init"


  basic: (callback) ->
    ###
      Get a basic, non-transactional connection. (Only for simple queries.)
    ###
    setImmediate =>
      callback?(@conn)


  checkQueue: ->
    ###
      Check the queue for waiting transaction initializations.
    ###
    if @queue.length > 0 && @pool.length > 0
      callback = @queue.shift()
      callback?(@pool.shift())


  finalCmd: (cmd, conn, callback) ->
    ###
      Execute rollback or commit.
    ###
    q = conn.query cmd
    reterr = null
    q.on "result", (res) =>
      res.on "error", (err) => reterr = err
    q.on "end", =>
      callback?(reterr)
      @pool.push(conn)
      @checkQueue()


  commit: (conn, callback) ->
    ###
      Commit a transaction.
    ###
    @finalCmd "COMMIT", conn, callback


  rollback: (conn, callback) ->
    ###
      Roll back a transaction.
    ###
    @finalCmd "ROLLBACK", conn, callback


  command: (conn, sql, params, callback) ->
    ###
      Perform an SQL command.
    ###
    if typeof params == "function"
      callback = params
      params = {}
    ret = null
    rerr = null
    q = conn.query(sql, params)
    q.on "result", (res) ->
      res.on "end", (info) -> ret = info
      res.on "error", (err) -> rerr = err
    q.on "end", ->
      callback?(rerr, ret)


  fetchArray: (conn, sql, params, callback) ->
    ###
      Fetch an array of SQL result rows.
    ###
    if typeof params == "function"
      callback = params
      params = {}
    rows = []
    rerr = null
    q = conn.query(sql, params)
    q.on "result", (res) ->
      res.on "row", (row) -> rows.push(row)
      res.on "error", (err) -> rerr = err
    q.on "end", ->
      callback?(rerr, rows)


  fetchOne: (conn, sql, params, callback) ->
    ###
      Fetch a single SQL result row.
    ###
    if typeof params == "function"
      callback = params
      params = {}
    resrow = null
    rerr = null
    q = conn.query(sql, params)
    q.on "result", (res) ->
      res.on "row", (row) -> if resrow==null then resrow = row
      res.on "error", (err) -> rerr = err
    q.on "end", ->
      callback?(rerr, resrow)


  begin: ->
    ###
      Attempt to begin a transaction. Add to queue if pool is empty.
    ###
    callback = null
    transact = true
    # Parse args.
    if arguments.length == 0
      # No callback = bye bye
      return
    else if arguments.length == 1
      # If the only passed argument is not a function, bye bye
      if typeof arguments[0] != "function" then return
      callback = arguments[0]
    else
      # More than one argument. Second argument must be function.
      if typeof arguments[1] != "function" then return
      transact = !!arguments[0]
      callback = arguments[1]

    process.nextTick =>
      if @pool.length > 0
        # There is an available connection. Start immediately.
        callback?(@pool.shift())
      else
        # No connection available. Add to queue.
        @queue.push(callback)

module.exports = TransactionManager