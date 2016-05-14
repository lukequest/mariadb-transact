MariaSQL = require "mariasql"
Promise = require "bluebird"

class TransactionManager
  ###
    Handles a queue of transactions and a pool of MariaSQL connections.
  ###

  constructor: (opts) ->
    if !opts then opts = {}
    @conn = connected:false
    @autoconvert = if typeof opts.metadata == "undefined" then true else !!opts.metadata
    @_pool = []
    @_queue = []
    @_poolsize = if typeof opts.poolsize == "number" then opts.poolsize else 3
    @conncfg = opts


  _createConnection: ->
    ###
      Create a new connection object.
    ###
    return new Promise (resolve, reject) =>
      conn = new MariaSQL()
      conn.connect @conncfg
      conn.command = conn.cmd = @command.bind(@, conn)
      conn.commit = @commit.bind(@, conn)
      conn.fetchArray = @fetchArray.bind(@, conn)
      conn.fetchOne = @fetchOne.bind(@, conn)
      conn.rollback = @rollback.bind(@, conn)
      conn._queryAsync = Promise.promisify(conn.query, context: conn)

      conn.on "ready", =>
        resolve(conn)

      conn.on "error", (err) =>
        reject(err)


  init: ->
    ###
      Initialize all connections.
    ###
    Promise.try =>
      # First, create the basic "non-transactional" connection.
      @_createConnection()
    .then (conn) =>
      @conn = conn
      # Now create the transaction connections.
      funcs = []
      if @_poolsize > 0
        for i in [1..@_poolsize]
          funcs.push(@_createConnection())
      Promise.all(funcs)
    .then (tconns) =>
      # Disable autocommit on all transaction connections.
      funcs = []
      for tc in tconns
        @_pool.push(tc)
        funcs.push(tc._queryAsync("SET autocommit = 0"))
      Promise.all(funcs)


  basic: ->
    ###
      Get a basic, non-transactional connection. (Only for simple queries.)
    ###
    return new Promise (resolve, reject) =>
      if @conn.connected
        resolve(@conn)
      else
        reject(new Error("The transaction manager is not connected to a database."))


  close: ->
    ###
      Close all connections.
    ###
    return new Promise (resolve) =>
      @conn.end()
      for c in @_pool
        c.end()
      resolve()


  _checkQueue: ->
    ###
      Check the queue for waiting transaction initializations.
    ###
    if @_queue.length > 0 && @_pool.length > 0
      resolve = @_queue.shift()
      resolve(@_pool.pop())


  _finalCmd: (cmd, conn) ->
    ###
      Execute rollback or commit.
    ###
    Promise.try =>
      conn._queryAsync(cmd)
    .finally =>
      # Push this connection back into the pool and check the queue for unresolved promises.
      @_pool.push(conn)
      @_checkQueue()


  commit: (conn) ->
    ###
      Commit a transaction.
    ###
    @_finalCmd "COMMIT", conn


  rollback: (conn) ->
    ###
      Roll back a transaction.
    ###
    @_finalCmd "ROLLBACK", conn


  command: (conn, sql, params) ->
    ###
      Perform an SQL command (no result returned, use for INSERT/UPDATE queries).
    ###
    Promise.try =>
      conn._queryAsync(sql, params || {})
    .then (res) =>
      if res.info
        res.info.numRows = parseInt(res.info.numRows)
        res.info.affectedRows = parseInt(res.info.affectedRows)
        res.info.insertId = parseInt(res.info.insertId)
      return res.info


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
    Promise.try =>
      conn._queryAsync(sql, params || {})
    .then (res) =>
      for row in res
        if res.info && res.info.metadata && @autoconvert then @convert(row, res.info.metadata)
      return res


  fetchOne: (conn, sql, params) ->
    ###
      Fetch a single SQL result row.
    ###
    Promise.try =>
      conn._queryAsync(sql, params || {})
    .then (res) =>
      if res && res.length
        row = res[0]
        if res.info && res.info.metadata && @autoconvert then @convert(row, res.info.metadata)
        return row
      else
        return null


  begin: ->
    ###
      Attempt to begin a transaction. Add to promise to queue if connection pool is empty.
    ###
    return new Promise (resolve) =>
      if @_pool.length > 0
        # There is an available connection. Resolve immediately.
        resolve(@_pool.pop())
      else
        # No connection available. Add to queue.
        @_queue.push(resolve)

module.exports = TransactionManager