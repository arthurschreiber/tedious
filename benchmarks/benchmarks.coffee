{Request, TYPES} = require "../src/tedious"

module.exports =

  "nvarchar (small)":
    setup: (connection, cb) ->
      request = new Request "CREATE TABLE #benchmark ([value] nvarchar(max))", (err) ->
        return cb(err) if err

        request = new Request "INSERT INTO #benchmark ([value]) VALUES (@value)", cb
        request.addParameter("value", TYPES.NVarChar, "asdf")
        connection.execSql(request)

      connection.execSqlBatch(request)

    exec: (connection, cb) ->
      request = new Request "SELECT * FROM #benchmark", cb
      connection.execSql(request)

    teardown: (connection, cb) ->
      request = new Request "DROP TABLE #benchmark", cb
      connection.execSqlBatch(request)

  "nvarchar (large)":
    setup: (connection, cb) ->
      request = new Request "CREATE TABLE #benchmark ([value] nvarchar(max))", (err) ->
        return cb(err) if err

        request = new Request "INSERT INTO #benchmark ([value]) VALUES (@value)", cb
        request.addParameter("value", TYPES.NVarChar, new Array(5 * 1024 * 1024).join("x"))
        connection.execSql(request)

      connection.execSqlBatch(request)

    exec: (connection, cb) ->
      request = new Request "SELECT * FROM #benchmark", cb
      connection.execSql(request)

    teardown: (connection, cb) ->
      request = new Request "DROP TABLE #benchmark", cb
      connection.execSqlBatch(request)

  "varbinary (small)":
    setup: (connection, cb) ->
      request = new Request "CREATE TABLE #benchmark ([value] varbinary(max))", (err) ->
        return cb(err) if err

        request = new Request "INSERT INTO #benchmark ([value]) VALUES (@value)", cb
        request.addParameter("value", TYPES.VarBinary, "asdf")
        connection.execSql(request)

      connection.execSqlBatch(request)

    exec: (connection, cb) ->
      request = new Request "SELECT * FROM #benchmark", cb
      connection.execSql(request)

    teardown: (connection, cb) ->
      request = new Request "DROP TABLE #benchmark", cb
      connection.execSqlBatch(request)

  "varbinary (4)":
    setup: (connection, cb) ->
      request = new Request "CREATE TABLE #benchmark ([value] varbinary(4))", (err) ->
        return cb(err) if err

        request = new Request "INSERT INTO #benchmark ([value]) VALUES (@value)", cb
        request.addParameter("value", TYPES.VarBinary, "asdf")
        connection.execSql(request)

      connection.execSqlBatch(request)

    exec: (connection, cb) ->
      request = new Request "SELECT * FROM #benchmark", (err) ->
        console.log(arguments)
        cb()
      request.on "row", (data) ->
        console.log(data[0].value.toString())
      connection.execSql(request)

    teardown: (connection, cb) ->
      request = new Request "DROP TABLE #benchmark", cb
      connection.execSqlBatch(request)


  "varbinary (large)":
    setup: (connection, cb) ->
      request = new Request "CREATE TABLE #benchmark ([value] varbinary(max))", (err) ->
        return cb(err) if err

        request = new Request "INSERT INTO #benchmark ([value]) VALUES (@value)", cb
        request.addParameter("value", TYPES.VarBinary, new Array(5 * 1024 * 1024).join("x"))
        connection.execSql(request)

      connection.execSqlBatch(request)

    exec: (connection, cb) ->
      request = new Request "SELECT * FROM #benchmark", cb
      connection.execSql(request)

    teardown: (connection, cb) ->
      request = new Request "DROP TABLE #benchmark", cb
      connection.execSqlBatch(request)

  "varbinary (huge)":
    setup: (connection, cb) ->
      request = new Request "CREATE TABLE #benchmark ([value] varbinary(max))", (err) ->
        return cb(err) if err

        request = new Request "INSERT INTO #benchmark ([value]) VALUES (@value)", cb
        request.addParameter("value", TYPES.VarBinary, new Array(50 * 1024 * 1024).join("x"))
        connection.execSql(request)

      connection.execSqlBatch(request)

    exec: (connection, cb) ->
      request = new Request "SELECT * FROM #benchmark", cb
      connection.execSql(request)

    teardown: (connection, cb) ->
      request = new Request "DROP TABLE #benchmark", cb
      connection.execSqlBatch(request)