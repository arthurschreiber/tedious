iconv = require('iconv-lite')
sprintf = require('sprintf-js').sprintf
guidParser = require('./guid-parser')
require('./buffertools')

NULL = (1 << 16) - 1
MAX = (1 << 16) - 1
THREE_AND_A_THIRD = 3 + (1 / 3)
MONEY_DIVISOR = 10000

PLP_NULL = new Buffer([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF])
UNKNOWN_PLP_LEN = new Buffer([0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF])

DEFAULT_ENCODING = 'utf8'

parse = (buffer, metaData, options) ->
  value = undefined
  dataLength = undefined
  textPointerNull = undefined

  type = metaData.type

  if type.hasTextPointerAndTimestamp
    # Appear to be dummy values, so consume and discard them.
    textPointerLength = buffer.readUInt8()
    if textPointerLength != 0
      buffer.readBuffer(textPointerLength)
      buffer.readBuffer(8)
    else
      dataLength = 0
      textPointerNull = true

  if !dataLength && dataLength != 0
    # s2.2.4.2.1
    switch type.id & 0x30
      when 0x10 # xx01xxxx - s2.2.4.2.1.1
        # Zero length
        dataLength = 0
      when 0x20 # xx10xxxx - s2.2.4.2.1.3
        # Variable length
        if metaData.dataLength != MAX
          switch type.dataLengthLength
            when 0
              dataLength = undefined
            when 1
              dataLength = buffer.readUInt8()
            when 2
              dataLength = buffer.readUInt16LE()
            when 4
              dataLength = buffer.readUInt32LE()
            else
              throw Error("Unsupported dataLengthLength #{type.dataLengthLength} for data type #{type.name}")
      when 0x30 # xx11xxxx - s2.2.4.2.1.2
        # Fixed length
        dataLength = 1 << ((type.id & 0x0C) >> 2)

  switch type.name
    when 'Null'
      value = null
    when 'TinyInt'
      value = buffer.readUInt8()
    when 'Int'
      value = buffer.readInt32LE()
    when 'SmallInt'
      value = buffer.readInt16LE()
    when 'BigInt'
      value = buffer.readAsStringInt64LE()
    when 'IntN'
      switch dataLength
        when 0
          value = null
        when 1
          value = buffer.readUInt8()
        when 2
          value = buffer.readInt16LE()
        when 4
          value = buffer.readInt32LE()
        when 8
          value = buffer.readAsStringInt64LE()
        else
          throw new Error("Unsupported dataLength #{dataLength} for IntN")
    when 'Real'
      value = buffer.readFloatLE()
    when 'Float'
      value = buffer.readDoubleLE()
    when 'FloatN'
      switch dataLength
        when 0
          value = null
        when 4
          value = buffer.readFloatLE()
        when 8
          value = buffer.readDoubleLE()
        else
          throw new Error("Unsupported dataLength #{dataLength} for FloatN")
    when 'Money', 'SmallMoney', 'MoneyN'
      switch dataLength
        when 0
          value = null
        when 4
          value = buffer.readInt32LE() / MONEY_DIVISOR
        when 8
          high = buffer.readInt32LE()
          low = buffer.readUInt32LE()
          value = low + (0x100000000 * high)
          value /= MONEY_DIVISOR
        else
          throw new Error("Unsupported dataLength #{dataLength} for MoneyN")
    when 'Bit'
      value = !!buffer.readUInt8()
    when 'BitN'
      switch dataLength
        when 0
          value = null
        when 1
          value = !!buffer.readUInt8()
    when 'VarChar', 'Char'
      codepage = metaData.collation.codepage
      if metaData.dataLength == MAX
        value = readMaxChars(buffer, codepage)
      else
        value = readChars(buffer, dataLength, codepage)
    when 'NVarChar', 'NChar'
      if metaData.dataLength == MAX
        value = readMaxNChars(buffer)
      else
        value = readNChars(buffer, dataLength)
    when 'VarBinary', 'Binary'
      if metaData.dataLength == MAX
        value = readMaxBinary(buffer)
      else
        value = readBinary(buffer, dataLength)
    when 'Text'
      if textPointerNull
        value = null
      else
        value = readChars(buffer, dataLength, metaData.collation.codepage)
    when 'NText'
      if textPointerNull
        value = null
      else
        value = readNChars(buffer, dataLength)
    when 'Image'
      if textPointerNull
        value = null
      else
        value = readBinary(buffer, dataLength)
    when 'Xml'
      value = readMaxNChars(buffer)
    when 'SmallDateTime'
      value = readSmallDateTime(buffer, options.useUTC)
    when 'DateTime'
      value = readDateTime(buffer, options.useUTC)
    when 'DateTimeN'
      switch dataLength
        when 0
          value = null
        when 4
          value = readSmallDateTime(buffer, options.useUTC)
        when 8
          value = readDateTime(buffer, options.useUTC)
    when 'TimeN'
      if (dataLength = buffer.readUInt8()) == 0
        value = null
      else
        value = readTime buffer, dataLength, metaData.scale
    when 'DateN'
      if (dataLength = buffer.readUInt8()) == 0
        value = null
      else
        value = readDate buffer
    when 'DateTime2N'
      if (dataLength = buffer.readUInt8()) == 0
        value = null
      else
        value = readDateTime2 buffer, dataLength, metaData.scale
    when 'DateTimeOffsetN'
      if (dataLength = buffer.readUInt8()) == 0
        value = null
      else
        value = readDateTimeOffset buffer, dataLength, metaData.scale
    when 'NumericN', 'DecimalN'
      if dataLength == 0
        value = null
      else
        sign = if buffer.readUInt8() == 1 then 1 else -1

        switch dataLength - 1
          when 4
            value = buffer.readUInt32LE()
          when 8
            value = buffer.readUNumeric64LE()
          when 12
            value = buffer.readUNumeric96LE()
          when 16
            value = buffer.readUNumeric128LE()
          else
            throw new Error(sprintf('Unsupported numeric size %d at offset 0x%04X', dataLength - 1, buffer.position))
            break

        value *= sign
        value /= Math.pow(10, metaData.scale)
    when 'UniqueIdentifierN'
      switch dataLength
        when 0
          value = null
        when 0x10
          value = guidParser.arrayToGuid( buffer.readArray(0x10) )
        else
          throw new Error(sprintf('Unsupported guid size %d at offset 0x%04X', dataLength - 1, buffer.position))
    when 'UDT'
      value = readMaxBinary(buffer)
    else
      throw new Error(sprintf('Unrecognised type %s at offset 0x%04X', type.name, buffer.position))
      break

  value

readBinary = (buffer, dataLength) ->
  if dataLength == NULL
    null
  else
    buffer.readBuffer(dataLength)

readChars = (buffer, dataLength, codepage=DEFAULT_ENCODING) ->
  if dataLength == NULL
    null
  else
    iconv.decode(buffer.readBuffer(dataLength), codepage)

readNChars = (buffer, dataLength) ->
  if dataLength == NULL
    null
  else
    buffer.readString(dataLength, 'ucs2')

readMaxBinary = (buffer) ->
  readMax(buffer, (valueBuffer) ->
    valueBuffer
  )

readMaxChars = (buffer, codepage=DEFAULT_ENCODING) ->
  readMax(buffer, (valueBuffer) ->
    iconv.decode(valueBuffer, codepage)
  )

readMaxNChars = (buffer) ->
  readMax(buffer, (valueBuffer) ->
    valueBuffer.toString('ucs2')
  )

readMax = (buffer, decodeFunction) ->
  type = buffer.readBuffer(8)
  if (type.equals(PLP_NULL))
    null
  else
    if (type.equals(UNKNOWN_PLP_LEN))
      expectedLength = undefined
    else
      buffer.rollback()
      expectedLength = buffer.readUInt64LE()

    length = 0
    chunks = []

    # Read, and accumulate, chunks from buffer.
    chunkLength = buffer.readUInt32LE()
    while (chunkLength != 0)
      length += chunkLength
      chunks.push(buffer.readBuffer(chunkLength))

      chunkLength = buffer.readUInt32LE()

    if expectedLength
      if length != expectedLength
        throw new Error("Partially Length-prefixed Bytes unmatched lengths : expected #{expectedLength}, but got #{length} bytes")

    # Assemble all of the chunks in to one Buffer.
    valueBuffer = new Buffer(length)
    position = 0
    for chunk in chunks
      chunk.copy(valueBuffer, position, 0)
      position += chunk.length

    decodeFunction(valueBuffer)

readSmallDateTime = (buffer, useUTC) ->
  days = buffer.readUInt16LE()
  minutes = buffer.readUInt16LE()

  if useUTC
    value = new Date(Date.UTC(1900, 0, 1))
    value.setUTCDate(value.getUTCDate() + days)
    value.setUTCMinutes(value.getUTCMinutes() + minutes)
  else
    value = new Date(1900, 0, 1)
    value.setDate(value.getDate() + days)
    value.setMinutes(value.getMinutes() + minutes)
    
  value

readDateTime = (buffer, useUTC) ->
  days = buffer.readInt32LE()
  threeHundredthsOfSecond = buffer.readUInt32LE()
  milliseconds = threeHundredthsOfSecond * THREE_AND_A_THIRD

  if useUTC
    value = new Date(Date.UTC(1900, 0, 1))
    value.setUTCDate(value.getUTCDate() + days)
    value.setUTCMilliseconds(value.getUTCMilliseconds() + milliseconds)
  else
    value = new Date(1900, 0, 1)
    value.setDate(value.getDate() + days)
    value.setMilliseconds(value.getMilliseconds() + milliseconds)
    
  value

readTime = (buffer, dataLength, scale) ->
  switch dataLength
    when 3 then value = buffer.readUInt24LE()
    when 4 then value = buffer.readUInt32LE()
    when 5 then value = buffer.readUInt40LE()

  if scale < 7
	  value *= 10 for i in [scale+1..7]
  
  date = new Date(Date.UTC(1970, 0, 1, 0, 0, 0, value / 10000))
  Object.defineProperty date, "nanosecondsDelta",
    enumerable: false
    value: (value % 10000) / Math.pow(10, 7)

  date

readDate = (buffer) ->
  days = buffer.readUInt24LE()
  
  new Date(Date.UTC(2000, 0, days - 730118))

readDateTime2 = (buffer, dataLength, scale) ->
  time = readTime buffer, dataLength - 3, scale
  days = buffer.readUInt24LE()

  date = new Date(Date.UTC(2000, 0, days - 730118, 0, 0, 0, +time))
  Object.defineProperty date, "nanosecondsDelta",
    enumerable: false
    value: time.nanosecondsDelta
    
  date

readDateTimeOffset = (buffer, dataLength, scale) ->
  time = readTime buffer, dataLength - 5, scale
  days = buffer.readUInt24LE()
  offset = buffer.readInt16LE()

  date = new Date(Date.UTC(2000, 0, days - 730118, 0, 0, 0, +time))
  Object.defineProperty date, "nanosecondsDelta",
    enumerable: false
    value: time.nanosecondsDelta
    
  date

module.exports = parse
