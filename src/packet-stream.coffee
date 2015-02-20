Transform = require("stream").Transform

packetHeaderLength = require('./packet').HEADER_LENGTH

STATUS =
  NORMAL: 0x00,
  EOM: 0x01,                      # End Of Message (last packet).
  IGNORE: 0x02,                   # EOM must also be set.
  RESETCONNECTION: 0x08,
  RESETCONNECTIONSKIPTRAN: 0x10

module.exports = class PacketStream extends Transform
  constructor: (options) ->
    @packetDataSize = options.packetSize - packetHeaderLength

    status = if options.resetConnection
      STATUS.RESETCONNECTION
    else
      STATUS.NORMAL

    @header = new Buffer(packetHeaderLength)
    @header.writeUInt8(options.type, 0)
    @header.writeUInt8(status, 1)
    @header.writeUInt16BE(options.packetSize, 2)
    @header.writeUInt16BE(0, 4)
    @header.writeUInt8(0, 6) # PacketId
    @header.writeUInt8(0, 7) # Window

    @currentPacket = -1

    @remainder = new Buffer(0)

    super({})

  _transform: (chunk, encoding, done) ->
    if @remainder.length && @remainder.length + chunk.length > @packetDataSize
      this.push(@nextPacketHeader())
      this.push(@remainder)
      this.push(chunk.slice(0, @packetDataSize))
      chunk = chunk.slice(@packetDataSize)

    while chunk.length > @packetDataSize
      this.push(@nextPacketHeader())
      console.log(@header)
      this.push(chunk.slice(0, @packetDataSize))
      console.log(chunk.slice(0, @packetDataSize).length)

    @remainder = chunk
    done()

  _flush: (done) ->
    this.push(@lastPacketHeader())
    console.log(@header)
    this.push(@remainder)
    console.log(@remainder.length)

  nextPacketHeader: () ->
    @currentPacket = (@currentPacket + 1) % 256
    @header.writeUInt8(@currentPacket, 6)
    @header

  lastPacketHeader: () ->
    @nextPacketHeader()
    @header.writeUInt8(@header.readUInt8(1) | STATUS.EOM, 1)
    @header.writeUInt16BE(packetHeaderLength + @remainder.length, 2)
    @header
