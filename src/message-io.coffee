require('./buffertools')
EventEmitter = require('events').EventEmitter
isPacketComplete = require('./packet').isPacketComplete
packetLength = require('./packet').packetLength
packetHeaderLength = require('./packet').HEADER_LENGTH
Packet = require('./packet').Packet
TYPE = require('./packet').TYPE

Concentrate = require('concentrate')

class MessageIO extends EventEmitter
  constructor: (@socket, @_packetSize, @debug) ->
    @socket.addListener('data', @eventData)

    @packetDataSize = @_packetSize - packetHeaderLength
    @packetBuffer = new Buffer(0)
    @payloadBuffer = new Buffer(0)

  eventData: (data) =>
    if (@packetBuffer.length > 0)
      @packetBuffer = Buffer.concat([@packetBuffer, data])
    else
      @packetBuffer = data

    packetsData = []
    endOfMessage = false

    while isPacketComplete(@packetBuffer)
      length = packetLength(@packetBuffer)
      packet = Packet.fromBuffer(@packetBuffer.slice(0, length))
      @logPacket('Received', packet);

      packetsData.push(packet.data)
      if (packet.last)
        endOfMessage = true

      @packetBuffer = @packetBuffer.slice(length)

    if packetsData.length > 0
      @emit('data', Buffer.concat(packetsData))
      if endOfMessage
        @emit('message')

  packetSize: (packetSize) ->
    if arguments.length > 0
      @debug.log("Packet size changed from #{@_packetSize} to #{packetSize}")
      @_packetSize = packetSize
      @packetDataSize = @_packetSize - packetHeaderLength

    @_packetSize

  tlsNegotiationStarting: (securePair) ->
    @securePair = securePair
    @tlsNegotiationInProgress = true;

  encryptAllFutureTraffic: () ->
    @socket.removeAllListeners('data')
    @securePair.encrypted.removeAllListeners('data')

    @socket.pipe(@securePair.encrypted)
    @securePair.encrypted.pipe(@socket)

    @securePair.cleartext.addListener('data', @eventData)

    @tlsNegotiationInProgress = false;

  # TODO listen for 'drain' event when socket.write returns false.
  # TODO implement incomplete request cancelation (2.2.1.6)
  sendMessage: (packetType, data, resetConnection) ->
    if data
      numberOfPackets = (Math.floor((data.length - 1) / @packetDataSize)) + 1
    else
      numberOfPackets = 1
      data = new Buffer 0

    for packetNumber in [0..numberOfPackets - 1]
      payloadStart = packetNumber * @packetDataSize
      if packetNumber < numberOfPackets - 1
        payloadEnd = payloadStart + @packetDataSize
      else
        payloadEnd = data.length
      packetPayload = data.slice(payloadStart, payloadEnd)

      packet = new Packet(packetType)
      packet.last = packetNumber == numberOfPackets - 1
      packet.resetConnection = true if resetConnection
      packet.packetId = packetNumber + 1
      packet.data = packetPayload

      @sendPacket(packet)

  sendPacket: (packet) =>
    @logPacket('Sent', packet)

    targetStream = if @tlsNegotiationInProgress && packet.type != TYPE.PRELOGIN
      # LOGIN7 packet.
      #   Something written to cleartext stream will initiate TLS handshake.
      #   Will not emerge from the encrypted stream until after negotiation has completed.
      @securePair.cleartext
    else if @securePair && !@tlsNegotiationInProgress
      @securePair.cleartext
    else
      @socket

    c = new Concentrate()
    c.pipe(targetStream)

    c.uint8(packet.type)
    c.uint8(packet.status)
    c.uint16be(packet.length)
    c.uint16be(packet.spid)
    c.uint8(packet.packetId)
    c.uint8(packet.window)
    c.flush()
    c.buffer(packet.data)
    c.flush()
    c.unpipe()

  logPacket: (direction, packet) ->
    @debug.packet(direction, packet)
    @debug.data(packet)

module.exports = MessageIO
