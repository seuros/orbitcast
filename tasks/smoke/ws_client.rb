# frozen_string_literal: true

# Minimal RFC 6455 WebSocket client for the native-gem smoke test.
#
# Deliberately not a gem: the smoke runs on a freshly provisioned machine (and
# inside an Alpine container) with network installs disabled, so it may only
# depend on the Ruby standard library. It is also deliberately not the
# server's own WebSocket implementation — a client that shares the code under
# test cannot expose a bug in it. Text frames only, client-to-server frames
# masked as the RFC requires, control frames (ping/pong/close) handled inline,
# no extensions, no fragmentation (the relay never fragments ActionCable
# frames).

require "base64"
require "digest/sha1"
require "securerandom"
require "socket"
require "uri"

module OrbitcastSmoke
  class WebSocketError < StandardError; end

  class WebSocket
    GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    OPCODE_CONTINUATION = 0x0
    OPCODE_TEXT = 0x1
    OPCODE_BINARY = 0x2
    OPCODE_CLOSE = 0x8
    OPCODE_PING = 0x9
    OPCODE_PONG = 0xA

    attr_reader :subprotocol

    # @param url [String] ws://host:port/path (plain TCP; TLS terminates upstream)
    # @param subprotocol [String, nil] offered Sec-WebSocket-Protocol
    # @param origin [String, nil] optional Origin header (none = non-browser client)
    def initialize(url, subprotocol: nil, origin: nil, timeout: 10)
      @uri = URI.parse(url)
      raise WebSocketError, "only ws:// is supported, got #{url}" unless @uri.scheme == "ws"

      @timeout = timeout
      @closed = false
      @socket = Socket.tcp(@uri.host, @uri.port, connect_timeout: timeout)
      @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
      @subprotocol = handshake(subprotocol, origin)
    end

    def send_text(text)
      write_frame(OPCODE_TEXT, text.b)
    end

    # Next text frame, or nil on timeout. Pings are answered, pongs ignored,
    # a close frame is acknowledged and returns :closed.
    def read_text(timeout: @timeout)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

      loop do
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        return nil if remaining <= 0

        opcode, payload = read_frame(remaining)
        return nil if opcode.nil?

        case opcode
        when OPCODE_TEXT
          return payload.force_encoding(Encoding::UTF_8)
        when OPCODE_PING
          write_frame(OPCODE_PONG, payload)
        when OPCODE_PONG
          next
        when OPCODE_CLOSE
          begin
            write_frame(OPCODE_CLOSE, payload[0, 2].to_s) unless @closed
          rescue SystemCallError, IOError
            nil
          end
          @closed = true
          return :closed
        when OPCODE_BINARY
          raise WebSocketError, "unexpected binary frame (#{payload.bytesize} bytes)"
        when OPCODE_CONTINUATION
          raise WebSocketError, "fragmented frames are not supported by this client"
        else
          raise WebSocketError, "unknown opcode #{opcode}"
        end
      end
    end

    def close(code = 1000)
      return if @closed

      @closed = true
      write_frame(OPCODE_CLOSE, [code].pack("n"))
    rescue SystemCallError, IOError
      nil
    ensure
      @socket.close unless @socket.closed?
    end

    def closed? = @closed

    private

    def handshake(subprotocol, origin)
      key = Base64.strict_encode64(SecureRandom.random_bytes(16))
      path = @uri.path.empty? ? "/" : @uri.path
      path += "?#{@uri.query}" if @uri.query

      lines = [
        "GET #{path} HTTP/1.1",
        "Host: #{@uri.host}:#{@uri.port}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        "Sec-WebSocket-Key: #{key}",
        "Sec-WebSocket-Version: 13"
      ]
      lines << "Sec-WebSocket-Protocol: #{subprotocol}" if subprotocol
      lines << "Origin: #{origin}" if origin
      @socket.write("#{lines.join("\r\n")}\r\n\r\n")

      status_line = read_line
      raise WebSocketError, "no HTTP status line from #{@uri}" if status_line.nil?

      headers = {}
      while (line = read_line)
        break if line.empty?

        name, value = line.split(":", 2)
        headers[name.to_s.strip.downcase] = value.to_s.strip
      end

      unless status_line.start_with?("HTTP/1.1 101")
        raise WebSocketError, "upgrade refused: #{status_line.inspect} #{headers.inspect}"
      end

      expected = Base64.strict_encode64(Digest::SHA1.digest(key + GUID))
      unless headers["sec-websocket-accept"] == expected
        raise WebSocketError, "bad Sec-WebSocket-Accept: expected #{expected}, got #{headers["sec-websocket-accept"].inspect}"
      end

      negotiated = headers["sec-websocket-protocol"]
      if subprotocol && negotiated != subprotocol
        raise WebSocketError, "server selected subprotocol #{negotiated.inspect}, offered #{subprotocol.inspect}"
      end

      negotiated
    end

    def read_line
      line = +""
      loop do
        byte = read_exact(1, @timeout)
        return nil if byte.nil?

        line << byte
        return line.chomp("\r\n") if line.end_with?("\r\n")
        raise WebSocketError, "handshake line too long" if line.bytesize > 8192
      end
    end

    # Client frames MUST be masked (RFC 6455 section 5.3).
    def write_frame(opcode, payload)
      payload = payload.b
      header = [0x80 | opcode].pack("C")
      length = payload.bytesize

      header << if length < 126
        [0x80 | length].pack("C")
      elsif length < 65_536
        [0x80 | 126, length].pack("Cn")
      else
        [0x80 | 127, length].pack("CQ>")
      end

      mask = SecureRandom.random_bytes(4)
      masked = payload.bytes.each_with_index.map { |b, i| b ^ mask.getbyte(i % 4) }.pack("C*")
      @socket.write(header + mask + masked)
    end

    # @return [Array(Integer, String)] opcode and payload, or [nil, nil] on timeout
    def read_frame(timeout)
      head = read_exact(2, timeout)
      return [nil, nil] if head.nil?

      first, second = head.unpack("CC")
      fin = (first & 0x80) != 0
      opcode = first & 0x0F
      masked = (second & 0x80) != 0
      length = second & 0x7F

      raise WebSocketError, "server frames must not be masked" if masked
      raise WebSocketError, "fragmented frames are not supported by this client" unless fin

      length = read_exact(2, timeout).unpack1("n") if length == 126
      length = read_exact(8, timeout).unpack1("Q>") if length == 127

      payload = length.zero? ? +"" : read_exact(length, timeout)
      raise WebSocketError, "socket closed mid-frame" if payload.nil?

      [opcode, payload]
    end

    def read_exact(count, timeout)
      buffer = +""
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

      while buffer.bytesize < count
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        return nil if remaining <= 0
        return nil unless @socket.wait_readable(remaining)

        chunk = @socket.read_nonblock(count - buffer.bytesize, exception: false)
        case chunk
        when :wait_readable then next
        when nil
          raise WebSocketError, "socket closed by peer" unless buffer.empty?

          return nil
        else buffer << chunk
        end
      end

      buffer
    end
  end
end
