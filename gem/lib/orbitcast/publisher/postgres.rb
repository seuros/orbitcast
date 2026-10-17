# frozen_string_literal: true

require "json"
require "openssl"
require "securerandom"

require_relative "../error"
require_relative "../streams"
require_relative "instrumentation"

module Orbitcast
  module Publisher
    # Direct PostgreSQL publisher.
    #
    # Rails encodes a broadcast, this wraps it in a versioned notice envelope and
    # emits `SELECT pg_notify($1, $2)` on a bounded, fork-safe connection pool.
    # Delivery back to the publishing node happens through that node's own
    # PostgreSQL listener, so there is no local fan-out here.
    #
    # A publication too large to inline either raises `Orbitcast::PayloadTooLarge`
    # or, with `overflow_enabled`, is relayed: the row and its reference are
    # written in one transaction, so `NOTIFY` becomes visible only once the row
    # is readable, and a rollback sends nothing.
    #
    # ## Which connection
    #
    # `config.connection` picks it:
    #
    # * `:pool` (default) — a bounded, fork-safe dedicated `pg` pool for
    #   `database_url`. Independent of Active Record: a broadcast is visible as
    #   soon as the statement commits, whatever the application's transaction
    #   is doing. Safe behind PgBouncer transaction pooling.
    # * `:active_record` — the application's Active Record connection
    #   (`ActiveRecord::Base.with_connection { |c| c.raw_connection }`). The
    #   `NOTIFY` (and the overflow row) join the surrounding
    #   `ActiveRecord::Base.transaction`, so a rollback sends nothing and a
    #   commit publishes atomically with the application's writes. The cable
    #   database must then be the application's primary database.
    #
    # Either way, every publish emits `publish.orbitcast` through
    # `ActiveSupport::Notifications` (`stream:`, `bytes:`, `via: :postgres`),
    # including when it raises.
    class Postgres
      # PostgreSQL's default NOTIFY payload limit is 8000 bytes *including* the
      # terminator; the encoded notice must stay strictly below it.
      MAX_NOTIFY_BYTES = 8000

      CHANNEL_PREFIX = "orbitcast_"
      CHANNEL_DIGEST_LENGTH = 48

      NOTICE_VERSION = 1
      PUBLICATION_VERSION = 1
      INLINE_KIND = "inline"
      OVERFLOW_KIND = "overflow"

      NOTIFY_SQL = "SELECT pg_notify($1, $2)"

      # Column contract shared with the Rust server and
      # db/orbitcast_migrate/001_create_orbitcast_payloads.rb. `expires_at` is
      # computed from the *database* clock, never from this process's clock.
      OVERFLOW_INSERT_SQL = <<~SQL.freeze
        INSERT INTO orbitcast_payloads (id, namespace, stream, json_payload, expires_at)
        VALUES ($1::text::uuid, $2, $3, $4, now() + ($5::bigint * interval '1 second'))
      SQL

      MIGRATION_HINT =
        "run the overflow relay migration on the cable database " \
        "(bin/rails orbitcast:install:migrations && bin/rails db:migrate)"

      # Routing, not authorization: `orbitcast_` + the first 48 lowercase hex
      # characters of SHA-256 over the length-prefixed UTF-8 (namespace, stream)
      # tuple. 58 ASCII bytes, below PostgreSQL's 63-byte identifier limit.
      #
      # Length prefixes make the tuple unambiguous: ("ab", "c") and ("a", "bc")
      # must not collide. Both parts are hashed as raw bytes (`String#b`) so a
      # Unicode stream name does not raise an encoding error during concat.
      def self.channel_name(namespace, stream)
        namespace = namespace.to_s
        stream = stream.to_s

        tuple = +"".b
        tuple << [namespace.bytesize].pack("N") << namespace.b
        tuple << [stream.bytesize].pack("N") << stream.b

        "#{CHANNEL_PREFIX}#{OpenSSL::Digest::SHA256.hexdigest(tuple)[0, CHANNEL_DIGEST_LENGTH]}"
      end

      # @param configuration [Orbitcast::Configuration]
      # @param connection_factory [#call] override used by tests; must return an
      #   object responding to `exec_params`, `close`, and `finished?`
      # @param active_record [#with_connection, #transaction] the Active Record
      #   base used by `connection: :active_record`; defaults to
      #   `::ActiveRecord::Base`, loaded on first use
      # @param instrumenter [#instrument] defaults to
      #   `ActiveSupport::Notifications` when loaded
      def initialize(configuration = Orbitcast.config, connection_factory: nil,
                     active_record: nil, instrumenter: nil)
        @configuration = configuration.validate!
        @active_record = active_record
        @instrumenter = Instrumentation.resolve(instrumenter)
        @pool =
          if configuration.active_record_connection?
            nil
          else
            Pool.new(
              size: configuration.pool_size,
              timeout: configuration.pool_timeout,
              &(connection_factory || method(:connect))
            )
          end
      end

      attr_reader :configuration

      # @param stream [String] logical stream name
      # @param payload [String] JSON text already encoded by ActionCable; it is
      #   embedded verbatim, never re-encoded
      # @return [String] publication UUID
      def publish(stream, payload)
        # Both guards run before anything is instrumented or touches SQL: an
        # invalid stream (empty, over 1024 UTF-8 bytes) or a non-String payload
        # is a caller error, not a publication that failed.
        stream = Streams.validated_stream(stream)
        payload = validated_payload(payload)

        Instrumentation.publish(@instrumenter, stream: stream, bytes: payload.bytesize, via: :postgres) do
          if payload.bytesize > configuration.max_publication_size
            raise PayloadTooLarge,
                  "publication payload is #{payload.bytesize} bytes; the configured maximum is " \
                  "#{configuration.max_publication_size} bytes (max_publication_size). The overflow " \
                  "relay carries large Turbo HTML, not arbitrarily large objects."
          end

          publication_id = SecureRandom.uuid
          notice = inline_notice(publication_id, stream, payload)
          channel = self.class.channel_name(configuration.namespace, stream)

          if notice.bytesize < MAX_NOTIFY_BYTES
            run { |connection| connection.exec_params(NOTIFY_SQL, [channel, notice]) }
            next publication_id
          end

          unless configuration.overflow_enabled?
            raise PayloadTooLarge,
                  "encoded notice is #{notice.bytesize} bytes; PostgreSQL NOTIFY payloads must be " \
                  "under #{MAX_NOTIFY_BYTES} bytes. Enable the overflow relay " \
                  "(config.orbitcast.overflow_enabled = true) and #{MIGRATION_HINT}, or broadcast a " \
                  "smaller payload (for example a reference the client re-fetches)."
          end

          publish_overflow(channel, publication_id, stream, payload)
          publication_id
        end
      end

      def shutdown
        @pool&.shutdown
      end

      private

      # Store the payload and notify its reference in ONE transaction.
      #
      # A listener that receives the reference is therefore guaranteed to be
      # able to read the row (NOTIFY is delivered on commit), and a failure
      # anywhere in the sequence sends nothing and leaves no row: the pool
      # discards a connection whose statement raised, and closing it aborts the
      # open transaction server-side. On the Active Record connection the same
      # two statements join the application's transaction instead.
      def publish_overflow(channel, publication_id, stream, payload)
        notice = overflow_notice(publication_id, stream)

        run(transaction: true) do |connection|
          connection.exec_params(
            OVERFLOW_INSERT_SQL,
            [publication_id, configuration.namespace, stream, payload, configuration.overflow_ttl]
          )
          connection.exec_params(NOTIFY_SQL, [channel, notice])
        end
      rescue StandardError => e
        raise OverflowTableMissing, "#{MIGRATION_HINT}: #{e.message}" if overflow_table_missing?(e)

        raise
      end

      # Yield a raw PostgreSQL connection (`#exec_params`) from wherever the
      # configuration says it comes from, optionally inside a transaction.
      def run(transaction: false, &block)
        return run_on_active_record(transaction, &block) if configuration.active_record_connection?

        @pool.with do |connection|
          next yield(connection) unless transaction

          connection.exec_params("BEGIN", [])
          result = yield(connection)
          connection.exec_params("COMMIT", [])
          result
        end
      end

      # `ActiveRecord::Base.transaction` joins an open transaction rather than
      # starting a second one, which is the whole point: the NOTIFY commits —
      # or rolls back — together with the application's own writes.
      def run_on_active_record(transaction)
        base = active_record
        body = -> { base.with_connection { |connection| yield connection.raw_connection } }

        transaction ? base.transaction(&body) : body.call
      end

      def active_record
        @active_record ||= begin
          require "active_record"
          ::ActiveRecord::Base
        end
      end

      # PG raises `PG::UndefinedTable` (SQLSTATE 42P01); the class check is
      # guarded so this file still works with a test double.
      def overflow_table_missing?(error)
        return true if defined?(PG::UndefinedTable) && error.is_a?(PG::UndefinedTable)

        error.message.to_s.match?(/orbitcast_payloads.*does not exist/m)
      end

      def inline_notice(publication_id, stream, payload)
        JSON.generate(
          version: NOTICE_VERSION,
          kind: INLINE_KIND,
          publication: {
            version: PUBLICATION_VERSION,
            namespace: configuration.namespace,
            id: publication_id,
            stream: stream,
            json_payload: payload
          }
        )
      end

      # The reference is routing, not authorization: the server re-validates
      # namespace and stream against the row it fetches.
      def overflow_notice(publication_id, stream)
        JSON.generate(
          version: NOTICE_VERSION,
          kind: OVERFLOW_KIND,
          id: publication_id,
          namespace: configuration.namespace,
          stream: stream
        )
      end

      def validated_payload(payload)
        unless payload.is_a?(String)
          raise ArgumentError,
                "payload must be the JSON text encoded by ActionCable, got #{payload.class}"
        end

        unless payload.valid_encoding?
          raise ArgumentError, "payload is not valid #{payload.encoding}"
        end

        payload.encoding == Encoding::UTF_8 ? payload : payload.encode(Encoding::UTF_8)
      end

      def connect
        require "pg"
        PG.connect(configuration.database_url)
      end

      # Bounded, fork-safe connection pool.
      #
      # No `connection_pool` dependency: this needs one primitive, and the
      # fork-safety rule (a child must never touch a socket inherited from its
      # parent) is easier to state than to configure.
      class Pool
        def initialize(size:, timeout:, &factory)
          @size = Integer(size)
          @timeout = Float(timeout)
          @factory = factory
          @mutex = Mutex.new
          @resource = ConditionVariable.new
          @available = []
          @created = 0
          @pid = Process.pid
          @shutdown = false
        end

        def with
          connection = checkout
          begin
            yield connection
          rescue StandardError
            # A connection that raised may be in an unknown protocol state.
            discard(connection)
            raise
          else
            checkin(connection)
          end
        end

        def shutdown
          connections =
            @mutex.synchronize do
              @shutdown = true
              taken = @available
              @available = []
              @created = 0
              taken
            end

          connections.each { |connection| close(connection) }
          nil
        end

        private

        def checkout
          deadline = now + @timeout

          loop do
            connection = nil

            @mutex.synchronize do
              raise Orbitcast::Error, "publisher pool is shut down" if @shutdown

              discard_after_fork

              if (pooled = @available.pop)
                connection = pooled
              elsif @created < @size
                @created += 1
                connection = :build
              else
                remaining = deadline - now
                if remaining <= 0
                  raise PoolTimeout,
                        "timed out after #{@timeout}s waiting for one of #{@size} orbitcast " \
                        "publisher connections"
                end
                @resource.wait(@mutex, remaining)
              end
            end

            next if connection.nil?
            return connection unless connection == :build

            return build
          end
        end

        def build
          @factory.call
        rescue StandardError
          @mutex.synchronize do
            @created -= 1
            @resource.signal
          end
          raise
        end

        def checkin(connection)
          closed =
            @mutex.synchronize do
              if @shutdown || @pid != Process.pid
                @created -= 1
                true
              else
                @available.push(connection)
                @resource.signal
                false
              end
            end

          close(connection) if closed
        end

        def discard(connection)
          @mutex.synchronize do
            @created -= 1
            @resource.signal
          end
          close(connection)
        end

        # Sockets inherited across `fork` belong to the parent process: drop the
        # references without closing them, and let this process open its own.
        def discard_after_fork
          return if @pid == Process.pid

          @available = []
          @created = 0
          @pid = Process.pid
        end

        def close(connection)
          connection.close if connection.respond_to?(:close)
        rescue StandardError
          nil
        end

        def now
          Process.clock_gettime(Process::CLOCK_MONOTONIC)
        end
      end
    end
  end
end
