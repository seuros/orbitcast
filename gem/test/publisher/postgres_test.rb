# frozen_string_literal: true

require "test_helper"
require "json"

class PublisherPostgresTest < Minitest::Test
  Publisher = Orbitcast::Publisher::Postgres

  # MOCK: stands in for a `PG::Connection`. Only `exec_params` and `close` are
  # exercised, which is the whole surface the publisher uses. No database is
  # started for these tests.
  class FakeConnection
    attr_reader :calls

    def initialize(fail_with: nil)
      @calls = []
      @fail_with = fail_with
      @closed = false
    end

    def exec_params(sql, params)
      raise @fail_with if @fail_with

      @calls << [sql, params]
      [{ "pg_notify" => nil }]
    end

    def close
      @closed = true
    end

    def closed? = @closed
  end

  # --- channel-name golden vectors -----------------------------------------
  #
  # `orbitcast_` + first 48 hex chars of SHA-256 over
  # pack("N", namespace.bytesize) + namespace + pack("N", stream.bytesize) + stream.
  # These exact strings become shared Rust/Ruby fixtures; changing them is a wire
  # break, not a refactor.

  def test_channel_name_golden_vector_one
    assert_equal(
      "orbitcast_75ba535bd0c12e8f0990bf756607b4f5ddb148f2e22070e1",
      Publisher.channel_name("example_production", "chat:42")
    )
  end

  def test_channel_name_golden_vector_two
    assert_equal(
      "orbitcast_c5748d585378540c557a6893ad3d8866d449a7673b2590b9",
      Publisher.channel_name("demo", "posts:1")
    )
  end

  def test_channel_name_golden_vector_unicode
    assert_equal(
      "orbitcast_b630c21a8012cd05b047cec754aad9601a46823df496aa85",
      Publisher.channel_name("café", "stream:ünïcode")
    )
  end

  def test_channel_name_fits_postgres_identifier_limit
    name = Publisher.channel_name("x" * 64, "y" * 512)

    assert_equal 58, name.bytesize
    assert_match(/\Aorbitcast_[0-9a-f]{48}\z/, name)
  end

  def test_length_prefix_prevents_tuple_collisions
    refute_equal Publisher.channel_name("ab", "c"), Publisher.channel_name("a", "bc")
  end

  # --- notice envelope ------------------------------------------------------

  def test_publish_emits_a_parameterized_inline_notice
    connection = FakeConnection.new
    publisher = build_publisher(connection)

    publication_id = publisher.publish("chat:42", '{"message":"hi"}')

    assert_equal 1, connection.calls.size
    sql, params = connection.calls.first

    assert_equal "SELECT pg_notify($1, $2)", sql
    assert_equal Publisher.channel_name(Orbitcast::TestHelper::NAMESPACE, "chat:42"), params[0]

    notice = JSON.parse(params[1])

    assert_equal 1, notice["version"]
    assert_equal "inline", notice["kind"]
    assert_equal 1, notice.dig("publication", "version")
    assert_equal Orbitcast::TestHelper::NAMESPACE, notice.dig("publication", "namespace")
    assert_equal "chat:42", notice.dig("publication", "stream")
    assert_equal publication_id, notice.dig("publication", "id")
    assert_match(/\A[0-9a-f-]{36}\z/, publication_id)

    # The application payload is embedded verbatim, not re-encoded.
    assert_equal '{"message":"hi"}', notice.dig("publication", "json_payload")
  end

  def test_each_publication_has_a_unique_id
    connection = FakeConnection.new
    publisher = build_publisher(connection)

    ids = 3.times.map { publisher.publish("chat:42", "{}") }

    assert_equal 3, ids.uniq.size
  end

  def test_payload_larger_than_the_notify_limit_is_rejected
    connection = FakeConnection.new
    publisher = build_publisher(connection)
    payload = JSON.generate(html: "x" * 8_000)

    error = assert_raises(Orbitcast::PayloadTooLarge) { publisher.publish("chat:42", payload) }

    assert_includes error.message, "8000 bytes"
    assert_includes error.message, "overflow_enabled"
    assert_includes error.message, "db:migrate"
    assert_empty connection.calls, "oversized payloads must not reach the database"
  end

  def test_escaping_overhead_counts_toward_the_limit
    connection = FakeConnection.new
    publisher = build_publisher(connection)

    # Each quote costs two bytes once embedded in json_payload; a payload that
    # fits raw must still be rejected when the envelope pushes it over.
    payload = JSON.generate(Array.new(1_300) { '"' })

    assert_operator payload.bytesize, :<, Publisher::MAX_NOTIFY_BYTES
    assert_raises(Orbitcast::PayloadTooLarge) { publisher.publish("chat:42", payload) }
  end

  def test_payload_just_below_the_limit_is_published
    connection = FakeConnection.new
    publisher = build_publisher(connection)
    envelope_overhead = JSON.generate(html: "").bytesize

    publisher.publish("chat:42", JSON.generate(html: "x" * (7_000 - envelope_overhead)))

    assert_equal 1, connection.calls.size
  end

  # --- overflow relay -------------------------------------------------------

  # Big enough that no envelope overhead can bring it back under the limit.
  def oversized_payload(marker = "x")
    JSON.generate(marker: marker, html: "y" * 9_000)
  end

  def test_overflow_writes_the_row_and_the_reference_in_one_transaction
    connection = FakeConnection.new
    publisher = build_publisher(connection, overflow_enabled: true)
    payload = oversized_payload

    publication_id = publisher.publish("chat:42", payload)

    statements = connection.calls.map(&:first)

    assert_equal(
      ["BEGIN", Orbitcast::Publisher::Postgres::OVERFLOW_INSERT_SQL, "SELECT pg_notify($1, $2)",
       "COMMIT"],
      statements,
      "the row and its reference must commit together"
    )

    _, insert_params = connection.calls[1]

    assert_equal publication_id, insert_params[0]
    assert_equal Orbitcast::TestHelper::NAMESPACE, insert_params[1]
    assert_equal "chat:42", insert_params[2]
    assert_equal payload, insert_params[3], "the payload is stored verbatim"
    assert_equal Orbitcast::Configuration::DEFAULT_OVERFLOW_TTL, insert_params[4]
  end

  def test_overflow_notice_carries_only_the_reference
    connection = FakeConnection.new
    publisher = build_publisher(connection, overflow_enabled: true)

    publication_id = publisher.publish("chat:42", oversized_payload)

    channel, notice = connection.calls[2][1]

    assert_equal Publisher.channel_name(Orbitcast::TestHelper::NAMESPACE, "chat:42"), channel
    assert_operator notice.bytesize, :<, Publisher::MAX_NOTIFY_BYTES

    # Matches tests/fixtures/publications/valid_overflow_ref.json exactly: no
    # payload travels, and the reference is not authorization.
    assert_equal(
      {
        "version" => 1,
        "kind" => "overflow",
        "id" => publication_id,
        "namespace" => Orbitcast::TestHelper::NAMESPACE,
        "stream" => "chat:42"
      },
      JSON.parse(notice)
    )
  end

  def test_small_payloads_stay_inline_when_overflow_is_enabled
    connection = FakeConnection.new
    publisher = build_publisher(connection, overflow_enabled: true)

    publisher.publish("chat:42", '{"message":"hi"}')

    assert_equal ["SELECT pg_notify($1, $2)"], connection.calls.map(&:first)
    assert_equal "inline", JSON.parse(connection.calls.first[1][1])["kind"]
  end

  def test_payload_above_max_publication_size_is_rejected_before_any_sql
    connection = FakeConnection.new
    publisher = build_publisher(connection, overflow_enabled: true, max_publication_size: 9_000)
    payload = JSON.generate(html: "y" * 9_000)

    error = assert_raises(Orbitcast::PayloadTooLarge) { publisher.publish("chat:42", payload) }

    assert_includes error.message, "max_publication_size"
    assert_empty connection.calls, "the ceiling is checked before the database is touched"
  end

  def test_missing_relay_table_names_the_migration
    connection = FakeConnection.new(
      fail_with: RuntimeError.new(%(ERROR: relation "orbitcast_payloads" does not exist))
    )
    publisher = build_publisher(connection, overflow_enabled: true)

    error = assert_raises(Orbitcast::OverflowTableMissing) do
      publisher.publish("chat:42", oversized_payload)
    end

    assert_includes error.message, "db:migrate"
    assert connection.closed?, "a failed transaction must not return to the pool"
  end

  def test_a_failed_overflow_transaction_discards_the_connection
    connection = FakeConnection.new(fail_with: RuntimeError.new("server closed the connection"))
    replacement = FakeConnection.new
    queue = [connection, replacement]
    publisher = Publisher.new(
      build_config(overflow_enabled: true), connection_factory: -> { queue.shift }
    )

    assert_raises(RuntimeError) { publisher.publish("chat:42", oversized_payload) }
    assert connection.closed?, "closing aborts the open transaction server-side"
  end

  def test_rejects_unencoded_payloads_and_empty_streams
    publisher = build_publisher(FakeConnection.new)

    assert_raises(ArgumentError) { publisher.publish("chat:42", { message: "hi" }) }
    assert_raises(ArgumentError) { publisher.publish("", "{}") }
    assert_raises(ArgumentError) { publisher.publish(nil, "{}") }
  end

  def test_requires_a_valid_configuration
    config = Orbitcast::Configuration.new
    config.namespace = "demo"

    assert_raises(Orbitcast::ConfigurationError) { Publisher.new(config) }
  end

  # --- pool behaviour -------------------------------------------------------

  def test_connections_are_reused_not_reopened_per_broadcast
    connection = FakeConnection.new
    built = 0
    publisher = Publisher.new(build_config, connection_factory: lambda {
      built += 1
      connection
    })

    5.times { publisher.publish("chat:42", "{}") }

    assert_equal 1, built
    assert_equal 5, connection.calls.size
  end

  def test_failed_statements_discard_the_connection
    connection = FakeConnection.new(fail_with: RuntimeError.new("server closed the connection"))
    replacement = FakeConnection.new
    queue = [connection, replacement]
    publisher = Publisher.new(build_config, connection_factory: -> { queue.shift })

    assert_raises(RuntimeError) { publisher.publish("chat:42", "{}") }
    assert connection.closed?, "a connection that raised must not go back into the pool"

    publisher.publish("chat:42", "{}")

    assert_equal 1, replacement.calls.size
  end

  def test_shutdown_closes_pooled_connections
    connection = FakeConnection.new
    publisher = build_publisher(connection)
    publisher.publish("chat:42", "{}")

    publisher.shutdown

    assert connection.closed?
    assert_raises(Orbitcast::Error) { publisher.publish("chat:42", "{}") }
  end

  def test_pool_times_out_instead_of_growing
    held = Queue.new
    release = Queue.new
    publisher = Publisher.new(
      build_config(pool_size: 1, pool_timeout: 0.1),
      connection_factory: -> { FakeConnection.new }
    )

    holder = Thread.new do
      publisher.instance_variable_get(:@pool).with do |_connection|
        held << true
        release.pop
      end
    end
    held.pop

    assert_raises(Orbitcast::PoolTimeout) { publisher.publish("chat:42", "{}") }
  ensure
    release&.<<(true)
    holder&.join(1)
  end

  private

  def build_publisher(connection, **config_overrides)
    Publisher.new(build_config(**config_overrides), connection_factory: -> { connection })
  end

  # --- stream guard ---------------------------------------------------------------

  def test_streams_over_1024_bytes_are_rejected_before_any_sql
    connection = FakeConnection.new
    publisher = build_publisher(connection)

    publisher.publish("x" * 1024, "{}")

    assert_equal 1, connection.calls.size

    error = assert_raises(ArgumentError) { publisher.publish("x" * 1025, "{}") }

    assert_includes error.message, "1024"
    assert_equal 1, connection.calls.size, "the oversized stream never reached the database"

    # Bytes, not characters: four-byte emoji count four.
    assert_raises(ArgumentError) { publisher.publish("🚀" * 257, "{}") }
  end

  # --- instrumentation --------------------------------------------------------------

  # MOCK: records what `ActiveSupport::Notifications.instrument` would see,
  # including the failure path (the event still fires, the error re-raises).
  class RecordingInstrumenter
    attr_reader :events

    def initialize
      @events = []
    end

    def instrument(name, payload = {})
      yield payload
    rescue StandardError => e
      payload[:exception] = [e.class.name, e.message]
      raise
    ensure
      @events << [name, payload]
    end
  end

  def test_every_publish_is_instrumented_with_stream_bytes_and_transport
    instrumenter = RecordingInstrumenter.new
    publisher = Publisher.new(
      build_config, connection_factory: -> { FakeConnection.new }, instrumenter: instrumenter
    )

    publisher.publish("chat:42", '{"message":"hi"}')

    assert_equal(
      [["publish.orbitcast", { stream: "chat:42", bytes: '{"message":"hi"}'.bytesize, via: :postgres }]],
      instrumenter.events
    )
  end

  def test_a_failed_publish_is_instrumented_and_the_error_still_raises
    instrumenter = RecordingInstrumenter.new
    publisher = Publisher.new(
      build_config,
      connection_factory: -> { FakeConnection.new(fail_with: RuntimeError.new("server closed the connection")) },
      instrumenter: instrumenter
    )

    assert_raises(RuntimeError) { publisher.publish("chat:42", "{}") }

    name, payload = instrumenter.events.fetch(0)

    assert_equal "publish.orbitcast", name
    assert_equal ["RuntimeError", "server closed the connection"], payload[:exception]
    assert_equal :postgres, payload[:via]
  end

  def test_a_payload_too_large_is_instrumented_too
    instrumenter = RecordingInstrumenter.new
    publisher = Publisher.new(
      build_config, connection_factory: -> { FakeConnection.new }, instrumenter: instrumenter
    )

    assert_raises(Orbitcast::PayloadTooLarge) { publisher.publish("chat:42", oversized_payload) }
    assert_equal "Orbitcast::PayloadTooLarge", instrumenter.events.fetch(0).last[:exception].first
  end

  def test_without_active_support_the_null_instrumenter_is_used
    publisher = Publisher.new(build_config, connection_factory: -> { FakeConnection.new })
    instrumenter = publisher.instance_variable_get(:@instrumenter)

    if defined?(::ActiveSupport::Notifications)
      assert_same ::ActiveSupport::Notifications, instrumenter
    else
      assert_same Orbitcast::Publisher::Instrumentation::Null, instrumenter
    end
  end

  # --- connection: :active_record ------------------------------------------------------

  # MOCK: the slice of `ActiveRecord::Base` the publisher touches —
  # `with_connection` yielding an adapter whose `raw_connection` is the pg
  # connection, and `transaction`, which joins when one is already open.
  class FakeActiveRecord
    Adapter = Struct.new(:raw_connection)

    attr_reader :raw, :log

    def initialize(raw)
      @raw = raw
      @log = []
      @depth = 0
    end

    def with_connection
      @log << :with_connection
      yield Adapter.new(@raw)
    end

    def transaction
      if @depth.positive?
        @log << :joined
        return yield
      end

      @depth += 1
      @log << :begin
      result = yield
      @log << :commit
      result
    rescue StandardError
      @log << :rollback
      raise
    ensure
      @depth -= 1
    end
  end

  def test_active_record_mode_publishes_on_the_application_connection
    raw = FakeConnection.new
    base = FakeActiveRecord.new(raw)
    publisher = Publisher.new(
      build_config(connection: :active_record),
      connection_factory: -> { raise "the dedicated pool must not be used" },
      active_record: base
    )

    publisher.publish("chat:42", '{"message":"hi"}')

    assert_equal ["SELECT pg_notify($1, $2)"], raw.calls.map(&:first)
    assert_equal [:with_connection], base.log
    assert_nil publisher.instance_variable_get(:@pool), "no dedicated pool is built"
    publisher.shutdown
  end

  def test_active_record_mode_joins_the_surrounding_transaction
    raw = FakeConnection.new
    base = FakeActiveRecord.new(raw)
    publisher = Publisher.new(build_config(connection: :active_record), active_record: base)

    base.transaction do
      publisher.publish("chat:42", "{}")
      raise "business rule failed"
    rescue RuntimeError
      nil
    end

    assert_equal [:begin, :with_connection, :rollback], base.log,
                 "the NOTIFY ran inside the application's transaction, which rolled back"
  end

  def test_active_record_mode_overflow_uses_active_record_transactions_not_raw_begin_commit
    raw = FakeConnection.new
    base = FakeActiveRecord.new(raw)
    publisher = Publisher.new(
      build_config(connection: :active_record, overflow_enabled: true), active_record: base
    )

    publisher.publish("chat:42", oversized_payload)

    assert_equal(
      [Orbitcast::Publisher::Postgres::OVERFLOW_INSERT_SQL, "SELECT pg_notify($1, $2)"],
      raw.calls.map(&:first),
      "no raw BEGIN/COMMIT: Active Record owns the transaction"
    )
    assert_equal [:begin, :with_connection, :commit], base.log

    # And inside an open transaction it joins rather than nesting.
    base.log.clear
    base.transaction { publisher.publish("chat:42", oversized_payload) }

    assert_equal [:begin, :joined, :with_connection, :commit], base.log
  end

  def test_active_record_mode_still_names_the_missing_relay_table
    raw = FakeConnection.new(fail_with: RuntimeError.new(%(ERROR: relation "orbitcast_payloads" does not exist)))
    publisher = Publisher.new(
      build_config(connection: :active_record, overflow_enabled: true), active_record: FakeActiveRecord.new(raw)
    )

    assert_raises(Orbitcast::OverflowTableMissing) { publisher.publish("chat:42", oversized_payload) }
  end
end
