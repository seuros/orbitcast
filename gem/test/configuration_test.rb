# frozen_string_literal: true

require "test_helper"
require "base64"

class ConfigurationTest < Minitest::Test
  def test_defaults
    config = Orbitcast::Configuration.new

    assert_equal :postgres, config.publish_via
    assert_equal :signed, config.mode
    assert_nil config.namespace
    assert_nil config.database_url
    assert_equal Orbitcast::Configuration::DEFAULT_POOL_SIZE, config.pool_size
  end

  def test_assign_accepts_string_keys_and_ignores_adapter
    config = Orbitcast::Configuration.new
    config.assign("adapter" => "orbitcast", "namespace" => "demo", "publish_via" => "postgres")

    assert_equal "demo", config.namespace
    assert_equal :postgres, config.publish_via
  end

  def test_assign_rejects_unknown_key
    error = assert_raises(Orbitcast::ConfigurationError) do
      Orbitcast::Configuration.new.assign(namesapce: "typo")
    end

    assert_includes error.message, "unknown Orbitcast setting :namesapce"
    assert_kind_of ArgumentError, error
  end

  def test_assign_rejects_non_hash
    assert_raises(Orbitcast::ConfigurationError) { Orbitcast::Configuration.new.assign("nope") }
  end

  def test_publish_via_rejects_unsupported_backend
    error = assert_raises(Orbitcast::ConfigurationError) do
      Orbitcast::Configuration.new.publish_via = :redis
    end

    assert_includes error.message, "publish_via :redis is not supported"
  end

  def test_mode_supports_signed_and_dispatch
    config = Orbitcast::Configuration.new
    config.mode = "signed"

    assert_equal :signed, config.mode
    refute_predicate config, :dispatch?

    config.mode = :dispatch

    assert_predicate config, :dispatch?
    assert_raises(Orbitcast::ConfigurationError) { config.mode = :rails }
  end

  # --- Rails dispatch -------------------------------------------------------

  def test_dispatch_mode_requires_a_service_credential
    config = build_config(mode: :dispatch)
    error = assert_raises(Orbitcast::ConfigurationError) { config.validate! }

    assert_includes error.message, "rpc_token is required"

    config.rpc_token = "service-token"

    assert_same config, config.validate!
  end

  def test_the_endpoint_is_mounted_at_a_private_path_by_default
    assert_equal "/_orbitcast/v1/rails", Orbitcast::Configuration.new.rpc_mount_path

    config = Orbitcast::Configuration.new
    config.rpc_mount_path = "/internal/orbitcast/"

    assert_equal "/internal/orbitcast", config.rpc_mount_path
    assert_raises(Orbitcast::ConfigurationError) { config.rpc_mount_path = "orbitcast" }
    assert_raises(Orbitcast::ConfigurationError) { config.rpc_mount_path = "" }
  end

  def test_the_channel_allowlist_only_accepts_names_or_auto
    config = Orbitcast::Configuration.new

    assert_nil config.channels

    config.channels = %w[ChatChannel]

    assert_equal %w[ChatChannel], config.channels

    config.channels = :auto

    assert_equal :auto, config.channels
    assert_raises(Orbitcast::ConfigurationError) { config.channels = "ChatChannel" }
  end

  def test_rpc_endpoint_and_ingress_urls_must_be_http
    config = Orbitcast::Configuration.new

    assert_raises(Orbitcast::ConfigurationError) { config.rpc_endpoint_url = "app.internal/rails" }
    assert_raises(Orbitcast::ConfigurationError) { config.ingress_url = "unix:///tmp/sock" }

    config.rpc_endpoint_url = "https://app.internal/_orbitcast/v1/rails"

    assert_equal "https://app.internal/_orbitcast/v1/rails", config.rpc_endpoint_url
  end

  # Rails holds the raw token and compares it in constant time; the relay gets
  # the same bytes as strict base64, because it decodes key material rather
  # than guessing a UTF-8 interpretation.
  def test_the_service_credential_is_exported_as_base64_of_the_raw_token
    config = build_config(
      mode: :dispatch,
      rpc_token: "s3rvice-token",
      rpc_endpoint_url: "https://app.internal/_orbitcast/v1/rails"
    )
    env = config.validate!.exported_env

    assert_equal "s3rvice-token", config.rpc_token
    assert_equal Base64.strict_encode64("s3rvice-token"), env["ORBITCAST_RPC_TOKEN_BASE64"]
    assert_equal "https://app.internal/_orbitcast/v1/rails", env["ORBITCAST_RPC_ENDPOINT_URL"]
    refute env.key?("ORBITCAST_RPC_TOKEN"), "the raw token is never exported"
  end

  def test_a_signed_mode_deployment_exports_no_rpc_credential
    env = build_config(rpc_token: "unused").exported_env

    refute env.key?("ORBITCAST_RPC_TOKEN_BASE64")
    refute env.key?("ORBITCAST_RPC_ENDPOINT_URL")
  end

  def test_every_exported_rpc_key_is_one_the_launcher_allows
    require "orbitcast/launcher"

    env = build_config(
      mode: :dispatch, rpc_token: "t", rpc_endpoint_url: "http://app.internal/x"
    ).exported_env

    env.each_key do |key|
      assert_includes Orbitcast::Launcher::EXPORTED_KEYS, key
    end
  end

  def test_publish_via_http_requires_an_ingress_url
    error = assert_raises(Orbitcast::ConfigurationError) do
      build_config(publish_via: :http).validate!
    end

    assert_includes error.message, "ingress_url"

    config = build_config(publish_via: :http, ingress_url: "http://127.0.0.1:28081/v1/publications")

    assert_same config, config.validate!
  end

  def test_secrets_never_reach_an_inspect_string
    config = build_config(
      mode: :dispatch, rpc_token: "s3rvice-token", ingress_token: "ingress-credential"
    )

    refute_includes config.inspect, "s3rvice-token"
    refute_includes config.inspect, "ingress-credential"
    assert_includes config.inspect, "[REDACTED]"
  end

  def test_namespace_validation
    config = Orbitcast::Configuration.new

    assert_raises(Orbitcast::ConfigurationError) { config.namespace = "" }
    assert_raises(Orbitcast::ConfigurationError) { config.namespace = "has space" }
    assert_raises(Orbitcast::ConfigurationError) { config.namespace = "_leading" }
    assert_raises(Orbitcast::ConfigurationError) { config.namespace = "x" * 65 }
    assert_raises(Orbitcast::ConfigurationError) { config.namespace = :demo }

    config.namespace = "app.production-1"

    assert_equal "app.production-1", config.namespace
  end

  def test_database_url_must_be_postgres_dsn
    config = Orbitcast::Configuration.new

    assert_raises(Orbitcast::ConfigurationError) { config.database_url = "mysql2://localhost/db" }

    config.database_url = "postgresql://localhost/db"

    assert_equal "postgresql://localhost/db", config.database_url
  end

  def test_secrets_must_be_non_empty_strings
    config = Orbitcast::Configuration.new

    assert_raises(Orbitcast::ConfigurationError) { config.streams_secret = "" }
    assert_raises(Orbitcast::ConfigurationError) { config.turbo_secret = 42 }
  end

  def test_pool_settings_must_be_positive
    config = Orbitcast::Configuration.new

    assert_raises(Orbitcast::ConfigurationError) { config.pool_size = 0 }
    assert_raises(Orbitcast::ConfigurationError) { config.pool_size = "many" }
    assert_raises(Orbitcast::ConfigurationError) { config.pool_timeout = -1 }
  end

  # --- overflow relay -------------------------------------------------------

  def test_overflow_relay_is_disabled_by_default
    config = Orbitcast::Configuration.new

    refute_predicate config, :overflow_enabled?
    assert_equal Orbitcast::Configuration::DEFAULT_OVERFLOW_TTL, config.overflow_ttl
    assert_equal(
      Orbitcast::Configuration::DEFAULT_MAX_PUBLICATION_SIZE,
      config.max_publication_size
    )
  end

  def test_overflow_settings_are_assignable
    config = build_config(
      overflow_enabled: true, overflow_ttl: 60, max_publication_size: 262_144
    )

    assert_predicate config, :overflow_enabled?
    assert_equal 60, config.overflow_ttl
    assert_equal 262_144, config.max_publication_size
  end

  def test_overflow_enabled_rejects_non_boolean_values
    config = Orbitcast::Configuration.new

    assert_raises(Orbitcast::ConfigurationError) { config.overflow_enabled = "yes" }
    assert_raises(Orbitcast::ConfigurationError) { config.overflow_enabled = nil }

    config.overflow_enabled = "true"

    assert_predicate config, :overflow_enabled?
  end

  def test_overflow_bounds_must_be_usable
    config = Orbitcast::Configuration.new

    assert_raises(Orbitcast::ConfigurationError) { config.overflow_ttl = 0 }
    assert_raises(Orbitcast::ConfigurationError) { config.overflow_ttl = "soon" }
    # Below PostgreSQL's NOTIFY limit the ceiling would reject payloads that
    # need no relay at all.
    error = assert_raises(Orbitcast::ConfigurationError) { config.max_publication_size = 4_096 }

    assert_includes error.message, "8000"
  end

  def test_exported_env_carries_the_overflow_settings_to_the_server
    env = build_config(
      overflow_enabled: true, overflow_ttl: 90, max_publication_size: 524_288
    ).exported_env

    assert_equal "true", env["ORBITCAST_OVERFLOW_ENABLED"]
    assert_equal "90000", env["ORBITCAST_OVERFLOW_TTL_MS"]
    assert_equal "524288", env["ORBITCAST_MAX_PUBLICATION_SIZE"]
  end

  def test_exported_env_reports_a_disabled_relay_explicitly
    assert_equal "false", build_config.exported_env["ORBITCAST_OVERFLOW_ENABLED"]
  end

  def test_validate_requires_namespace_and_database_url
    config = Orbitcast::Configuration.new

    error = assert_raises(Orbitcast::ConfigurationError) { config.validate! }
    assert_includes error.message, "namespace is required"

    config.namespace = "demo"
    error = assert_raises(Orbitcast::ConfigurationError) { config.validate! }
    assert_includes error.message, "database_url is required"

    config.database_url = Orbitcast::TestHelper::DATABASE_URL

    assert_same config, config.validate!
  end

  def test_streams_secret_bang_explains_missing_key
    config = build_config(streams_secret: nil)

    error = assert_raises(Orbitcast::ConfigurationError) { config.streams_secret! }
    assert_includes error.message, "streams_secret is not configured"
  end

  def test_exported_env_uses_strict_base64_for_key_material
    config = build_config(turbo_secret: "turbo-key")
    env = config.exported_env

    assert_equal Orbitcast::TestHelper::NAMESPACE, env["ORBITCAST_NAMESPACE"]
    assert_equal Orbitcast::TestHelper::DATABASE_URL, env["ORBITCAST_DATABASE_URL"]
    # mode/publish_via are Ruby-side concerns; the relay reads neither.
    refute env.key?("ORBITCAST_MODE")
    refute env.key?("ORBITCAST_PUBLISH_VIA")
    assert_equal(
      Orbitcast::TestHelper::STREAMS_SECRET,
      Base64.strict_decode64(env["ORBITCAST_STREAMS_SECRET_BASE64"])
    )
    assert_equal "turbo-key", Base64.strict_decode64(env["ORBITCAST_TURBO_SECRET_BASE64"])
    refute_includes env.values, Orbitcast::TestHelper::STREAMS_SECRET
  end

  def test_exported_env_omits_absent_secrets
    env = build_config(streams_secret: nil).exported_env

    refute env.key?("ORBITCAST_STREAMS_SECRET_BASE64")
    refute env.key?("ORBITCAST_TURBO_SECRET_BASE64")
  end

  def test_inspect_redacts_secrets_and_password
    config = build_config(
      database_url: "postgres://cable:hunter2@db.internal/orbitcast",
      turbo_secret: "turbo-key"
    )
    text = config.inspect

    refute_includes text, "hunter2"
    refute_includes text, Orbitcast::TestHelper::STREAMS_SECRET
    refute_includes text, "turbo-key"
    assert_includes text, "[REDACTED]"
  end

  # --- relay transport, tuning, rotation ----------------------------------------

  # MOCK: `ActiveSupport::KeyGenerator#generate_key(salt, size)`; records the
  # purpose it was asked for and returns deterministic bytes of that size.
  class FakeKeyGenerator
    attr_reader :requests

    def initialize
      @requests = []
    end

    def generate_key(salt, size)
      @requests << [salt, size]
      ("k" * size).b
    end
  end

  def test_listen_addr_must_be_host_and_port
    config = Orbitcast::Configuration.new

    assert_nil config.listen_addr
    assert_raises(Orbitcast::ConfigurationError) { config.listen_addr = "28080" }
    assert_raises(Orbitcast::ConfigurationError) { config.listen_addr = "localhost:port" }

    config.listen_addr = "[::1]:28080"

    assert_equal "[::1]:28080", config.listen_addr
    assert_equal "[::1]:28080", build_config(listen_addr: "[::1]:28080").exported_env["ORBITCAST_LISTEN_ADDR"]
    refute build_config.exported_env.key?("ORBITCAST_LISTEN_ADDR")
  end

  def test_listener_database_url_must_be_a_postgres_dsn
    config = Orbitcast::Configuration.new

    assert_raises(Orbitcast::ConfigurationError) { config.listener_database_url = "mysql2://x/y" }

    env = build_config(listener_database_url: "postgres://listener@db/cable").exported_env

    assert_equal "postgres://listener@db/cable", env["ORBITCAST_LISTENER_DATABASE_URL"]
  end

  def test_trusted_proxies_are_validated_cidrs_and_exported_comma_separated
    config = Orbitcast::Configuration.new

    assert_empty config.trusted_proxies
    assert_raises(Orbitcast::ConfigurationError) { config.trusted_proxies = ["not-a-cidr"] }
    assert_raises(Orbitcast::ConfigurationError) { config.trusted_proxies = 42 }

    config.trusted_proxies = ["10.0.0.0/8", "fd00::/8", "127.0.0.1"]

    assert_equal ["10.0.0.0/8", "fd00::/8", "127.0.0.1"], config.trusted_proxies

    env = build_config(trusted_proxies: "10.0.0.0/8, 192.168.0.0/16").exported_env

    assert_equal "10.0.0.0/8,192.168.0.0/16", env["ORBITCAST_TRUSTED_PROXIES"]
    refute build_config.exported_env.key?("ORBITCAST_TRUSTED_PROXIES")
  end

  def test_outbound_queue_limits_are_positive_integers
    config = Orbitcast::Configuration.new

    assert_nil config.outbound_queue_messages
    assert_raises(Orbitcast::ConfigurationError) { config.outbound_queue_messages = 0 }
    assert_raises(Orbitcast::ConfigurationError) { config.outbound_queue_bytes = "lots" }

    env = build_config(outbound_queue_messages: 512, outbound_queue_bytes: "1048576").exported_env

    assert_equal "512", env["ORBITCAST_OUTBOUND_QUEUE_MESSAGES"]
    assert_equal "1048576", env["ORBITCAST_OUTBOUND_QUEUE_BYTES"]
  end

  def test_previous_secrets_are_exported_as_comma_separated_strict_base64
    env = build_config(
      turbo_secret: "turbo-now",
      streams_previous_secrets: ["old-one", "\x00\xff-binary".b],
      turbo_previous_secrets: ["turbo-old"]
    ).exported_env

    assert_equal(
      [Base64.strict_encode64("old-one"), Base64.strict_encode64("\x00\xff-binary".b)].join(","),
      env["ORBITCAST_STREAMS_PREVIOUS_SECRETS_BASE64"]
    )
    assert_equal Base64.strict_encode64("turbo-old"), env["ORBITCAST_TURBO_PREVIOUS_SECRETS_BASE64"]
    assert_equal "true", env["ORBITCAST_TURBO_STREAMS"]
  end

  def test_previous_secrets_must_be_arrays_of_non_empty_strings
    config = Orbitcast::Configuration.new

    assert_raises(Orbitcast::ConfigurationError) { config.streams_previous_secrets = "old" }
    assert_raises(Orbitcast::ConfigurationError) { config.streams_previous_secrets = [""] }
    assert_raises(Orbitcast::ConfigurationError) { config.turbo_previous_secrets = [nil] }

    config.streams_previous_secrets = nil

    assert_empty config.streams_previous_secrets
    refute build_config.exported_env.key?("ORBITCAST_STREAMS_PREVIOUS_SECRETS_BASE64")
  end

  # Turbo previous keys without a current Turbo key verify nothing: the Turbo
  # profile is off, so they are not exported either.
  def test_turbo_previous_secrets_need_a_current_turbo_key
    env = build_config(turbo_previous_secrets: ["turbo-old"]).exported_env

    refute env.key?("ORBITCAST_TURBO_STREAMS")
    refute env.key?("ORBITCAST_TURBO_PREVIOUS_SECRETS_BASE64")
  end

  def test_turbo_streams_is_exported_exactly_when_a_turbo_key_is_configured
    assert_equal "true", build_config(turbo_secret: "t").exported_env["ORBITCAST_TURBO_STREAMS"]
    refute build_config.exported_env.key?("ORBITCAST_TURBO_STREAMS")
  end

  def test_connection_is_pool_or_active_record
    config = Orbitcast::Configuration.new

    assert_equal :pool, config.connection
    refute_predicate config, :active_record_connection?

    config.connection = "active_record"

    assert_predicate config, :active_record_connection?
    assert_raises(Orbitcast::ConfigurationError) { config.connection = :redis }
    refute build_config(connection: :active_record).exported_env.key?("ORBITCAST_CONNECTION")
  end

  # --- derived stream key ---------------------------------------------------------

  def test_the_stream_key_is_derived_from_the_key_generator_under_a_namespaced_purpose
    generator = FakeKeyGenerator.new
    config = build_config(streams_secret: nil)
    config.key_generator = generator

    assert_equal ("k" * 32).b, config.streams_secret
    assert_equal ("k" * 32).b, config.streams_secret!
    assert_predicate config, :streams_secret_derived?
    assert_equal [["orbitcast/streams/v1/#{Orbitcast::TestHelper::NAMESPACE}", 32]], generator.requests
    assert_equal(
      Base64.strict_encode64(("k" * 32).b), config.exported_env["ORBITCAST_STREAMS_SECRET_BASE64"]
    )
    assert_includes config.inspect, "[DERIVED]"
  end

  def test_an_explicit_stream_key_wins_over_derivation
    generator = FakeKeyGenerator.new
    config = build_config
    config.key_generator = generator

    assert_equal Orbitcast::TestHelper::STREAMS_SECRET, config.streams_secret
    refute_predicate config, :streams_secret_derived?
    assert_empty generator.requests, "the generator is never consulted"
  end

  def test_derivation_follows_the_namespace_and_needs_one
    generator = FakeKeyGenerator.new
    config = Orbitcast::Configuration.new
    config.key_generator = generator

    assert_nil config.streams_secret, "no namespace, no key"
    assert_raises(Orbitcast::ConfigurationError) { config.streams_secret! }

    config.namespace = "first"
    config.streams_secret
    config.namespace = "second"
    config.streams_secret

    assert_equal(
      [["orbitcast/streams/v1/first", 32], ["orbitcast/streams/v1/second", 32]], generator.requests
    )
  end

  def test_the_derivation_purpose_is_the_documented_string
    assert_equal "orbitcast/streams/v1/example_production", build_config.streams_key_purpose
    assert_equal 32, Orbitcast::Configuration::STREAMS_KEY_SIZE
  end
end
