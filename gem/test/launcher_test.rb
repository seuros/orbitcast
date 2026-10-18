# frozen_string_literal: true

require "test_helper"
require "orbitcast/launcher"
require "tmpdir"

class LauncherTest < Minitest::Test
  # MOCK: the slice of a booted Rails application the launcher reads —
  # `config_for(:cable)` and `config.action_cable.allowed_request_origins`.
  FakeActionCable = Struct.new(:allowed_request_origins)
  FakeAppConfig = Struct.new(:action_cable)

  class FakeApplication
    attr_reader :config

    def initialize(cable: {}, origins: [], cable_error: nil)
      @cable = cable
      @cable_error = cable_error
      @config = FakeAppConfig.new(FakeActionCable.new(origins))
    end

    def config_for(name)
      raise ArgumentError, "unexpected config_for(#{name.inspect})" unless name == :cable
      raise @cable_error if @cable_error

      @cable
    end
  end

  def test_child_env_keeps_only_allowlisted_keys
    launcher = build_launcher({
      "PATH" => "/usr/bin",
      "HOME" => "/home/app",
      "RAILS_MASTER_KEY" => "master",
      "SECRET_KEY_BASE" => "base",
      "DATABASE_URL" => "postgres://app/primary",
      "AWS_SECRET_ACCESS_KEY" => "nope"
    })

    env = launcher.child_env(build_config(turbo_secret: "turbo-key").exported_env)

    assert_equal "/usr/bin", env["PATH"]
    assert_equal "/home/app", env["HOME"]
    assert_equal Orbitcast::TestHelper::NAMESPACE, env["ORBITCAST_NAMESPACE"]
    assert env.key?("ORBITCAST_STREAMS_SECRET_BASE64")

    refute env.key?("RAILS_MASTER_KEY")
    refute env.key?("SECRET_KEY_BASE")
    refute env.key?("DATABASE_URL")
    refute env.key?("AWS_SECRET_ACCESS_KEY")
  end

  def test_parent_environment_overrides_rails_configuration
    launcher = build_launcher({ "ORBITCAST_NAMESPACE" => "from_env", "ORBITCAST_LOG" => "debug" })

    env = launcher.child_env(build_config.exported_env)

    assert_equal "from_env", env["ORBITCAST_NAMESPACE"]
    assert_equal "debug", env["ORBITCAST_LOG"]
  end

  # Operator-set transport and tuning survive the allowlist; the listener
  # address set by the operator wins over the one Rails exported.
  def test_transport_and_tuning_env_is_inherited_from_the_parent
    launcher = build_launcher({
      "ORBITCAST_LISTEN_ADDR" => "127.0.0.1:8080",
      "ORBITCAST_CONFIG" => "/etc/orbitcast.toml",
      "ORBITCAST_RATE_SUBSCRIBES_PER_CONN_PER_SEC" => "20"
    })

    env = launcher.child_env(build_config(listen_addr: "0.0.0.0:28080").exported_env)

    assert_equal "127.0.0.1:8080", env["ORBITCAST_LISTEN_ADDR"]
    assert_equal "/etc/orbitcast.toml", env["ORBITCAST_CONFIG"]
    assert_equal "20", env["ORBITCAST_RATE_SUBSCRIBES_PER_CONN_PER_SEC"]
  end

  # The Turbo switch is produced by the export whenever a Turbo key is
  # configured; it is no longer something only the operator can set.
  def test_turbo_streams_switch_is_exported_with_the_turbo_key
    env = build_launcher.child_env(build_config(turbo_secret: "turbo-key").exported_env)

    assert_equal "true", env["ORBITCAST_TURBO_STREAMS"]
    assert env.key?("ORBITCAST_TURBO_SECRET_BASE64")

    without = build_launcher.child_env(build_config.exported_env)

    refute without.key?("ORBITCAST_TURBO_STREAMS")
  end

  # Every launcher-handled ORBITCAST_* key must be read by the Rust relay;
  # dead exports (MODE, PUBLISH_VIA, BIND, PORT, ...) were dropped.
  def test_dead_keys_are_not_exported
    %w[ORBITCAST_MODE ORBITCAST_PUBLISH_VIA ORBITCAST_BIND ORBITCAST_PORT ORBITCAST_MOUNT_PATH].each do |key|
      refute_includes Orbitcast::Launcher::EXPORTED_KEYS, key
      refute_includes Orbitcast::Launcher::INHERITED_KEYS, key
    end
  end

  def test_every_configuration_export_is_an_allowlisted_key
    config = build_config(
      turbo_secret: "t", recovery_secret: "r", listen_addr: "127.0.0.1:1",
      listener_database_url: "postgres://l/db", trusted_proxies: ["10.0.0.0/8"],
      outbound_queue_messages: 1, outbound_queue_bytes: 1,
      streams_previous_secrets: ["old"], turbo_previous_secrets: ["old-turbo"],
      mode: :dispatch, rpc_token: "tok", rpc_endpoint_url: "http://app.internal/x"
    )

    config.exported_env.each_key do |key|
      assert_includes Orbitcast::Launcher::EXPORTED_KEYS, key
    end
  end

  def test_exporting_an_unlisted_key_is_refused
    launcher = build_launcher

    error = assert_raises(Orbitcast::ConfigurationError) do
      launcher.child_env({ "ORBITCAST_STREAMS_SECRET" => "raw", "PATH" => "/tmp" })
    end

    assert_includes error.message, "refusing to export"
  end

  def test_forbidden_keys_can_never_be_exported
    Orbitcast::Launcher::FORBIDDEN_KEYS.each do |key|
      refute_includes Orbitcast::Launcher::EXPORTED_KEYS, key
      refute_includes Orbitcast::Launcher::INHERITED_KEYS, key
    end
  end

  # --- direct mode ------------------------------------------------------------

  def test_direct_env_never_hands_the_full_parent_environment_to_the_child
    launcher = build_launcher({
      "PATH" => "/usr/bin",
      "ORBITCAST_LISTEN_ADDR" => "0.0.0.0:28080",
      "ORBITCAST_STREAMS_SECRET_BASE64" => "c2VjcmV0",
      "ORBITCAST_WHATEVER_NEW_KNOB" => "1",
      "RAILS_MASTER_KEY" => "master",
      "SECRET_KEY_BASE" => "base",
      "DATABASE_URL" => "postgres://app/primary",
      "AWS_SECRET_ACCESS_KEY" => "nope"
    }, argv: ["--log-level=debug"])

    env = launcher.direct_env

    assert_equal "/usr/bin", env["PATH"]
    assert_equal "0.0.0.0:28080", env["ORBITCAST_LISTEN_ADDR"]
    assert_equal "c2VjcmV0", env["ORBITCAST_STREAMS_SECRET_BASE64"]
    assert_equal "1", env["ORBITCAST_WHATEVER_NEW_KNOB"], "operator ORBITCAST_* knobs pass through"

    refute env.key?("RAILS_MASTER_KEY")
    refute env.key?("SECRET_KEY_BASE")
    refute env.key?("DATABASE_URL")
    refute env.key?("AWS_SECRET_ACCESS_KEY")
  end

  def test_a_docked_relay_keeps_the_supervisor_environment
    launcher = build_launcher({
      "PATH" => "/usr/bin",
      "MS_PID" => "42",
      "MS_SHIP" => "orbitcast",
      "MS_SOCKET_DIR" => "/tmp/mothership",
      "MS_SOCKET_PATH" => "/tmp/mothership/orbitcast.sock",
      "ORBITCAST_RPC_TOKEN_BASE64" => "dG9rZW4=",
      "DATABASE_URL" => "postgres://app/primary",
      "RAILS_MASTER_KEY" => "master"
    })

    assert_predicate launcher, :docked?
    env = launcher.docked_env
    assert_equal %w[42 orbitcast /tmp/mothership /tmp/mothership/orbitcast.sock],
                 env.values_at("MS_PID", "MS_SHIP", "MS_SOCKET_DIR", "MS_SOCKET_PATH")
    assert_equal "dG9rZW4=", env["ORBITCAST_RPC_TOKEN_BASE64"]
    refute env.key?("DATABASE_URL")
    refute env.key?("RAILS_MASTER_KEY")
    refute_predicate build_launcher({ "MS_PID" => "42" }), :docked?, "MS_SHIP is needed too"
  end

  def test_a_docked_serve_execs_the_relay_without_booting_rails
    Dir.mktmpdir do |dir|
      FileUtils.mkdir_p(File.join(dir, "config"))
      File.write(File.join(dir, "config", "environment.rb"), "raise 'Rails must not boot'\n")
      launcher = build_launcher({ "MS_PID" => "42", "MS_SHIP" => "orbitcast" }, cwd: dir)
      execs = []
      launcher.define_singleton_method(:exec_process) { |child, *args, **| execs << [child, args] }

      launcher.run

      assert_equal 1, execs.size
      assert_equal "orbitcast", execs.first.first["MS_SHIP"]
      assert_equal ["serve"], execs.first.last
    end
  end

  # --- argv ---------------------------------------------------------------------

  def test_command_detection_skips_flag_values
    assert_equal "serve", command(%w[serve])
    assert_equal "serve", command(%w[-l debug serve])
    assert_equal "serve", command(%w[--log-level debug serve])
    assert_equal "serve", command(%w[--log-level=debug serve])
    assert_equal "doctor", command(%w[-l debug doctor])
    assert_equal "doctor", command(%w[-- doctor])
  end

  def test_a_bare_invocation_is_serve
    assert_equal "serve", command([])
    assert_equal "serve", command(%w[-l debug])
    assert_equal "serve", command(%w[--log-level=debug])
  end

  # `--help` / `--version` must never boot Rails or reach PostgreSQL.
  def test_passthrough_detection
    assert passthrough?(["--help"])
    assert passthrough?(["-h"])
    assert passthrough?(["--version"])
    assert passthrough?(["-V"])
    assert passthrough?(["help"])
    assert passthrough?(["serve", "--help"])
    assert passthrough?(%w[-l debug version])

    refute passthrough?([]), "a bare invocation is serve, which boots Rails"
    refute passthrough?(["serve"])
    refute passthrough?(["doctor"])
    refute passthrough?(%w[-l debug serve])
  end

  def test_binary_resolution_honours_the_injected_environment
    launcher = build_launcher({ "ORBITCAST_BINARY" => "/nonexistent/orbitcast" })

    assert_raises(Orbitcast::BinaryNotFoundError) { launcher.binary_path }
  end

  # --- Rails-derived configuration -----------------------------------------------

  def test_a_malformed_cable_yml_raises_instead_of_becoming_empty
    Dir.mktmpdir("orbitcast-launcher") do |dir|
      write_cable_yml(dir)
      application = FakeApplication.new(cable_error: RuntimeError.new("(<unknown>): did not find expected key"))
      launcher = build_launcher({}, cwd: dir)

      error = assert_raises(Orbitcast::ConfigurationError) { launcher.cable_configuration(application) }

      assert_includes error.message, "config/cable.yml could not be loaded"
      assert_includes error.message, "did not find expected key"
    end
  end

  def test_a_cable_yml_whose_erb_fails_raises_too
    Dir.mktmpdir("orbitcast-launcher") do |dir|
      write_cable_yml(dir)
      application = FakeApplication.new(cable_error: KeyError.new("key not found: \"ORBITCAST_DATABASE_URL\""))
      launcher = build_launcher({}, cwd: dir)

      error = assert_raises(Orbitcast::ConfigurationError) { launcher.cable_configuration(application) }

      assert_includes error.message, "ORBITCAST_DATABASE_URL"
    end
  end

  def test_a_missing_cable_yml_is_not_an_error
    Dir.mktmpdir("orbitcast-launcher") do |dir|
      launcher = build_launcher({}, cwd: dir)

      assert_empty launcher.cable_configuration(FakeApplication.new(cable_error: RuntimeError.new("unused")))
    end
  end

  def test_cable_yml_settings_drop_action_cable_own_keys
    Dir.mktmpdir("orbitcast-launcher") do |dir|
      write_cable_yml(dir)
      application = FakeApplication.new(
        cable: { "adapter" => "orbitcast", "id" => "x", "namespace" => "demo", "publish_via" => "postgres" }
      )

      settings = build_launcher({}, cwd: dir).cable_configuration(application)

      assert_equal({ namespace: "demo", publish_via: "postgres" }, settings)
    end
  end

  def test_another_adapters_cable_yml_is_left_alone
    Dir.mktmpdir("orbitcast-launcher") do |dir|
      write_cable_yml(dir)
      application = FakeApplication.new(cable: { "adapter" => "redis", "url" => "redis://x" })

      assert_empty build_launcher({}, cwd: dir).cable_configuration(application)
    end
  end

  def test_literal_allowed_origins_are_exported
    application = FakeApplication.new(origins: ["https://app.example", "", "http://localhost:3000"])

    exported = build_launcher.rails_cable_env(application)

    assert_equal "https://app.example,http://localhost:3000", exported["ORBITCAST_ALLOWED_ORIGINS"]
  end

  def test_regexp_allowed_origins_are_refused_with_an_explanation
    application = FakeApplication.new(origins: ["https://app.example", %r{\Ahttps://.*\.example\z}])

    error = assert_raises(Orbitcast::ConfigurationError) { build_launcher.rails_cable_env(application) }

    assert_includes error.message, "Regexp"
    assert_includes error.message, "literal origins"
  end

  def test_the_listener_defaults_to_loopback_in_development_and_test
    %w[development test].each do |rails_env|
      config = build_config
      build_launcher.resolve_listen_addr!(config, rails_env)

      assert_equal Orbitcast::Configuration::DEFAULT_DEVELOPMENT_LISTEN_ADDR, config.listen_addr, rails_env
    end
  end

  def test_the_listener_is_required_in_production
    config = build_config

    error = assert_raises(Orbitcast::ConfigurationError) do
      build_launcher.resolve_listen_addr!(config, "production")
    end

    assert_includes error.message, "listen_addr"
    assert_includes error.message, "ORBITCAST_LISTEN_ADDR"
    assert_nil config.listen_addr
  end

  def test_a_parent_listen_addr_satisfies_production
    config = build_config
    launcher = build_launcher({ "ORBITCAST_LISTEN_ADDR" => "0.0.0.0:28080" })

    launcher.resolve_listen_addr!(config, "production")

    assert_nil config.listen_addr, "the parent value is inherited, not copied into Rails configuration"
  end

  def test_an_explicit_listen_addr_is_kept_everywhere
    config = build_config(listen_addr: "10.0.0.5:9000")
    build_launcher.resolve_listen_addr!(config, "production")

    assert_equal "10.0.0.5:9000", config.listen_addr
  end

  def test_exported_configuration_merges_cable_yml_the_listener_and_the_origins
    Dir.mktmpdir("orbitcast-launcher") do |dir|
      write_cable_yml(dir)
      application = FakeApplication.new(
        cable: { "adapter" => "orbitcast", "namespace" => "demo", "database_url" => "postgres://cable@db/cable" },
        origins: ["http://localhost:3000"]
      )

      with_global_config(streams_secret: "s") do
        exported = build_launcher({}, cwd: dir).exported_configuration(application, "development")

        assert_equal "demo", exported["ORBITCAST_NAMESPACE"]
        assert_equal "postgres://cable@db/cable", exported["ORBITCAST_DATABASE_URL"]
        assert_equal Orbitcast::Configuration::DEFAULT_DEVELOPMENT_LISTEN_ADDR, exported["ORBITCAST_LISTEN_ADDR"]
        assert_equal "http://localhost:3000", exported["ORBITCAST_ALLOWED_ORIGINS"]
      end
    end
  end

  private

  def build_launcher(env = {}, argv: ["serve"], cwd: Dir.pwd)
    Orbitcast::Launcher.new(argv, env: env, cwd: cwd)
  end

  def command(argv)
    Orbitcast::Launcher.new(argv, env: {}, cwd: Dir.pwd).command
  end

  def passthrough?(argv)
    Orbitcast::Launcher.new(argv, env: {}, cwd: Dir.pwd).passthrough?
  end

  def write_cable_yml(dir)
    require "fileutils"
    FileUtils.mkdir_p(File.join(dir, "config"))
    File.write(File.join(dir, "config", "cable.yml"), "development:\n  adapter: orbitcast\n")
  end
end
