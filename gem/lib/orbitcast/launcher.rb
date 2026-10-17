# frozen_string_literal: true

require_relative "error"
require_relative "version"
require_relative "binary"

module Orbitcast
  # `exe/orbitcast` implementation.
  #
  # The launcher is not a supervisor: it resolves the executable, optionally
  # boots Rails *once* to export configuration, then replaces its own process
  # image with `Process.exec`. argv, stdio, exit status, and signals therefore
  # belong to the Rust process directly.
  #
  # This file must stay loadable without Rails. It deliberately does not
  # `require "orbitcast"`: doing so before `Bundler.require` would mark the gem
  # loaded and `Orbitcast::Engine` would never be registered with the
  # application.
  class Launcher
    # Commands that must never boot Rails or touch PostgreSQL.
    PASSTHROUGH_FLAGS = %w[-h --help -V --version].freeze
    PASSTHROUGH_COMMANDS = %w[help version].freeze

    # Flags of the Rust CLI (`src/main.rs`) that consume the next argv element
    # as their value, so `orbitcast -l debug serve` is a `serve`, not a
    # `debug`. The `--flag=value` spelling needs no entry.
    VALUE_FLAGS = %w[-l --log-level].freeze

    # A bare `orbitcast` is `orbitcast serve`, exactly as the Rust binary
    # treats it.
    DEFAULT_COMMAND = "serve"

    # Non-secret process environment forwarded to the child in *both* modes.
    # Anything not listed here, in EXPORTED_KEYS, or (direct mode only) an
    # `ORBITCAST_*` key of the parent is dropped (`unsetenv_others: true`).
    #
    # The `ORBITCAST_*` entries here are read by the Rust relay but never
    # produced by the Rails export: they only ever arrive from the parent
    # environment (operator/Procfile), so they are inherited, not exported.
    INHERITED_KEYS = %w[
      PATH HOME LANG LC_ALL LC_CTYPE TZ TMPDIR TERM NO_COLOR
      SSL_CERT_FILE SSL_CERT_DIR
      RUST_LOG RUST_BACKTRACE
      ORBITCAST_CONFIG
      ORBITCAST_LOG
      ORBITCAST_MAX_MESSAGE_SIZE
      ORBITCAST_MAX_CONNECTIONS
      ORBITCAST_SHUTDOWN_TIMEOUT_MS
      ORBITCAST_PUBLIC_STREAMS
      ORBITCAST_RATE_SUBSCRIBES_PER_CONN_PER_SEC
    ].freeze

    # `ORBITCAST_*` keys the Rails export may produce; every key here is read
    # by the Rust relay (`src/config.rs`). A value already present in the
    # parent environment wins over the Rails-exported value: precedence is
    # CLI > environment > Rails/TOML configuration > defaults.
    EXPORTED_KEYS = %w[
      ORBITCAST_NAMESPACE
      ORBITCAST_DATABASE_URL
      ORBITCAST_LISTENER_DATABASE_URL
      ORBITCAST_LISTEN_ADDR
      ORBITCAST_ALLOWED_ORIGINS
      ORBITCAST_TRUSTED_PROXIES
      ORBITCAST_OUTBOUND_QUEUE_MESSAGES
      ORBITCAST_OUTBOUND_QUEUE_BYTES
      ORBITCAST_OVERFLOW_ENABLED
      ORBITCAST_OVERFLOW_TTL_MS
      ORBITCAST_MAX_PUBLICATION_SIZE
      ORBITCAST_TURBO_STREAMS
      ORBITCAST_STREAMS_SECRET_BASE64
      ORBITCAST_STREAMS_PREVIOUS_SECRETS_BASE64
      ORBITCAST_TURBO_SECRET_BASE64
      ORBITCAST_TURBO_PREVIOUS_SECRETS_BASE64
      ORBITCAST_RECOVERY_SECRET_BASE64
      ORBITCAST_RPC_ENDPOINT_URL
      ORBITCAST_RPC_TOKEN_BASE64
    ].freeze

    # Application secrets that must never cross into the relay process, even if
    # someone adds them to an allowlist by accident.
    FORBIDDEN_KEYS = %w[RAILS_MASTER_KEY SECRET_KEY_BASE].freeze

    # Environments that get a loopback listener for free. Anything else must
    # configure one: the relay exits with NoTransport otherwise.
    DEFAULT_LISTENER_ENVIRONMENTS = %w[development test].freeze

    def self.call(argv, env: ENV, cwd: Dir.pwd)
      new(argv, env: env, cwd: cwd).run
    end

    # @param application [#config_for, #config] the booted Rails application;
    #   tests inject a stand-in, production resolves `::Rails.application`
    #   after the boot
    # @param rails_env [String] the Rails environment name; same rule
    def initialize(argv, env: ENV, cwd: Dir.pwd, application: nil, rails_env: nil)
      @argv = argv.dup.freeze
      @env = env
      @cwd = cwd
      @application = application
      @rails_env = rails_env
    end

    attr_reader :argv, :env, :cwd

    def run
      return exec_direct(verify: false) if passthrough?
      return exec_serve if command == DEFAULT_COMMAND && rails_application?

      # Direct invocation: TOML/environment configured, no Rails boot.
      exec_direct
    end

    # The child environment for `serve`. Pure function of the exported values
    # and the parent environment, so it is testable without Rails.
    def child_env(exported = {})
      exported.each_key do |key|
        next if EXPORTED_KEYS.include?(key.to_s)

        raise ConfigurationError,
              "refusing to export #{key} to the orbitcast process; " \
              "allowed keys: #{EXPORTED_KEYS.join(", ")}"
      end

      child = {}
      INHERITED_KEYS.each { |key| child[key] = env[key] if env[key] }
      exported.each { |key, value| child[key.to_s] = value.to_s unless value.nil? }
      # Parent ORBITCAST_* overrides win over exported Rails configuration.
      EXPORTED_KEYS.each { |key| child[key] = env[key] if env[key] && !env[key].empty? }

      strip_forbidden(child)
    end

    # The child environment for a direct (non-Rails) invocation: the inherited
    # allowlist plus every `ORBITCAST_*` key the operator set. The parent's
    # other variables — application database URLs, cloud credentials, the
    # Rails master key — never reach the relay.
    def direct_env
      child = {}
      INHERITED_KEYS.each { |key| child[key] = env[key] if env[key] }
      env.each_pair do |key, value|
        child[key] = value if key.to_s.start_with?("ORBITCAST_")
      end

      strip_forbidden(child)
    end

    # The positional command, ignoring flags and the values they consume.
    # `nil` never: a bare invocation is `serve`.
    def command
      args = argv.dup

      until args.empty?
        arg = args.shift
        return args.first || DEFAULT_COMMAND if arg == "--"
        next if arg.start_with?("--") && arg.include?("=")

        if VALUE_FLAGS.include?(arg)
          args.shift
          next
        end
        next if arg.start_with?("-")

        return arg
      end

      DEFAULT_COMMAND
    end

    def passthrough?
      argv.any? { |arg| PASSTHROUGH_FLAGS.include?(arg) } ||
        PASSTHROUGH_COMMANDS.include?(command)
    end

    # Binary resolution. An `ORBITCAST_BINARY` override is checked with
    # `doctor --json` unless this is a `--help`/`--version` passthrough.
    def binary_path(verify: true, require_features: [])
      Binary.path(env: env, verify: verify, require_features: require_features)
    end

    # The relay needs a listener. Development and test get loopback; anything
    # else must say where to bind, in Rails configuration or in the relay's
    # own environment.
    def resolve_listen_addr!(config, rails_env)
      return config if config.listen_addr
      return config if env["ORBITCAST_LISTEN_ADDR"] && !env["ORBITCAST_LISTEN_ADDR"].empty?

      if DEFAULT_LISTENER_ENVIRONMENTS.include?(rails_env.to_s)
        config.listen_addr = Configuration::DEFAULT_DEVELOPMENT_LISTEN_ADDR
        return config
      end

      raise ConfigurationError,
            "listen_addr is not configured for the #{rails_env} environment and " \
            "ORBITCAST_LISTEN_ADDR is not set: the relay would start with no transport. Set " \
            "config.orbitcast.listen_addr (e.g. \"0.0.0.0:28080\") or export ORBITCAST_LISTEN_ADDR."
    end

    # `config/cable.yml` for the current environment, minus ActionCable's own
    # `adapter`/`id` keys. A missing file is fine (the initializer may have
    # configured everything); an unreadable or malformed one is not — a typo
    # in the file must not silently become an empty configuration.
    def cable_configuration(application = self.application)
      return {} unless File.file?(File.join(cwd, "config", "cable.yml"))

      settings =
        begin
          application.config_for(:cable)
        rescue StandardError => e
          raise ConfigurationError,
                "config/cable.yml could not be loaded (#{e.class}: #{e.message}); fix the file " \
                "rather than starting a relay with an empty configuration"
        end

      settings = settings.to_h.transform_keys(&:to_sym)
      adapter = settings[:adapter]&.to_s
      # Another adapter's keys are not Orbitcast settings; leave them alone.
      return {} if adapter && adapter != "orbitcast"

      settings.except(:adapter, :id)
    end

    # `config.action_cable.allowed_request_origins`, for the relay's own
    # origin check. The relay matches literal origins only.
    def rails_cable_env(application = self.application)
      origins = Array(application.config.action_cable.allowed_request_origins)

      origins.each do |origin|
        next unless origin.is_a?(Regexp)

        raise ConfigurationError,
              "config.action_cable.allowed_request_origins contains a Regexp (#{origin.inspect}); " \
              "the Orbitcast relay matches literal origins only. List each allowed origin as a " \
              "string, e.g. [\"https://app.example\"]."
      end

      strings = origins.map(&:to_s).reject(&:empty?)
      exported = {}
      exported["ORBITCAST_ALLOWED_ORIGINS"] = strings.join(",") unless strings.empty?
      exported
    end

    # Everything `serve` exports, given a booted application. Separated from
    # the boot so it is testable with a stand-in application.
    def exported_configuration(application = self.application, rails_env = self.rails_env)
      # `orbitcast` is fully loaded by Bundler at this point, so referencing the
      # constant here (not at the top of the file) is intentional.
      config = ::Orbitcast.config
      config.assign(cable_configuration(application))
      resolve_listen_addr!(config, rails_env)
      config.validate!
      config.exported_env.merge(rails_cable_env(application))
    end

    private

    def application
      @application || ::Rails.application
    end

    def rails_env
      @rails_env || ::Rails.env.to_s
    end

    def environment_file
      File.join(cwd, "config", "environment.rb")
    end

    def rails_application?
      File.file?(environment_file)
    end

    # Boot Rails once, in this process, purely to read configuration.
    def exec_serve
      require environment_file

      exported = exported_configuration
      features = ::Orbitcast.config.publish_via == :postgres ? ["postgres"] : []

      exec_process(child_env(exported), *argv, require_features: features)
    end

    # Direct mode is configured through ORBITCAST_* / TOML by whoever started
    # us; the allowlist still applies.
    def exec_direct(verify: true)
      exec_process(direct_env, *argv, verify: verify)
    end

    def strip_forbidden(child)
      FORBIDDEN_KEYS.each { |key| child.delete(key) }
      child
    end

    def exec_process(child, *args, verify: true, require_features: [])
      binary = binary_path(verify: verify, require_features: require_features)
      # [path, argv0] form: never shell-interpreted, even with no arguments and
      # a path containing shell metacharacters.
      command = [binary, binary]

      Process.exec(child, command, *args, close_others: true, unsetenv_others: true)
    end
  end
end
