# frozen_string_literal: true

require "base64"
require "ipaddr"

require_relative "error"

module Orbitcast
  # Validated Orbitcast settings.
  #
  # Configuration is deliberately stricter than the tolerant wire envelopes:
  # unknown keys, unsupported modes, and malformed values raise immediately
  # instead of being defaulted into something permissive. Writers validate at the
  # boundary; `validate!` only checks cross-field requirements.
  class Configuration
    PUBLISH_VIA = %i[postgres http].freeze

    # `:signed` — the relay authorizes subscriptions from signed stream names
    #   and runs no application Ruby.
    # `:dispatch` — the relay calls back into Rails for connection
    #   authentication, channel authorization, and actions.
    MODES = %i[signed dispatch].freeze

    # How `Publisher::Postgres` reaches the cable database.
    # `:pool` — a bounded, fork-safe dedicated `pg` pool for `database_url`.
    # `:active_record` — the application's Active Record connection, so a
    #   broadcast joins the surrounding transaction and rolls back with it.
    CONNECTIONS = %i[pool active_record].freeze

    # Every key `assign` accepts. Also the allowlist used by the installer and
    # by `cable.yml` parsing.
    ATTRIBUTES = %i[
      database_url
      namespace
      publish_via
      mode
      streams_secret
      turbo_secret
      recovery_secret
      pool_size
      pool_timeout
      stream_expires_in
      overflow_enabled
      overflow_ttl
      max_publication_size
      rpc_token
      rpc_endpoint_url
      rpc_mount_path
      connection_class
      channels
      ingress_url
      ingress_token
      listen_addr
      listener_database_url
      trusted_proxies
      outbound_queue_messages
      outbound_queue_bytes
      streams_previous_secrets
      turbo_previous_secrets
      connection
    ].freeze

    NAMESPACE_PATTERN = /\A[a-zA-Z0-9][a-zA-Z0-9_.\-]{0,63}\z/
    POSTGRES_URL_PATTERN = %r{\Apostgres(?:ql)?://}
    HTTP_URL_PATTERN = %r{\Ahttps?://}

    # Where the Engine mounts the private RPC endpoint in dispatch mode. The
    # relay appends the operation name to it.
    DEFAULT_RPC_MOUNT_PATH = "/_orbitcast/v1/rails"

    DEFAULT_CONNECTION_CLASS = "ApplicationCable::Connection"

    # Where the relay listens when a development or test application does not
    # say otherwise. Production has no default: an operator must choose.
    DEFAULT_DEVELOPMENT_LISTEN_ADDR = "127.0.0.1:28080"

    # Purpose string for the derived Orbitcast stream key:
    # `Rails.application.key_generator.generate_key(purpose, STREAMS_KEY_SIZE)`
    # with the namespace baked into the purpose, so two namespaces sharing one
    # `secret_key_base` still verify with different keys. Changing it
    # invalidates every outstanding signed name.
    STREAMS_KEY_PURPOSE_FORMAT = "orbitcast/streams/v1/%<namespace>s"
    STREAMS_KEY_SIZE = 32

    DEFAULT_POOL_SIZE = 5
    DEFAULT_POOL_TIMEOUT = 5.0
    DEFAULT_STREAM_EXPIRES_IN = 3600

    # Overflow relay defaults; both match the Rust server's defaults, because
    # both sides read the same `orbitcast_payloads` rows.
    DEFAULT_OVERFLOW_TTL = 300
    DEFAULT_MAX_PUBLICATION_SIZE = 1024 * 1024

    # PostgreSQL's NOTIFY payload limit. A publication limit below it would
    # reject payloads that need no relay at all.
    MIN_MAX_PUBLICATION_SIZE = 8000

    attr_reader :database_url, :namespace, :publish_via, :mode,
                :streams_secret, :turbo_secret, :recovery_secret,
                :pool_size, :pool_timeout, :stream_expires_in,
                :overflow_enabled, :overflow_ttl, :max_publication_size,
                :rpc_token, :rpc_endpoint_url, :rpc_mount_path,
                :connection_class, :channels,
                :ingress_url, :ingress_token,
                :listen_addr, :listener_database_url, :trusted_proxies,
                :outbound_queue_messages, :outbound_queue_bytes,
                :streams_previous_secrets, :turbo_previous_secrets,
                :connection

    # The application's `ActiveSupport::KeyGenerator` (set by the Engine) that
    # derives a stream key when none is configured explicitly. Never assigned
    # from cable.yml or `config.orbitcast`.
    attr_accessor :key_generator

    def initialize
      @database_url = nil
      @namespace = nil
      @publish_via = :postgres
      @mode = :signed
      @streams_secret = nil
      @turbo_secret = nil
      @recovery_secret = nil
      @pool_size = DEFAULT_POOL_SIZE
      @pool_timeout = DEFAULT_POOL_TIMEOUT
      @stream_expires_in = DEFAULT_STREAM_EXPIRES_IN
      # Off by default: the relay requires the orbitcast_payloads migration,
      # and nothing here ever creates it implicitly.
      @overflow_enabled = false
      @overflow_ttl = DEFAULT_OVERFLOW_TTL
      @max_publication_size = DEFAULT_MAX_PUBLICATION_SIZE
      # Rails-dispatch mode. The token is held here in raw form; the launcher
      # exports it to the relay as strict base64.
      @rpc_token = nil
      @rpc_endpoint_url = nil
      @rpc_mount_path = DEFAULT_RPC_MOUNT_PATH
      @connection_class = DEFAULT_CONNECTION_CLASS
      # nil derives the allowlist from app/channels; an Array is the reviewed,
      # explicit form a deployment should ship.
      @channels = nil
      @ingress_url = nil
      @ingress_token = nil
      # Relay transport and tuning. All optional here; the launcher fills the
      # development default and refuses a production boot without a listener.
      @listen_addr = nil
      @listener_database_url = nil
      @trusted_proxies = [].freeze
      @outbound_queue_messages = nil
      @outbound_queue_bytes = nil
      # Verify-only previous keys for rotation.
      @streams_previous_secrets = [].freeze
      @turbo_previous_secrets = [].freeze
      @connection = :pool
      @key_generator = nil
      @derived_streams_secret = nil
    end

    def dispatch?
      mode == :dispatch
    end

    def overflow_enabled?
      @overflow_enabled
    end

    # Bulk assignment from `cable.yml`, `config.orbitcast`, or the installer.
    # Unknown keys are a hard error: a typo must never leave a secret unset.
    def assign(attributes)
      unless attributes.respond_to?(:each_pair)
        raise ConfigurationError, "expected a Hash of Orbitcast settings, got #{attributes.class}"
      end

      attributes.each_pair do |key, value|
        name = key.to_sym
        # `adapter:` is ActionCable's own selector, not an Orbitcast setting.
        next if name == :adapter

        unless ATTRIBUTES.include?(name)
          raise ConfigurationError,
                "unknown Orbitcast setting #{name.inspect}; known settings: #{ATTRIBUTES.join(", ")}"
        end

        public_send(:"#{name}=", value)
      end

      self
    end

    def database_url=(value)
      @database_url = optional_string(:database_url, value)
      return if @database_url.nil? || @database_url.match?(POSTGRES_URL_PATTERN)

      raise ConfigurationError,
            "database_url must be a postgres:// or postgresql:// DSN"
    end

    def namespace=(value)
      namespace = required_string(:namespace, value)
      unless namespace.match?(NAMESPACE_PATTERN)
        raise ConfigurationError,
              "namespace #{namespace.inspect} must be 1-64 chars of [A-Za-z0-9_.-] and start alphanumeric"
      end

      @namespace = namespace
    end

    def publish_via=(value)
      @publish_via = enum_value(:publish_via, value, PUBLISH_VIA)
    end

    def mode=(value)
      @mode = enum_value(:mode, value, MODES)
    end

    def streams_secret=(value)
      @streams_secret = secret(:streams_secret, value)
    end

    # The explicit key when configured; otherwise the key derived from the
    # application's key generator and the namespace (see
    # STREAMS_KEY_PURPOSE_FORMAT). Nil when neither is available.
    def streams_secret
      @streams_secret || derived_streams_secret
    end

    def streams_secret_derived?
      @streams_secret.nil? && !derived_streams_secret.nil?
    end

    # The purpose string the derived key is generated under.
    def streams_key_purpose(namespace = self.namespace)
      raise ConfigurationError, "namespace is required to derive the stream key" if namespace.nil?

      format(STREAMS_KEY_PURPOSE_FORMAT, namespace: namespace)
    end

    def turbo_secret=(value)
      @turbo_secret = secret(:turbo_secret, value)
    end

    def recovery_secret=(value)
      @recovery_secret = secret(:recovery_secret, value)
    end

    def pool_size=(value)
      @pool_size = positive_integer(:pool_size, value)
    end

    def pool_timeout=(value)
      @pool_timeout = positive_float(:pool_timeout, value)
    end

    def stream_expires_in=(value)
      @stream_expires_in = positive_integer(:stream_expires_in, value)
    end

    # Relay oversized publications through `orbitcast_payloads`. Requires the
    # migration on the cable database *and* `overflow_enabled` on the server.
    def overflow_enabled=(value)
      @overflow_enabled = boolean(:overflow_enabled, value)
    end

    # Relay row lifetime in seconds, measured from the database clock.
    def overflow_ttl=(value)
      @overflow_ttl = positive_integer(:overflow_ttl, value)
    end

    # Ceiling on one publication's payload, in bytes.
    def max_publication_size=(value)
      size = positive_integer(:max_publication_size, value)
      if size < MIN_MAX_PUBLICATION_SIZE
        raise ConfigurationError,
              "max_publication_size must be at least #{MIN_MAX_PUBLICATION_SIZE} bytes " \
              "(PostgreSQL's NOTIFY payload limit)"
      end

      @max_publication_size = size
    end

    # The raw service credential the relay presents as `Authorization: Bearer`.
    #
    # Rails holds the **raw** token; `exported_env` hands the relay the same
    # bytes as strict base64 (`ORBITCAST_RPC_TOKEN_BASE64`), because the relay
    # decodes key material rather than guessing a UTF-8 interpretation. The two
    # are the same secret in two encodings, not two secrets.
    def rpc_token=(value)
      @rpc_token = secret(:rpc_token, value)
    end

    # Where the relay reaches this application, e.g.
    # `https://app.internal/_orbitcast/v1/rails`. Exported for convenience; an
    # operator may set ORBITCAST_RPC_ENDPOINT_URL on the relay instead.
    def rpc_endpoint_url=(value)
      @rpc_endpoint_url = optional_string(:rpc_endpoint_url, value)
      return if @rpc_endpoint_url.nil? || @rpc_endpoint_url.match?(HTTP_URL_PATTERN)

      raise ConfigurationError, "rpc_endpoint_url must be an http:// or https:// URL"
    end

    # Where the Engine mounts the private endpoint inside this application.
    def rpc_mount_path=(value)
      path = required_string(:rpc_mount_path, value)
      unless path.start_with?("/")
        raise ConfigurationError, "rpc_mount_path must start with a slash"
      end

      @rpc_mount_path = path.chomp("/")
    end

    def connection_class=(value)
      @connection_class = value.is_a?(Class) ? value : required_string(:connection_class, value)
    end

    # The channel allowlist: an Array of class names, or :auto to derive it
    # from app/channels. A client-supplied channel name that is not on it is
    # never constantized.
    def channels=(value)
      @channels =
        case value
        when nil then nil
        when :auto, "auto" then :auto
        when Array then value.map { |entry| entry.is_a?(Class) ? entry.name : entry.to_s }.freeze
        else
          raise ConfigurationError,
                "channels must be an Array of channel class names or :auto, got #{value.class}"
        end
    end

    def ingress_url=(value)
      @ingress_url = optional_string(:ingress_url, value)
      return if @ingress_url.nil? || @ingress_url.match?(HTTP_URL_PATTERN)

      raise ConfigurationError, "ingress_url must be an http:// or https:// URL"
    end

    def ingress_token=(value)
      @ingress_token = secret(:ingress_token, value)
    end

    # `host:port` the relay binds its public listener to.
    def listen_addr=(value)
      addr = optional_string(:listen_addr, value)
      if addr && !addr.match?(/\A\S+:\d{1,5}\z/)
        raise ConfigurationError, "listen_addr must look like host:port, got #{addr.inspect}"
      end

      @listen_addr = addr
    end

    # A separate DSN for the relay's long-lived LISTEN session (direct or
    # session-pooled; never PgBouncer transaction pooling). Must address the
    # same cable database as `database_url`.
    def listener_database_url=(value)
      @listener_database_url = optional_string(:listener_database_url, value)
      return if @listener_database_url.nil? || @listener_database_url.match?(POSTGRES_URL_PATTERN)

      raise ConfigurationError,
            "listener_database_url must be a postgres:// or postgresql:// DSN"
    end

    # CIDRs whose forwarded scheme/host/client headers the relay trusts.
    def trusted_proxies=(value)
      list = string_list(:trusted_proxies, value)
      list.each do |cidr|
        IPAddr.new(cidr)
      rescue IPAddr::Error
        raise ConfigurationError,
              "trusted_proxies entry #{cidr.inspect} is not an IP address or CIDR (e.g. \"10.0.0.0/8\")"
      end

      @trusted_proxies = list.freeze
    end

    def outbound_queue_messages=(value)
      @outbound_queue_messages = value.nil? ? nil : positive_integer(:outbound_queue_messages, value)
    end

    def outbound_queue_bytes=(value)
      @outbound_queue_bytes = value.nil? ? nil : positive_integer(:outbound_queue_bytes, value)
    end

    # Retired Orbitcast stream keys that still verify (never sign) during a
    # rotation window. Raw bytes; exported as comma-separated strict base64.
    def streams_previous_secrets=(value)
      @streams_previous_secrets = secret_list(:streams_previous_secrets, value)
    end

    def turbo_previous_secrets=(value)
      @turbo_previous_secrets = secret_list(:turbo_previous_secrets, value)
    end

    def connection=(value)
      @connection = enum_value(:connection, value, CONNECTIONS)
    end

    def active_record_connection?
      connection == :active_record
    end

    # Cross-field requirements. Called before anything opens a connection or
    # signs a token.
    def validate!
      raise ConfigurationError, "namespace is required" if namespace.nil?

      if publish_via == :postgres && database_url.nil?
        raise ConfigurationError,
              "database_url is required when publish_via is :postgres"
      end

      if publish_via == :http && ingress_url.nil?
        raise ConfigurationError,
              "ingress_url is required when publish_via is :http"
      end

      # Never fall back to an unauthenticated endpoint: a dispatch-mode
      # application without a service credential is a boot-time error, not a
      # silently open callback surface.
      if dispatch? && rpc_token.nil?
        raise ConfigurationError,
              "rpc_token is required when mode is :dispatch; the relay authenticates every " \
              "call with it (ORBITCAST_RPC_TOKEN_BASE64 on the relay side)"
      end

      self
    end

    def streams_secret!
      return streams_secret if streams_secret

      raise ConfigurationError,
            "streams_secret is not configured; set config.orbitcast.streams_secret " \
            "(or ORBITCAST_STREAMS_SECRET) before signing stream names, or let the Engine " \
            "derive one from Rails.application.key_generator (requires a namespace)"
    end

    # The allowlisted `ORBITCAST_*` values handed to the Rust process.
    #
    # Key material is exported with strict base64 so the server can decode raw
    # key bytes instead of guessing a UTF-8 interpretation. Nothing here is
    # ever passed as an argv element.
    def exported_env
      # `mode` and `publish_via` are Ruby-side concerns (adapter behaviour and
      # token production); the relay reads neither, so they are not exported.
      env = {
        "ORBITCAST_NAMESPACE" => namespace,
        "ORBITCAST_DATABASE_URL" => database_url,
        # Both sides of the relay must agree: the publisher writes the row, the
        # server reads it back and owns expiry cleanup.
        "ORBITCAST_OVERFLOW_ENABLED" => overflow_enabled.to_s,
        "ORBITCAST_OVERFLOW_TTL_MS" => (overflow_ttl * 1000).to_s,
        "ORBITCAST_MAX_PUBLICATION_SIZE" => max_publication_size.to_s
      }

      env["ORBITCAST_LISTEN_ADDR"] = listen_addr
      env["ORBITCAST_LISTENER_DATABASE_URL"] = listener_database_url
      env["ORBITCAST_TRUSTED_PROXIES"] = trusted_proxies.join(",") unless trusted_proxies.empty?
      env["ORBITCAST_OUTBOUND_QUEUE_MESSAGES"] = outbound_queue_messages&.to_s
      env["ORBITCAST_OUTBOUND_QUEUE_BYTES"] = outbound_queue_bytes&.to_s

      env["ORBITCAST_STREAMS_SECRET_BASE64"] = Base64.strict_encode64(streams_secret) if streams_secret
      env["ORBITCAST_RECOVERY_SECRET_BASE64"] = Base64.strict_encode64(recovery_secret) if recovery_secret
      unless streams_previous_secrets.empty?
        env["ORBITCAST_STREAMS_PREVIOUS_SECRETS_BASE64"] =
          streams_previous_secrets.map { |key| Base64.strict_encode64(key) }.join(",")
      end

      # The Turbo profile is enabled exactly when a Turbo key is configured:
      # a relay handed the key without the switch would reject every
      # `turbo_stream_from` subscription, and vice versa.
      if turbo_secret
        env["ORBITCAST_TURBO_STREAMS"] = "true"
        env["ORBITCAST_TURBO_SECRET_BASE64"] = Base64.strict_encode64(turbo_secret)
        unless turbo_previous_secrets.empty?
          env["ORBITCAST_TURBO_PREVIOUS_SECRETS_BASE64"] =
            turbo_previous_secrets.map { |key| Base64.strict_encode64(key) }.join(",")
        end
      end

      # Rails-dispatch mode: the relay needs the endpoint URL and the same
      # service credential, base64 of the exact raw bytes Rails compares
      # against. Exported only in dispatch mode, so a signed-mode deployment
      # cannot accidentally hand a token to a relay that will not use it.
      if dispatch?
        env["ORBITCAST_RPC_ENDPOINT_URL"] = rpc_endpoint_url
        env["ORBITCAST_RPC_TOKEN_BASE64"] = Base64.strict_encode64(rpc_token) if rpc_token
      end

      env.compact
    end

    # Never let a secret reach a log line or an exception message.
    def inspect
      "#<#{self.class.name} namespace=#{namespace.inspect} mode=#{mode.inspect} " \
        "publish_via=#{publish_via.inspect} connection=#{connection.inspect} " \
        "database_url=#{redacted_database_url.inspect} " \
        "listener_database_url=#{redacted_url(listener_database_url).inspect} " \
        "listen_addr=#{listen_addr.inspect} overflow_enabled=#{overflow_enabled.inspect} " \
        "streams_secret=#{streams_secret_derived? ? "[DERIVED]" : redact(streams_secret)} " \
        "turbo_secret=#{redact(turbo_secret)} " \
        "rpc_token=#{redact(rpc_token)} ingress_token=#{redact(ingress_token)}>"
    end
    alias to_s inspect

    private

    def redacted_database_url
      redacted_url(database_url)
    end

    def redacted_url(url)
      return nil if url.nil?

      url.sub(%r{://([^:/@]+):[^@]*@}, '://\1:[REDACTED]@')
    end

    # Memoized per namespace: a namespace change after derivation must not
    # keep handing out the previous namespace's key.
    def derived_streams_secret
      return nil if key_generator.nil? || namespace.nil?

      if @derived_streams_secret.nil? || @derived_streams_secret.first != namespace
        key = key_generator.generate_key(streams_key_purpose(namespace), STREAMS_KEY_SIZE)
        @derived_streams_secret = [namespace, key.to_s.b.freeze]
      end

      @derived_streams_secret.last
    end

    def redact(value)
      value.nil? ? "nil" : "[REDACTED]"
    end

    def optional_string(name, value)
      return nil if value.nil?

      string = string!(name, value)
      string.empty? ? nil : string
    end

    def required_string(name, value)
      string = string!(name, value)
      raise ConfigurationError, "#{name} must not be empty" if string.empty?

      string
    end

    def string!(name, value)
      unless value.is_a?(String)
        raise ConfigurationError, "#{name} must be a String, got #{value.class}"
      end

      value
    end

    def secret(name, value)
      return nil if value.nil?

      string = string!(name, value)
      raise ConfigurationError, "#{name} must not be empty" if string.empty?

      string.dup.freeze
    end

    def secret_list(name, value)
      return [].freeze if value.nil?

      unless value.is_a?(Array)
        raise ConfigurationError, "#{name} must be an Array of secrets, got #{value.class}"
      end

      value.map { |entry| secret(name, entry) || raise(ConfigurationError, "#{name} must not contain nil") }
           .freeze
    end

    def string_list(name, value)
      return [] if value.nil?

      list = value.is_a?(String) ? value.split(",").map(&:strip) : value
      unless list.is_a?(Array)
        raise ConfigurationError, "#{name} must be an Array of Strings, got #{value.class}"
      end

      list.map { |entry| required_string(name, entry) }
    end

    # Strict: a typo must never quietly disable (or enable) a switch.
    def boolean(name, value)
      case value
      when true, false then value
      when "true", "1", 1 then true
      when "false", "0", 0 then false
      else
        raise ConfigurationError,
              "#{name} must be a boolean (true/false), got #{value.inspect}"
      end
    end

    def enum_value(name, value, allowed)
      symbol =
        case value
        when Symbol then value
        when String then value.to_sym
        else
          raise ConfigurationError, "#{name} must be a Symbol or String, got #{value.class}"
        end

      unless allowed.include?(symbol)
        raise ConfigurationError,
              "#{name} #{symbol.inspect} is not supported; supported values: #{allowed.map(&:inspect).join(", ")}"
      end

      symbol
    end

    def positive_integer(name, value)
      integer = Integer(value)
      raise ConfigurationError, "#{name} must be greater than zero" unless integer.positive?

      integer
    rescue TypeError, ArgumentError => e
      raise e if e.is_a?(ConfigurationError)

      raise ConfigurationError, "#{name} must be an Integer, got #{value.inspect}"
    end

    def positive_float(name, value)
      float = Float(value)
      raise ConfigurationError, "#{name} must be greater than zero" unless float.positive?

      float
    rescue TypeError, ArgumentError => e
      raise e if e.is_a?(ConfigurationError)

      raise ConfigurationError, "#{name} must be a Float, got #{value.inspect}"
    end
  end
end
