# frozen_string_literal: true

require "action_cable"
require "action_dispatch"
require "active_support/json"
require "active_support/rescuable"
require "stringio"
require "uri"

require_relative "error"
require_relative "protocol"
require_relative "state_codec"

module Orbitcast
  # The Rails-dispatch connection base.
  #
  #   module ApplicationCable
  #     class Connection < Orbitcast::Connection
  #       identified_by :current_user
  #       orbitcast_identity_key :current_user
  #
  #       def connect
  #         self.current_user = find_verified_user
  #       end
  #
  #       private
  #
  #       def find_verified_user
  #         User.find_by(id: cookies.signed[:user_id]) || reject_unauthorized_connection
  #       end
  #     end
  #   end
  #
  # ## What this is, and what it is not
  #
  # It is **not** an `ActionCable::Connection::Base`. That class owns a live
  # WebSocket: a socket, a message buffer, an event loop, a worker pool, and a
  # `process`/`handle_open`/`handle_close` lifecycle driven by a Rack hijack.
  # None of that exists here — the socket is in the Rust relay, and this object
  # lives for exactly one HTTP request.
  #
  # What it *is* is the same **composition** ActionCable builds that class from:
  # `Identification` (so `identified_by` and the channel-side delegation work
  # unchanged), `Authorization` (so `reject_unauthorized_connection` is the same
  # method raising the same error), `Callbacks` (so `around_command` still wraps
  # subscribe/unsubscribe/perform), and `Rescuable`. It then implements the small
  # surface `ActionCable::Channel::Base` actually calls on its connection —
  # `identifiers`, `logger`, `transmit`, `subscriptions`, `config` — which is
  # what lets a real, unpatched `Channel::Base` run against it.
  #
  # No `prepend`, no monkey patch, no synthetic socket.
  #
  # ## Identity
  #
  # `identify` returns the identifiers as declared-state values, so the relay
  # can hand them back on the next call. Add `orbitcast_identity_key` to make
  # the connection addressable by `Orbitcast.disconnect`; without it the session
  # is a perfectly valid anonymous one that simply cannot be reached by a
  # control message.
  class Connection
    include ActionCable::Connection::Identification
    include ActionCable::Connection::Authorization
    include ActionCable::Connection::Callbacks
    include ActiveSupport::Rescuable
    include StateCodec::Instance
    extend StateCodec::Declaration

    # The key Rust indexes sessions by. It is an ordinary entry in the identity
    # map, spelled out so nothing has to guess which identifier is canonical.
    IDENTITY_KEY = "identity_key"

    class << self
      # Declare how this connection is addressed by a remote disconnect.
      #
      #   orbitcast_identity_key :current_user            # to_gid_param / to_s
      #   orbitcast_identity_key { "team:#{team.id}" }
      def orbitcast_identity_key(name = nil, &block)
        if name.nil? && block.nil?
          return @orbitcast_identity_key ||
                 (superclass.respond_to?(:orbitcast_identity_key) ? superclass.orbitcast_identity_key : nil)
        end

        if name && block
          raise ArgumentError, "pass a method name or a block to orbitcast_identity_key, not both"
        end

        @orbitcast_identity_key = block || name.to_sym
      end
    end

    # @param snapshot [Orbitcast::Protocol::V1::Request::ConnectionSnapshot]
    # @param effects [Orbitcast::Protocol::V1::EffectCollector]
    def initialize(snapshot, effects:, logger: nil)
      @orbitcast_snapshot = snapshot
      @orbitcast_effects = effects
      @orbitcast_logger = logger
      @orbitcast_identifier = nil
    end

    attr_reader :orbitcast_snapshot, :orbitcast_effects

    # --- the request, reconstructed from validated metadata -----------------

    # The original WebSocket request as an ActionDispatch::Request. The browser
    # could not submit any of it: the relay rebuilt it from the verified URL,
    # origin, client address, and the configured header allowlist.
    def request
      @request ||= begin
        environment = rack_env
        if defined?(::Rails) && ::Rails.respond_to?(:application) && ::Rails.application
          environment = ::Rails.application.env_config.merge(environment)
        end

        ActionDispatch::Request.new(environment)
      end
    end

    def cookies
      # In a booted application the cookie jar arrives with the middleware
      # stack; this endpoint can also be driven directly, so load it on demand.
      require "action_dispatch/middleware/cookies" unless request.respond_to?(:cookie_jar)

      request.cookie_jar
    end

    def env
      request.env
    end

    def logger
      @orbitcast_logger ||= default_logger
    end

    def server
      ActionCable.server
    end

    def config
      server.config
    end

    # The relay owns fan-out. A connection in dispatch mode never reaches the
    # in-process pub/sub, and pretending otherwise would silently install a
    # subscription no browser is attached to.
    def pubsub
      raise UnsupportedChannelFeature,
            "an Orbitcast connection has no in-process pub/sub: streams are installed by the " \
            "relay through add_stream effects"
    end
    alias worker_pool pubsub
    alias event_loop pubsub

    # --- the surface ActionCable::Channel::Base calls -----------------------

    # Channel::Base#transmit sends `identifier:, message:`; the framing helpers
    # send `identifier:, type:`. Only the first is the application's: `welcome`,
    # `confirm_subscription`, `reject_subscription`, and `disconnect` are framed
    # by Rust from the reply outcome, and are dropped here on purpose.
    def transmit(cable_message)
      return if cable_message[:message].nil? && cable_message["message"].nil?

      identifier = cable_message[:identifier] || cable_message["identifier"]
      message = cable_message.key?(:message) ? cable_message[:message] : cable_message["message"]

      orbitcast_effects.transmit(identifier, ActiveSupport::JSON.encode(message))
    end

    # `reject` inside `subscribed` routes through here in ActionCable. The
    # collection is one subscription wide: one RPC call carries one.
    def subscriptions
      @subscriptions ||= Subscriptions.new
    end

    # Close the whole connection with an allowlisted reason. `Orbitcast::Channel`
    # and the application call this instead of ActionCable's socket close.
    def close(reason: Protocol::V1::CLOSE_REASON_REMOTE, reconnect: false)
      orbitcast_effects.close(reason: reason, reconnect: reconnect)
    end

    # --- lifecycle ----------------------------------------------------------

    # Run the application's `connect`, if it defined one.
    def orbitcast_connect
      connect if respond_to?(:connect)
      self
    end

    # Run the application's `disconnect`, if it defined one. Best effort: the
    # browser is already gone by the time this runs.
    def orbitcast_disconnect
      disconnect if respond_to?(:disconnect)
      self
    end

    # Subscribe / unsubscribe / perform run inside the `command` callbacks, the
    # same way ActionCable runs them, so `around_command :set_current_account`
    # keeps working.
    def orbitcast_run_command(&block)
      run_callbacks(:command, &block)
    end

    # --- identity -----------------------------------------------------------

    # The identity map returned by an accepted `identify`.
    def orbitcast_identity
      map = self.class.identifiers.each_with_object({}) do |name, out|
        ivar = :"@#{name}"
        next unless instance_variable_defined?(ivar)

        value = instance_variable_get(ivar)
        next if value.nil?

        out[name.to_s] = StateCodec.dump(value)
      end

      key = orbitcast_identity_key
      map[IDENTITY_KEY] = key unless key.nil?
      Protocol::V1.validate_state_map!("identity", map)
    end

    # Restore the identifiers an earlier `identify` returned.
    def orbitcast_restore_identity(map)
      return self unless map.is_a?(Hash)

      self.class.identifiers.each do |name|
        key = name.to_s
        next unless map.key?(key)

        instance_variable_set(:"@#{name}", StateCodec.load(map[key]))
      end

      self
    end

    # The value a remote disconnect addresses this connection by, or nil when
    # the application declared none.
    def orbitcast_identity_key
      declared = self.class.orbitcast_identity_key
      return nil if declared.nil?

      value = declared.is_a?(Proc) ? instance_exec(&declared) : public_send(declared)
      return nil if value.nil?

      key = value.respond_to?(:to_gid_param) ? value.to_gid_param : value.to_s
      return nil if key.empty?

      Protocol::V1.validate_bounded!(
        "identity_key", key, Protocol::V1::MAX_IDENTITY_KEY_LEN
      )
    end

    def inspect
      "#<#{self.class.name} identifiers=#{self.class.identifiers.to_a.inspect}>"
    end

    private

    def default_logger
      if defined?(::Rails) && ::Rails.respond_to?(:logger) && ::Rails.logger
        ::Rails.logger
      else
        require "logger"
        ::Logger.new(IO::NULL)
      end
    end

    def rack_env
      uri = parsed_url
      scheme = uri.scheme || origin_scheme
      host = uri.host || origin_host

      env = {
        "REQUEST_METHOD" => "GET",
        "SCRIPT_NAME" => "",
        "PATH_INFO" => uri.path.to_s.empty? ? "/" : uri.path,
        "QUERY_STRING" => uri.query.to_s,
        "SERVER_NAME" => host,
        "SERVER_PORT" => (uri.port || (scheme == "https" ? 443 : 80)).to_s,
        "HTTP_HOST" => host,
        "REMOTE_ADDR" => orbitcast_snapshot.client_address,
        "rack.url_scheme" => scheme,
        "rack.input" => StringIO.new(""),
        "rack.errors" => $stderr,
        # Marks the request as the reconstruction it is, for application code
        # that wants to know it is not serving a browser directly.
        "orbitcast.dispatch" => true
      }

      env["HTTP_ORIGIN"] = orbitcast_snapshot.origin if orbitcast_snapshot.origin
      orbitcast_snapshot.headers.each do |name, value|
        env["HTTP_#{name.to_s.upcase.tr("-", "_")}"] = value
      end

      env
    end

    def parsed_url
      URI.parse(orbitcast_snapshot.url.to_s)
    rescue URI::InvalidURIError
      URI.parse("/")
    end

    def origin_scheme
      origin = orbitcast_snapshot.origin.to_s
      origin.start_with?("https://") ? "https" : "http"
    end

    def origin_host
      URI.parse(orbitcast_snapshot.origin.to_s).host || "localhost"
    rescue URI::InvalidURIError
      "localhost"
    end

    # The one-subscription stand-in for
    # `ActionCable::Connection::Subscriptions`. `Channel::Base#reject` calls
    # `connection.subscriptions.remove_subscription(self)`, which in ActionCable
    # runs the unsubscribe callbacks; it does the same here, so a rejected
    # subscription unwinds exactly as it does in a plain Rails application.
    class Subscriptions
      def initialize
        @subscriptions = {}
      end

      def add(subscription)
        @subscriptions[subscription.identifier] = subscription
      end

      def remove_subscription(subscription)
        subscription.unsubscribe_from_channel
        @subscriptions.delete(subscription.identifier)
      end

      def identifiers
        @subscriptions.keys
      end
    end
  end
end
