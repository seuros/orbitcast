# frozen_string_literal: true

require "action_cable"
require "active_support/core_ext/hash/indifferent_access"
require "active_support/core_ext/string/inflections"

require_relative "channel"
require_relative "channel_registry"
require_relative "connection"
require_relative "error"
require_relative "protocol"

module Orbitcast
  # Runs one validated RPC request against the application's connection and
  # channel classes and returns one reply.
  #
  # This is the whole Rails execution model in one place:
  #
  #   describe            capability handshake; no session, no callbacks
  #   identify            connect / reject_unauthorized_connection
  #   open_channel        registry lookup, subscribed, reject
  #   close_channel       unsubscribed, best effort, effects suppressed
  #   run_action          perform_action through the action allowlist + filters
  #   release_connection  remaining unsubscribes + disconnect, best effort
  #
  # Requests are **stateless**: every call rebuilds the connection from the
  # identity and declared state the relay handed back, runs the callbacks, and
  # returns what changed. Nothing is memoized between calls, no database
  # connection is held for a socket's lifetime, and an instance variable that
  # was not declared with `orbitcast_state` does not survive the request.
  class Dispatcher
    V1 = Protocol::V1

    # Named capabilities reported by `describe`. The relay admits a node by
    # envelope version and capability, never by build equality.
    CAPABILITIES = %w[rails_dispatch].freeze

    def initialize(config: nil, registry: nil, connection_class: nil, logger: nil)
      @config = config || Orbitcast.config
      @registry = registry
      @connection_class = connection_class
      @logger = logger
    end

    attr_reader :config

    def logger
      @logger ||= (defined?(::Rails) && ::Rails.respond_to?(:logger) && ::Rails.logger) || null_logger
    end

    def registry
      @registry ||= ChannelRegistry.build(@config.channels)
    end

    # @param request [Orbitcast::Protocol::V1::Request] already validated
    # @return [Orbitcast::Protocol::V1::Reply]
    def call(request)
      case request.operation
      when V1::OPERATION_DESCRIBE then describe(request)
      when V1::OPERATION_IDENTIFY then identify(request)
      when V1::OPERATION_OPEN_CHANNEL then open_channel(request)
      when V1::OPERATION_CLOSE_CHANNEL then close_channel(request)
      when V1::OPERATION_RUN_ACTION then run_action(request)
      when V1::OPERATION_RELEASE_CONNECTION then release_connection(request)
      else
        # Unreachable: Request.from refuses unknown operations.
        V1::Reply.fault(request, "unknown operation")
      end
    rescue ActionCable::Connection::Authorization::UnauthorizedError
      # An application may refuse from a channel callback too, not just from
      # `connect`. A refusal is a decision, so it is a denial, not a fault.
      V1::Reply.denied(request, "unauthorized")
    rescue StandardError => e
      # A Rails-side failure. The relay closes the session with
      # `reconnect: true`; it never falls back to signed-only acceptance.
      log_exception(request, e)
      V1::Reply.fault(request, e.class.name)
    end

    # The connection class the application wired up. Resolved per call so a
    # reloaded class is picked up (the endpoint runs inside the executor).
    def connection_class
      name = @connection_class || config.connection_class
      return name if name.is_a?(Class)

      constant = name.to_s.safe_constantize
      unless constant.is_a?(Class) && constant <= Orbitcast::Connection
        raise ConfigurationError,
              "#{name} is not an Orbitcast::Connection. In Rails-dispatch mode " \
              "ApplicationCable::Connection must inherit from Orbitcast::Connection " \
              "(config.orbitcast.connection_class selects another class)."
      end

      constant
    end

    # Whether this endpoint is ready to serve dispatch traffic.
    def ready?
      connection_class
      !registry.empty?
    rescue StandardError
      false
    end

    private

    # --- operations ---------------------------------------------------------

    def describe(request)
      V1::Reply.describe(
        request,
        supported_versions: [V1::VERSION],
        software_version: Orbitcast::VERSION,
        capabilities: CAPABILITIES,
        ready: ready?
      )
    end

    def identify(request)
      effects = V1::EffectCollector.new
      connection = build_connection(request, effects, restore_identity: false)

      begin
        connection.orbitcast_connect
      rescue ActionCable::Connection::Authorization::UnauthorizedError
        return V1::Reply.denied(request, "unauthorized")
      end

      V1::Reply.accepted(
        request,
        effects: effects,
        identity: connection.orbitcast_identity,
        state: state_change(request.connection.state, connection.orbitcast_dump_state)
      )
    end

    def open_channel(request)
      effects = V1::EffectCollector.new
      identifier = request.subscription.identifier

      params = decode_identifier(identifier)
      return V1::Reply.denied(request, "malformed channel identifier") if params.nil?

      channel_class = registry.resolve(params[:channel])
      if channel_class.nil?
        logger.error { "Orbitcast: refused subscription to #{params[:channel].inspect}" }
        return V1::Reply.denied(request, "channel is not in the Orbitcast channel registry")
      end

      connection = build_connection(request, effects)
      channel = build_channel(channel_class, connection, request)
      connection.subscriptions.add(channel)

      connection.orbitcast_run_command { channel.subscribe_to_channel }

      if channel.orbitcast_rejected?
        # Everything the callbacks collected before the refusal is dropped by
        # the reply builder: a denial changes nothing.
        return V1::Reply.denied(request, "subscription rejected")
      end

      V1::Reply.accepted(
        request,
        effects: effects,
        state: state_change(request.connection.state, connection.orbitcast_dump_state),
        subscription_state: state_change(request.subscription.state, channel.orbitcast_dump_state)
      )
    end

    def run_action(request)
      effects = V1::EffectCollector.new
      identifier = request.subscription.identifier

      params = decode_identifier(identifier)
      return V1::Reply.denied(request, "malformed channel identifier") if params.nil?

      channel_class = registry.resolve(params[:channel])
      return V1::Reply.denied(request, "channel is not in the Orbitcast channel registry") if channel_class.nil?

      data = begin
        request.action.data
      rescue JSON::ParserError
        nil
      end
      return V1::Reply.denied(request, "action data is not JSON") unless data.is_a?(Hash)

      connection = build_connection(request, effects)
      channel = build_channel(channel_class, connection, request)
      connection.subscriptions.add(channel)

      unless channel.orbitcast_processable_action?(data)
        return V1::Reply.denied(request, "unprocessable action")
      end

      connection.orbitcast_run_command { channel.perform_action(data) }

      V1::Reply.accepted(
        request,
        effects: effects,
        state: state_change(request.connection.state, connection.orbitcast_dump_state),
        subscription_state: state_change(request.subscription.state, channel.orbitcast_dump_state)
      )
    end

    # Post-hoc: the client has already left and the relay has already dropped
    # the membership. Rails cannot veto it, and nothing it returns here can
    # resurrect the subscription, so the collector is suppressed.
    def close_channel(request)
      effects = V1::EffectCollector.new(suppressed: true)
      params = decode_identifier(request.subscription.identifier)
      channel_class = params && registry.resolve(params[:channel])

      if channel_class
        connection = build_connection(request, effects)
        channel = build_channel(channel_class, connection, request)
        connection.orbitcast_run_command { channel.unsubscribe_from_channel }
      end

      V1::Reply.accepted(request)
    end

    # Best effort, by definition: on process or network loss this never runs at
    # all. Every subscription is unwound independently so one raising
    # `unsubscribed` cannot swallow the `disconnect` callback.
    def release_connection(request)
      effects = V1::EffectCollector.new(suppressed: true)
      connection = build_connection(request, effects)

      request.release.subscriptions.each do |snapshot|
        params = decode_identifier(snapshot.identifier)
        channel_class = params && registry.resolve(params[:channel])
        next unless channel_class

        begin
          channel = build_channel(channel_class, connection, request, subscription: snapshot)
          connection.orbitcast_run_command { channel.unsubscribe_from_channel }
        rescue StandardError => e
          log_exception(request, e)
        end
      end

      connection.orbitcast_disconnect

      V1::Reply.accepted(request)
    end

    # --- construction -------------------------------------------------------

    def build_connection(request, effects, restore_identity: true)
      connection = connection_class.new(request.connection, effects: effects, logger: logger)
      connection.orbitcast_restore_identity(request.connection.identity) if restore_identity
      connection.orbitcast_restore_state(StateCodec.load_map(request.connection.state))
      connection
    end

    def build_channel(channel_class, connection, request, subscription: nil)
      subscription ||= request.subscription
      params = decode_identifier(subscription.identifier)

      channel = channel_class.new(connection, subscription.identifier, params)
      channel.orbitcast_installed_streams = subscription.streams
      channel.orbitcast_restore_state(StateCodec.load_map(subscription.state))
      channel
    end

    # ActionCable decodes the identifier itself and treats the decoded hash as
    # `params`; so does this, because the identifier is what the relay routes
    # by. The `params` field of the snapshot is a convenience copy and is never
    # authoritative.
    #
    # Plain `JSON.parse`, not `ActiveSupport::JSON.decode`: an identifier is
    # routing data, and no part of it should be coerced into a Time (or
    # anything else) on the way to `params`.
    #
    # @return [ActiveSupport::HashWithIndifferentAccess, nil]
    def decode_identifier(identifier)
      decoded = JSON.parse(identifier)
      decoded.is_a?(Hash) ? decoded.with_indifferent_access : nil
    rescue StandardError
      nil
    end

    # Absent means unchanged, so a map that did not move is omitted entirely.
    # A map that lost every key is `{}`, which is an explicit clear — the two
    # must never be confused, which is why this compares rather than always
    # emitting.
    def state_change(previous, current)
      previous = {} unless previous.is_a?(Hash)
      return V1::Reply::UNCHANGED if current == previous

      current
    end

    def log_exception(request, error)
      logger.error do
        "Orbitcast #{request.operation} raised #{error.class}: #{error.message}\n" \
          "#{Array(error.backtrace).first(10).join("\n")}"
      end
    end

    def null_logger
      require "logger"
      @null_logger ||= ::Logger.new(IO::NULL)
    end
  end
end
