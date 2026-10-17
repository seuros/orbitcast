# frozen_string_literal: true

require "action_cable"

require_relative "error"
require_relative "protocol"
require_relative "state_codec"

module Orbitcast
  # The Rails-dispatch channel base.
  #
  #   module ApplicationCable
  #     class Channel < Orbitcast::Channel; end
  #   end
  #
  #   class ChatChannel < ApplicationCable::Channel
  #     orbitcast_state :last_read_id
  #
  #     def subscribed
  #       room = Room.find_by(id: params[:room])
  #       return reject unless room && current_user.can_read?(room)
  #
  #       stream_for room
  #       grant_presence broadcasting_for(room), subject_id: "user:#{current_user.id}"
  #       transmit(backlog: room.recent_messages)
  #     end
  #
  #     def speak(data)
  #       room.messages.create!(body: data["body"], user: current_user)
  #     end
  #   end
  #
  # ## How it reuses ActionCable
  #
  # It is a **subclass of `ActionCable::Channel::Base`**, so `subscribed`,
  # `unsubscribed`, `before_subscribe`/`after_subscribe`, `reject`,
  # `perform_action` with its action allowlist and `rescue_from` handlers,
  # `broadcasting_for`, `broadcast_to`, and the `current_user`-style delegation
  # to the connection's identifiers are all Rails' own code, running unmodified.
  #
  # What a subclass overrides is the four methods whose *implementation* is a
  # live pub/sub subscription — `stream_from`, `stop_stream_from`,
  # `stop_all_streams`, and (through `stream_from`) `stream_for`. In dispatch
  # mode the relay owns the pub/sub side of a stream, so these record ordered
  # `add_stream` / `remove_stream` / `remove_all_streams` effects instead of
  # touching `connection.pubsub`. `transmit` needs no override at all: it
  # already goes through `connection.transmit`, which is where the transmit
  # effect is collected.
  #
  # Ordinary Ruby inheritance, and nothing else: no `prepend`, no patched
  # `ActionCable` constant, no fake socket.
  #
  # ## What it adds
  #
  # `grant_presence` / `revoke_presence` are the one piece of DSL that is not
  # ActionCable's. They exist because presence identity is an
  # authorization decision: the relay will not invent a `subject_id`, and a
  # client cannot supply one, so Rails hands it over explicitly — as an
  # ordered effect, like every other thing an operation produces.
  #
  # ## What cannot work
  #
  # A stream transformation block and a periodic timer are Ruby that would have
  # to run inside the Rust process on every message or every tick. They are
  # refused loudly rather than silently ignored — use an ordinary server-side
  # broadcast, or a job.
  class Channel < ActionCable::Channel::Base
    include StateCodec::Instance
    extend StateCodec::Declaration

    V1 = Protocol::V1

    # The presence operations a grant may authorize, spelled exactly as
    # `PresenceOp` spells them (`src/protocol/v1/presence.rs`). The same three
    # strings are what a client's `orbitcast_presence` command may ask for.
    PRESENCE_OPERATIONS = V1::PRESENCE_OPERATIONS

    # Default lease lifetime, in seconds, when `grant_presence` is called
    # without a `ttl:`. Short on purpose: a lease is refreshed by every `join`,
    # and a tab that dies stops refreshing.
    DEFAULT_PRESENCE_TTL = 45

    # Public methods that must never become client-callable actions: the
    # plumbing the RPC dispatcher calls, and the server-side presence DSL —
    # `grant_presence` is how identity is decided, so it is the last method a
    # client may ever name in `data["action"]`.
    #
    # `action_methods` subtracts `internal_methods`; that is the hook
    # ActionCable's own `Callbacks` module uses, and it is the reason a channel
    # can carry non-action public methods at all. Methods declared on
    # `ActionCable::Channel::Base` (`stream_from`, `transmit`, ...) are
    # subtracted by ActionCable itself; these are declared here, so they are
    # ours to exclude.
    INTERNAL_METHODS = %i[
      orbitcast_effects
      orbitcast_streams
      orbitcast_installed_streams=
      orbitcast_processable_action?
      orbitcast_rejected?
      orbitcast_dump_state
      orbitcast_restore_state
      grant_presence
      revoke_presence
    ].freeze

    # Action names beginning with this prefix belong to the relay.
    #
    # `orbitcast_presence` is the one that exists today (see
    # `src/protocol/v1/presence.rs`): the server answers that action itself and
    # never forwards it to Rails. The whole prefix is reserved so the next such
    # action does not silently start shadowing an application method that was
    # already deployed.
    RESERVED_ACTION_PREFIX = "orbitcast_"

    class << self
      # Periodic timers live in the ActionCable event loop, which is not where
      # this channel runs.
      def periodically(*, **, &)
        raise UnsupportedChannelFeature,
              "#{name || "this channel"} declares `periodically`, which needs an in-process " \
              "ActionCable event loop. In Rails-dispatch mode the socket is held by the relay: " \
              "use a recurring job that broadcasts to the stream instead."
      end

      # Fail at class-definition time on a reserved action name.
      #
      # A public method on a channel *is* a client-callable action
      # (`action_methods` minus `internal_methods`), so a channel defining
      # `orbitcast_presence` would be declaring an action the relay intercepts
      # and Rails is never asked about — the method would simply never run.
      # Excluding it with a warning would leave that dead code in the
      # application; raising here surfaces it on the line that wrote it, while
      # the class body is still on the stack.
      #
      # Only *public* methods are actions, so a helper with the same prefix
      # declared under a `private` (or `protected`) section is left alone —
      # visibility is decided before `method_added` runs there, which the
      # inline `private def` form cannot offer. The plumbing this base class
      # declares is allowlisted in {INTERNAL_METHODS}.
      def method_added(name)
        super

        if name.to_s.start_with?(RESERVED_ACTION_PREFIX) &&
           !INTERNAL_METHODS.include?(name) &&
           public_method_defined?(name)
          raise UnsupportedChannelFeature,
                "#{self.name || "this channel"} defines `#{name}`, and `" \
                "#{RESERVED_ACTION_PREFIX}` is a reserved action namespace: the relay answers " \
                "those actions itself, so Rails would never be asked to run this method. " \
                "Rename it, or make it private if it is not meant to be a client-callable " \
                "action."
        end
      end

      private

      def internal_methods
        super + INTERNAL_METHODS
      end
    end

    # Record an `add_stream` effect for an owned subscription.
    #
    # @param broadcasting [String, #to_s] the logical stream name
    def stream_from(broadcasting, callback = nil, coder: nil, &block)
      return if unsubscribed?

      if callback || block || coder
        raise UnsupportedChannelFeature,
              "stream_from with a callback, block, or coder cannot run in Rails-dispatch mode: " \
              "the relay delivers published payloads to the browser without calling back into " \
              "Ruby. Transform the payload where you broadcast it."
      end

      broadcasting = String(broadcasting)
      orbitcast_effects.add_stream(identifier, broadcasting)
      orbitcast_streams[broadcasting] = true
      broadcasting
    end

    # `stream_for model` keeps working: the broadcasting name still comes from
    # ActionCable's own `broadcasting_for`, so a `broadcast_to` from anywhere in
    # the application reaches this subscription.
    def stream_for(broadcastables, callback = nil, coder: nil, &block)
      stream_from(broadcasting_for(broadcastables), callback || block, coder: coder)
    end

    def stop_stream_from(broadcasting)
      broadcasting = String(broadcasting)
      return unless orbitcast_streams.delete(broadcasting)

      orbitcast_effects.remove_stream(identifier, broadcasting)
      broadcasting
    end

    def stop_stream_for(model)
      stop_stream_from(broadcasting_for(model))
    end

    # Registered by ActionCable itself as an `on_unsubscribe` callback, which is
    # why the collector is suppressed for teardown operations: the relay has
    # already dropped the subscription this would address.
    def stop_all_streams
      return if orbitcast_streams.empty?

      orbitcast_streams.clear
      orbitcast_effects.remove_all_streams(identifier)
    end

    # Grant this subscription an authorized presence membership on `stream`.
    #
    #   def subscribed
    #     stream_for room
    #     grant_presence broadcasting_for(room),
    #                    subject_id: "user:#{current_user.id}",
    #                    metadata: { name: current_user.name },
    #                    ttl: 45
    #   end
    #
    # ## Identity is decided here, and only here
    #
    # A client's `orbitcast_presence` command carries the operation and a
    # snapshot page window — nothing else. `subject_id` and `metadata` come
    # from this grant, so a browser cannot name, impersonate, or decorate a
    # subject: a `subject_id` it sends anyway is an unknown field on the
    # command envelope and is ignored on decode.
    #
    # ## The grant is bound to a stream this subscription holds
    #
    # Call it *after* `stream_from` / `stream_for` for the same name. The relay
    # refuses a grant for a stream the subscription does not stream from, and
    # releases the grant (announcing the leave) the moment the subscription
    # stops streaming from it — `stop_stream_from`, `stop_all_streams`,
    # unsubscribe, or the socket closing. Granting before streaming is the
    # one ordering mistake that is easy to make, so it raises here.
    #
    # A second grant for the same stream replaces the first: the newest
    # subject, metadata, TTL, and operations win. If the client had joined, a
    # different subject leaves (announced) before the new one joins, and the
    # same subject keeps its presence with the new metadata.
    #
    # ## Arguments
    #
    # @param stream [String, Symbol] the logical stream the roster belongs to —
    #   the same name `stream_from` installs. A model is *not* converted for
    #   you: pass `broadcasting_for(model)`, exactly as `stream_for` does
    #   internally, so the roster and the fanout agree on one name.
    # @param subject_id [String] the identity the roster reports, at most
    #   {Orbitcast::Protocol::V1::MAX_SUBJECT_ID_LEN} bytes. One subject, many
    #   sessions: tabs and devices of one subject produce one join and one
    #   leave.
    # @param metadata [Hash] public per-subject data, encoded at most
    #   {Orbitcast::Protocol::V1::MAX_PRESENCE_METADATA_LEN} bytes. It is
    #   broadcast to every member of the stream — nothing private belongs in
    #   it. Keys travel as strings.
    # @param ttl [Numeric, ActiveSupport::Duration, nil] lease lifetime in
    #   *seconds*; defaults to {DEFAULT_PRESENCE_TTL}. Emitted as `ttl_ms`.
    # @param operations [Array<String, Symbol>] the subset of
    #   {PRESENCE_OPERATIONS} this grant authorizes. Defaults to all three.
    # @return [Hash, nil] the recorded effect, or nil when the collector is
    #   suppressed (`close_channel` / `release_connection` address a
    #   subscription the relay has already tombstoned).
    # @raise [ArgumentError] when a value is outside the limits the relay
    #   enforces, or when this subscription does not stream from `stream`.
    #   Rails fails here rather than emitting a reply Rust rejects.
    def grant_presence(stream, subject_id:, metadata: {}, ttl: nil,
                       operations: PRESENCE_OPERATIONS)
      orbitcast_effects.presence_grant(
        identifier,
        stream: presence_streamed!(presence_stream!(stream)),
        subject_id: presence_subject_id!(subject_id),
        metadata: presence_metadata!(metadata),
        ttl_ms: presence_ttl_ms!(ttl),
        operations: presence_operations!(operations)
      )
    end

    # Drop a presence membership this subscription was granted.
    #
    # Idempotent by construction: the effect names `(identifier, stream,
    # subject_id)`, and revoking a membership that is already gone is a no-op
    # at the relay rather than an error here.
    #
    # @return [Hash, nil] the recorded effect, or nil when suppressed
    # @raise [ArgumentError] on an unusable stream or subject id
    def revoke_presence(stream, subject_id:)
      orbitcast_effects.presence_revoke(
        identifier,
        stream: presence_stream!(stream),
        subject_id: presence_subject_id!(subject_id)
      )
    end

    # The streams this subscription holds, as the relay reported them.
    def orbitcast_streams
      @orbitcast_streams ||= {}
    end

    # Seed the channel with the streams the relay already installed, so
    # `stop_stream_from` in a later `perform` knows what there is to remove.
    def orbitcast_installed_streams=(streams)
      @orbitcast_streams = Array(streams).each_with_object({}) { |stream, out| out[stream] = true }
    end

    # Whether `data["action"]` names a callable action on this channel.
    # ActionCable logs and ignores anything else; the RPC boundary answers with
    # an explicit denial instead, so the relay can record it.
    def orbitcast_processable_action?(data)
      processable_action?(extract_action(data))
    end

    # `subscription_rejected?` is private in ActionCable; the dispatcher needs
    # the answer to turn `reject` into a denied outcome.
    def orbitcast_rejected?
      !!subscription_rejected?
    end

    def orbitcast_effects
      connection.orbitcast_effects
    end

    private

    # --- presence argument checks ---------------------------------------------
    #
    # Each one mirrors the matching check in `Effect::validate` in
    # `src/protocol/v1/rpc.rs`, so an unusable grant fails in the application's
    # own callback — with the application's own stack trace — instead of
    # arriving as a rejected reply the relay logs and the browser never sees.
    # `ArgumentError` because this is the application-facing DSL, the same way
    # `Orbitcast::Streams` and `Orbitcast::Recovery` report bad arguments; the
    # `EffectCollector` behind it still raises `ProtocolError` for anything
    # that reaches it by another path.

    def presence_stream!(stream)
      unless stream.is_a?(String) || stream.is_a?(Symbol)
        raise ArgumentError,
              "presence stream must be a String, got #{stream.class}. For a model pass " \
              "broadcasting_for(model) — the same name stream_for installs."
      end

      stream = stream.to_s
      raise ArgumentError, "presence stream must not be empty" if stream.empty?

      if stream.bytesize > V1::MAX_STREAM_LEN
        raise ArgumentError,
              "presence stream is #{stream.bytesize} bytes; the maximum is #{V1::MAX_STREAM_LEN}"
      end

      stream
    end

    # The relay binds a grant to a stream the subscription holds and refuses
    # anything else; failing here keeps that refusal out of the relay's log
    # and in the callback that got the order wrong.
    def presence_streamed!(stream)
      return stream if orbitcast_streams.key?(stream)

      raise ArgumentError,
            "grant_presence(#{stream.inspect}) must follow stream_from/stream_for for that " \
            "stream: the relay binds the roster to a stream this subscription holds " \
            "(currently #{orbitcast_streams.keys.inspect})"
    end

    def presence_subject_id!(subject_id)
      unless subject_id.is_a?(String)
        raise ArgumentError, "subject_id must be a String, got #{subject_id.class}"
      end
      raise ArgumentError, "subject_id must not be empty" if subject_id.empty?

      if subject_id.bytesize > V1::MAX_SUBJECT_ID_LEN
        raise ArgumentError,
              "subject_id is #{subject_id.bytesize} bytes; the maximum is " \
              "#{V1::MAX_SUBJECT_ID_LEN}"
      end

      subject_id
    end

    # The *encoded* size is what travels and what is stored, so one huge value
    # is bounded exactly like many small ones.
    def presence_metadata!(metadata)
      unless metadata.is_a?(Hash)
        raise ArgumentError, "metadata must be a Hash, got #{metadata.class}"
      end

      metadata = metadata.transform_keys do |key|
        unless key.is_a?(String) || key.is_a?(Symbol)
          raise ArgumentError, "metadata keys must be Strings, got #{key.class}"
        end

        key.to_s
      end

      encoded =
        begin
          JSON.generate(metadata)
        rescue JSON::GeneratorError, SystemStackError => e
          raise ArgumentError, "metadata must be JSON-encodable: #{e.message}"
        end

      if encoded.bytesize > V1::MAX_PRESENCE_METADATA_LEN
        raise ArgumentError,
              "metadata encodes to #{encoded.bytesize} bytes; the maximum is " \
              "#{V1::MAX_PRESENCE_METADATA_LEN}"
      end

      metadata
    end

    # Seconds in, milliseconds out: `ttl: 45` and `ttl: 45.seconds` are the
    # same lease, and the wire field is `ttl_ms`.
    def presence_ttl_ms!(ttl)
      seconds = ttl.nil? ? DEFAULT_PRESENCE_TTL : ttl
      unless seconds.is_a?(Numeric)
        raise ArgumentError, "ttl must be a number of seconds, got #{seconds.class}"
      end

      ttl_ms = (seconds.to_f * 1000).round
      unless ttl_ms.positive?
        raise ArgumentError, "ttl must be greater than zero (it became #{ttl_ms}ms)"
      end

      ttl_ms
    end

    # A grant names what it authorizes. An operation the server does not know
    # could never be performed, and an empty allowlist grants nothing at all —
    # both are caller mistakes, not lenient defaults.
    def presence_operations!(operations)
      unless operations.is_a?(Array)
        raise ArgumentError, "operations must be an Array, got #{operations.class}"
      end

      operations = operations.map do |operation|
        unless operation.is_a?(String) || operation.is_a?(Symbol)
          raise ArgumentError, "operations must be Strings, got #{operation.class}"
        end

        operation = operation.to_s
        unless PRESENCE_OPERATIONS.include?(operation)
          raise ArgumentError,
                "unknown presence operation #{operation.inspect}; the operations are " \
                "#{PRESENCE_OPERATIONS.join(", ")}"
        end

        operation
      end.uniq

      if operations.empty?
        raise ArgumentError,
              "operations must name at least one of #{PRESENCE_OPERATIONS.join(", ")}; " \
              "a grant that authorizes nothing can never be used"
      end

      operations
    end
  end
end
