# frozen_string_literal: true

require "json"

require_relative "error"
require_relative "protocol"

module Orbitcast
  # Declared state serialization.
  #
  # Rails-dispatch requests are stateless: the relay holds the declared
  # connection and channel state between calls and hands it back on the next
  # one. That only works if what crosses the wire is *data*, so this codec
  # supports exactly two things:
  #
  #   * JSON primitives (and arrays/objects of them), and
  #   * records, tagged explicitly as `{"_gid": "gid://app/User/7"}`.
  #
  # There is no Marshal, no YAML, no arbitrary instance variables, and no
  # implicit capture of anything: state is declared with `orbitcast_state`, or
  # it does not survive the request. Ordinary memoization stays per-call.
  #
  #   class ApplicationCable::Connection < Orbitcast::Connection
  #     identified_by :current_user
  #     orbitcast_state :locale, :cart
  #   end
  #
  # A `_gid` whose record was deleted between two requests restores as **nil**,
  # not an exception: the relay cannot retry a callback that has already run,
  # and a channel that requires the record must check for it. The same goes for
  # a reference the application is no longer allowed to locate — `GlobalID`
  # locators raise, and a raised locator is a missing record here.
  module StateCodec
    # The tag that makes a record reference explicit. A plain Hash carrying this
    # key cannot be encoded, because it would be indistinguishable from one.
    GID_KEY = "_gid"

    class << self
      # @return [Object] a JSON-ready value
      # @raise [ProtocolError] for anything that is not declared-state material
      def dump(value)
        case value
        when nil, true, false, String, Integer then value
        when Float
          unless value.finite?
            raise Orbitcast::ProtocolError.new("declared state cannot hold #{value}", field: "state")
          end

          value
        when Symbol then value.to_s
        when Array then value.map { |entry| dump(entry) }
        when Hash then dump_hash(value)
        else
          dump_record(value)
        end
      end

      # @return [Object] the restored value; a missing record restores as nil
      def load(value)
        case value
        when Array then value.map { |entry| load(entry) }
        when Hash
          gid = value[GID_KEY]
          gid.is_a?(String) && value.size == 1 ? locate(gid) : value.transform_values { |entry| load(entry) }
        else
          value
        end
      end

      # Dump a whole declared-state map, validating it against the wire limits
      # before it can reach a reply.
      def dump_map(hash)
        map = hash.each_with_object({}) { |(key, value), out| out[key.to_s] = dump(value) }
        Protocol::V1.validate_state_map!("state", map)
      end

      def load_map(hash)
        return {} unless hash.is_a?(Hash)

        hash.each_with_object({}) { |(key, value), out| out[key.to_s] = load(value) }
      end

      private

      def dump_hash(hash)
        hash.each_with_object({}) do |(key, value), out|
          key = key.to_s
          if key == GID_KEY
            raise Orbitcast::ProtocolError.new(
              "declared state cannot hold a raw #{GID_KEY.inspect} key; it is reserved for " \
              "record references", field: "state"
            )
          end

          out[key] = dump(value)
        end
      end

      def dump_record(value)
        unless value.respond_to?(:to_global_id)
          raise Orbitcast::ProtocolError.new(
            "declared state holds #{value.class}, which is neither a JSON value nor a " \
            "GlobalID-identified record. Declared state is serialized, not captured: store an " \
            "id, or include GlobalID::Identification in the model.", field: "state"
          )
        end

        { GID_KEY => value.to_global_id.to_s }
      end

      # A deleted (or no longer locatable) record is nil, by design.
      def locate(gid)
        return nil unless defined?(::GlobalID)

        ::GlobalID::Locator.locate(gid)
      rescue StandardError
        nil
      end
    end

    # `orbitcast_state :cursor, :user` on a Connection or a Channel.
    #
    # Declaring a key defines an accessor for it and nothing else: the value is
    # dumped after every operation and restored before the next one.
    #
    # The accessors are **private**. `cursor` and `self.cursor = value` work
    # from inside the channel, exactly as an attribute should; what cannot
    # happen is a client performing `{"action":"cursor=","..."}` and writing
    # declared state directly — ActionCable's action allowlist is every public
    # method a channel adds.
    module Declaration
      def orbitcast_state(*names)
        names = names.flatten.map(&:to_sym)
        names.each do |name|
          send(:attr_accessor, name)
          send(:private, name, :"#{name}=")
        end

        @orbitcast_declared_state_keys = (declared_state_keys + names).uniq.freeze
      end

      # Declarations inherit: a base class may declare state every channel
      # keeps, and a subclass adds its own.
      def orbitcast_state_keys
        inherited =
          superclass.respond_to?(:orbitcast_state_keys) ? superclass.orbitcast_state_keys : []

        (inherited + declared_state_keys).uniq.freeze
      end

      private

      def declared_state_keys
        @orbitcast_declared_state_keys ||= [].freeze
      end
    end

    # The per-instance half of the declaration.
    module Instance
      # @return [Hash] the declared state, JSON-ready. Keys whose accessor was
      #   never written are omitted rather than emitted as null.
      def orbitcast_dump_state
        self.class.orbitcast_state_keys.each_with_object({}) do |name, out|
          ivar = :"@#{name}"
          next unless instance_variable_defined?(ivar)

          out[name.to_s] = StateCodec.dump(instance_variable_get(ivar))
        end
      end

      # @param map [Hash] the state the relay held for this session
      def orbitcast_restore_state(map)
        return self unless map.is_a?(Hash)

        self.class.orbitcast_state_keys.each do |name|
          key = name.to_s
          next unless map.key?(key)

          instance_variable_set(:"@#{name}", StateCodec.load(map[key]))
        end

        self
      end
    end
  end
end
