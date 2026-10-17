# frozen_string_literal: true

require_relative "error"

module Orbitcast
  # The allowlist of channel classes a client may subscribe to.
  #
  # ActionCable resolves `identifier["channel"]` with `safe_constantize`, which
  # is fine when the only thing on the other end is a browser talking to your
  # own process. Here the name arrives over an RPC boundary and is attacker
  # controlled, so it is matched against an explicit list of strings *first* and
  # only then resolved. Nothing unlisted is ever constantized, and a listed name
  # that does not resolve to an `Orbitcast::Channel` is refused too:
  # `"Kernel"` is a constant, it is just not a channel.
  #
  #   config.orbitcast.channels = %w[ChatChannel Turbo::StreamsChannel]
  #   config.orbitcast.channels = :auto   # derive from app/channels
  #
  # `:auto` derivation reads the application's `app/channels` tree, turns each
  # file name into the constant Zeitwerk would define, and keeps the ones that
  # are subclasses of `Orbitcast::Channel`. It is a convenience for development;
  # an explicit list is what a reviewed deployment ships.
  class ChannelRegistry
    AUTO = :auto

    # `ApplicationCable::Channel` is a base class, never a subscribable target.
    NEVER_SUBSCRIBABLE = %w[ApplicationCable::Channel Orbitcast::Channel].freeze

    class << self
      # @param names [Array<String, Class>, :auto, nil]
      def build(names, root: nil)
        case names
        when nil, AUTO, "auto" then auto(root)
        when Array then new(names)
        else
          raise ConfigurationError,
                "channels must be an Array of channel class names or :auto, got #{names.class}"
        end
      end

      # Derive the allowlist from `app/channels/**/*_channel.rb`.
      def auto(root = nil)
        root ||= default_root
        return new([]) if root.nil?

        base = File.join(root.to_s, "app", "channels")
        names = Dir[File.join(base, "**", "*_channel.rb")].sort.map do |path|
          constant_name_for(path.delete_prefix("#{base}/"))
        end

        new(names - NEVER_SUBSCRIBABLE)
      end

      def default_root
        return nil unless defined?(::Rails) && ::Rails.respond_to?(:root) && ::Rails.root

        ::Rails.root
      end

      # "chat/room_channel.rb" -> "Chat::RoomChannel", the constant Zeitwerk
      # defines for that path. Acronym inflections are respected when
      # ActiveSupport is loaded.
      def constant_name_for(relative_path)
        relative_path.delete_suffix(".rb").split("/").map { |part| camelize(part) }.join("::")
      end

      private

      def camelize(part)
        if part.respond_to?(:camelize)
          part.camelize
        else
          part.split("_").map(&:capitalize).join
        end
      end
    end

    def initialize(names)
      @names = Array(names).map { |name| name.is_a?(Class) ? name.name : name.to_s }.uniq.freeze
    end

    attr_reader :names

    def empty?
      @names.empty?
    end

    def include?(name)
      @names.include?(name)
    end

    # @param name [String] the client-supplied `identifier["channel"]`
    # @return [Class, nil] the channel class, or nil when the name is not
    #   allowlisted or does not resolve to a usable channel
    def resolve(name)
      return nil unless name.is_a?(String)
      return nil unless include?(name)

      constant = constantize(name)
      return nil unless constant.is_a?(Class)
      return nil unless defined?(Orbitcast::Channel) && constant <= Orbitcast::Channel

      constant
    end

    # Boot-time check: every allowlisted name must actually be a channel, so a
    # typo is a deployment error rather than a runtime denial.
    def validate!
      unresolved = @names.reject { |name| resolve(name) }
      return self if unresolved.empty?

      raise ConfigurationError,
            "config.orbitcast.channels lists #{unresolved.join(", ")}, which " \
            "#{unresolved.one? ? "is not a" : "are not"} subclass#{unresolved.one? ? "" : "es"} " \
            "of Orbitcast::Channel"
    end

    private

    # Only ever called with a name that is already on the allowlist.
    def constantize(name)
      if name.respond_to?(:safe_constantize)
        name.safe_constantize
      else
        Object.const_get(name)
      end
    rescue NameError
      nil
    end
  end
end
