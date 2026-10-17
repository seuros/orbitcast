# frozen_string_literal: true

require_relative "orbitcast/version"
require_relative "orbitcast/error"
require_relative "orbitcast/configuration"
require_relative "orbitcast/protocol"
require_relative "orbitcast/state_codec"
require_relative "orbitcast/streams"
require_relative "orbitcast/recovery"

# Orbitcast: Rails real-time stack backed by the Orbitcast Rust relay.
#
# Loading this file must not require Rails, ActionCable, or pg. Rails-touching
# code is loaded lazily:
#
#   * `Orbitcast::Engine` only when `Rails::Engine` is already defined
#   * `ActionCable::SubscriptionAdapter::Orbitcast` by ActionCable's own adapter
#     lookup
#   * `pg` by `Orbitcast::Publisher::Postgres` when it opens a connection
#
# Constant and file names follow the Zeitwerk convention so an application can
# reference `Orbitcast::Publisher::Postgres` without a classic-loader fallback.
module Orbitcast
  # Rails-dispatch surface. These need ActionCable, so they load on first
  # reference rather than at require time: a signed-mode
  # application, the executable wrapper, and the plain unit suite never touch
  # them.
  autoload :Channel, "orbitcast/channel"
  autoload :ChannelRegistry, "orbitcast/channel_registry"
  autoload :Connection, "orbitcast/connection"
  autoload :Dispatcher, "orbitcast/dispatcher"
  autoload :RailsEndpoint, "orbitcast/rails_endpoint"

  class << self
    # Global configuration. Applications normally write through the engine
    # (`config.orbitcast.*`) or `cable.yml`; `configure` is the direct path.
    def config
      @config ||= Configuration.new
    end

    def configure
      yield config if block_given?
      config
    end

    # Test/boot helper: drop the memoized configuration.
    def reset_config!
      @config = nil
    end

    def version
      VERSION
    end
  end
end

require_relative "orbitcast/binary"
require_relative "orbitcast/publisher/postgres"
require_relative "orbitcast/control"

# Rails integration, only when a host application already loaded Rails.
#
# The Rails-dispatch execution bases (`Orbitcast::Connection`,
# `Orbitcast::Channel`, `Orbitcast::Dispatcher`, `Orbitcast::RailsEndpoint`)
# need ActionCable and are NOT loaded here: the Engine requires the endpoint in
# dispatch mode, and an application's `ApplicationCable` classes require the
# bases themselves. A signed-mode application loads none of it.
require_relative "orbitcast/engine" if defined?(::Rails::Engine)
