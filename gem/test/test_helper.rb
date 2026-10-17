# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "orbitcast"

require "minitest/autorun"

# These tests must run with plain `ruby -Ilib -Itest`: no Rails boot, no
# ActionCable, no PostgreSQL. Anything that needs those is loaded lazily by the
# gem and covered by the integration suites instead.
module Orbitcast
  module TestHelper
    NAMESPACE = "example_production"
    DATABASE_URL = "postgres://cable@localhost/orbitcast_test"
    STREAMS_SECRET = "streams-secret-key"

    def build_config(**overrides)
      config = Orbitcast::Configuration.new
      config.assign(
        {
          namespace: NAMESPACE,
          database_url: DATABASE_URL,
          streams_secret: STREAMS_SECRET
        }.merge(overrides)
      )
      config
    end

    def with_global_config(**overrides)
      previous = Orbitcast.config
      Orbitcast.reset_config!
      Orbitcast.configure { |config| config.assign(overrides) }
      yield Orbitcast.config
    ensure
      Orbitcast.instance_variable_set(:@config, previous)
    end
  end
end

module Minitest
  class Test
    include Orbitcast::TestHelper
  end
end
