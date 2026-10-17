# frozen_string_literal: true

require_relative "boot"

require "rails"
require "active_model/railtie"
require "active_job/railtie"
require "active_record/railtie"
require "action_controller/railtie"
require "action_view/railtie"
require "action_cable/engine"

require "propshaft"
require "importmap-rails"
require "turbo-rails"

require "orbitcast"
# `orbitcast` only pulls the engine in when Rails is already loaded at require
# time. Requiring it explicitly makes the order irrelevant, which matters when
# a test harness loaded the gem before booting the application.
require "orbitcast/engine"

module Dummy
  # Minimal but real Rails 8.1 application: Propshaft + importmap-rails +
  # turbo-rails, one page that renders `turbo_stream_from "demo"`, and SQLite
  # for the application's own (unused) database. It exists to produce the real
  # Turbo signing fixtures in tests/fixtures/signed_streams/ and to host the
  # future WebSocket integration harness.
  #
  # It deliberately has no ActiveRecord models and never connects: the fixture
  # export must work on a checkout with no database at all.
  class Application < Rails::Application
    config.load_defaults 8.1

    config.root = File.expand_path("..", __dir__)
    config.eager_load = false
    config.consider_all_requests_local = true
    config.secret_key_base = "dummy-secret-key-base-for-the-orbitcast-fixture-application"

    # Rails must not mount a second /cable endpoint: Orbitcast serves it from
    # the separately supervised Rust process.
    config.action_cable.mount_path = nil
    config.action_cable.url = ENV.fetch("ORBITCAST_CABLE_URL", "ws://localhost:28080/cable")
    config.action_cable.allowed_request_origins = ["http://localhost:3000"]

    # Signed mode by default: that is what the fixture exporters and the
    # browser harness use. `ORBITCAST_MODE=dispatch` switches the same
    # application to the Rails execution model, which is what
    # gem/test/rails_contract_test.rb drives.
    config.orbitcast.mode = ENV.fetch("ORBITCAST_MODE", "signed").to_sym
    config.orbitcast.namespace = "dummy_#{Rails.env}"

    if config.orbitcast.mode == :dispatch
      # The raw service credential. The relay gets the same bytes as strict
      # base64 through the launcher; Rails compares the raw value.
      config.orbitcast.rpc_token = ENV.fetch("ORBITCAST_RPC_TOKEN")
      # An explicit, reviewed allowlist. Nothing else is ever constantized
      # from a client-supplied identifier.
      config.orbitcast.channels = %w[ChatChannel]
    end

    config.generators.system_tests = nil
    config.active_support.to_time_preserves_timezone = :zone
  end
end
