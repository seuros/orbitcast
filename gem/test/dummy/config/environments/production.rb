# frozen_string_literal: true

Rails.application.configure do
  config.enable_reloading = false
  config.eager_load = true
  config.consider_all_requests_local = false

  # The Orbitcast production wiring the installer prints as instructions.
  config.action_cable.mount_path = nil
  config.action_cable.url = ENV.fetch("ORBITCAST_CABLE_URL", "wss://example.test/cable")
  config.action_cable.allowed_request_origins = [ENV.fetch("APP_ORIGIN", "https://example.test")]
end
