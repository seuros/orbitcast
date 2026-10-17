# frozen_string_literal: true

require_relative "lib/orbitcast/version"

Gem::Specification.new do |spec|
  spec.name = "orbitcast"
  spec.version = Orbitcast::VERSION
  spec.authors = ["Abdelkader Boudih"]
  spec.email = ["oss@seuros.com"]

  spec.summary = "Rails real-time stack: ActionCable-compatible WebSocket relay in Rust"
  spec.description = <<~DESCRIPTION.strip
    Orbitcast serves the ActionCable JSON protocol from a separately supervised
    Rust process and publishes Rails broadcasts through PostgreSQL NOTIFY. This
    release covers signed-stream subscriptions and broadcasting; remote
    connection and channel callbacks are not executed.
  DESCRIPTION

  spec.homepage = "https://github.com/seuros/orbitcast"
  spec.license = "BSD-3-Clause"
  spec.required_ruby_version = ">= 4.0"

  spec.metadata = {
    "source_code_uri" => spec.homepage,
    "changelog_uri" => "#{spec.homepage}/blob/master/CHANGELOG.md",
    "bug_tracker_uri" => "#{spec.homepage}/issues",
    "rubygems_mfa_required" => "true"
  }

  # Explicit allowlist. No Dir.glob over the repository: study checkouts, build
  # trees, fixture applications, and other platforms' binaries must never end up
  # in a package by accident.
  #
  # `vendor/<gem-platform>/bin/orbitcast` and its manifest are appended by the
  # native packaging rake task, which also sets `spec.platform` for the target
  # (`arm64-darwin`, `x86_64-darwin`, `x86_64-linux-gnu`, `aarch64-linux-gnu`,
  # `x86_64-linux-musl`). This gemspec stays platform-agnostic: the GNU labels
  # must be spelled out there because RubyGems treats a bare `*-linux` as a libc
  # wildcard that can also match musl.
  spec.files = [
    "LICENSE.txt",
    "README.md",
    "app/assets/javascripts/orbitcast/recovery.js",
    "db/orbitcast_migrate/001_create_orbitcast_payloads.rb",
    "db/orbitcast_migrate/002_create_orbitcast_presence.rb",
    "db/orbitcast_migrate/003_create_orbitcast_broker.rb",
    "exe/orbitcast",
    "lib/action_cable/subscription_adapter/orbitcast.rb",
    "lib/generators/orbitcast/install/install_generator.rb",
    "lib/generators/orbitcast/install/templates/cable.yml.tt",
    "lib/generators/orbitcast/install/templates/orbitcast.rb.tt",
    "lib/orbitcast.rb",
    "lib/orbitcast/binary.rb",
    "lib/orbitcast/channel.rb",
    "lib/orbitcast/channel_registry.rb",
    "lib/orbitcast/configuration.rb",
    "lib/orbitcast/connection.rb",
    "lib/orbitcast/control.rb",
    "lib/orbitcast/dispatcher.rb",
    "lib/orbitcast/engine.rb",
    "lib/orbitcast/error.rb",
    "lib/orbitcast/launcher.rb",
    "lib/orbitcast/protocol.rb",
    "lib/orbitcast/protocol/v1.rb",
    "lib/orbitcast/protocol/v1/control_message.rb",
    "lib/orbitcast/protocol/v1/effects.rb",
    "lib/orbitcast/protocol/v1/reply.rb",
    "lib/orbitcast/protocol/v1/request.rb",
    "lib/orbitcast/publisher/control.rb",
    "lib/orbitcast/publisher/http.rb",
    "lib/orbitcast/publisher/instrumentation.rb",
    "lib/orbitcast/publisher/postgres.rb",
    "lib/orbitcast/rails_endpoint.rb",
    "lib/orbitcast/recovery.rb",
    "lib/orbitcast/state_codec.rb",
    "lib/orbitcast/streams.rb",
    "lib/orbitcast/version.rb"
  ].freeze

  missing = spec.files.reject { |file| File.exist?(File.expand_path(file, __dir__)) }
  raise "orbitcast.gemspec lists files that do not exist: #{missing.join(", ")}" unless missing.empty?

  spec.bindir = "exe"
  spec.executables = ["orbitcast"]
  spec.require_paths = ["lib"]

  spec.add_dependency "actioncable", ">= 8.1"
  spec.add_dependency "pg", "~> 1.5"
  spec.add_dependency "railties", ">= 8.1"
end
