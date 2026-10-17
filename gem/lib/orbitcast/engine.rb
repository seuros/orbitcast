# frozen_string_literal: true

require "rails/engine"

module Orbitcast
  # Minimal Rails engine.
  #
  # It exists for two reasons only: to expose `config.orbitcast.*` and to carry
  # the installer generator and the migration path. It has no `app/` tree, adds
  # nothing to the autoload or eager-load paths, and mounts no routes in
  # signed mode.
  #
  # It deliberately does NOT start the relay. The Rust process is separately
  # supervised (`bundle exec orbitcast serve` under bin/dev or the production
  # process manager — Puma, shuttlebay, or otherwise); launching it from an
  # initializer would spawn one relay per app-server worker, per console, and
  # per rake task, each fighting over the same listener socket.
  class Engine < ::Rails::Engine
    isolate_namespace Orbitcast

    # Registers `config.orbitcast` in the shared railtie options so an
    # application can write `config.orbitcast.mode = :signed` in
    # config/environments/*.rb.
    config.orbitcast = ActiveSupport::OrderedOptions.new

    # No autoloading surprises: nothing in this gem's lib/ is autoloaded, and
    # the engine contributes no paths of its own.
    config.autoload_once_paths = []
    config.eager_load_paths = []

    initializer "orbitcast.configuration", before: :load_config_initializers do |app|
      options = app.config.orbitcast
      # Unknown keys raise here, at boot, rather than silently disabling a
      # security-relevant setting at runtime.
      Orbitcast.configure { |config| config.assign(options.to_h) }
    end

    # The installer copies Orbitcast's migrations into the application's own
    # `db/orbitcast_migrate/` (never into `db/migrate`, so they are
    # recognisable and removable). When that directory exists it is appended
    # to the application's migration paths, which is what `bin/rails
    # db:migrate` and `ActiveRecord::Tasks::DatabaseTasks.migrations_paths`
    # read. Nothing runs DDL at boot.
    initializer "orbitcast.migrations" do |app|
      migrate = app.root.join("db", "orbitcast_migrate")
      next unless migrate.directory?

      paths = app.config.paths["db/migrate"]
      paths << migrate.to_s unless paths.expanded.include?(migrate.to_s)
    end

    # The derived Orbitcast stream key: when no
    # `streams_secret` is configured explicitly, `Orbitcast::Configuration`
    # derives one from the application's key generator under the purpose
    # `orbitcast/streams/v1/<namespace>`. Only the derived 32 bytes ever leave
    # this process; `secret_key_base` and the master key do not. The namespace
    # may still arrive later (cable.yml is read lazily by the adapter and the
    # launcher), which is why the generator is handed over and the key is
    # derived on first use rather than here.
    config.after_initialize do |app|
      Orbitcast.config.key_generator ||= app.key_generator
    end

    # Rails-dispatch mode only: mount the private RPC endpoint the relay calls
    # back into. Signed-mode applications mount nothing.
    #
    # ##########################################################################
    # The mounted path speaks for every user of this application: it runs
    # `connect`, channel callbacks, and actions on behalf of whoever the relay
    # says is connected. It authenticates the relay's bearer credential itself,
    # but it must ALSO be blocked at the public proxy / load balancer:
    #
    #   # nginx
    #   location /_orbitcast/ { deny all; }
    #
    #   # Traefik / Kamal
    #   middlewares: [ orbitcast-internal-only ]
    #
    # Reach it from the relay over the private network or over TLS with a
    # rotating credential, never from the internet.
    # ##########################################################################
    initializer "orbitcast.rails_endpoint", after: :load_config_initializers do |app|
      next unless Orbitcast.config.dispatch?

      require "orbitcast/rails_endpoint"

      mount_path = Orbitcast.config.rpc_mount_path
      app.routes.append do
        mount Orbitcast::RailsEndpoint.new, at: mount_path, as: :orbitcast_rails_endpoint
      end

      Rails.logger&.info(
        "[orbitcast] Rails-dispatch endpoint mounted at #{mount_path}; block this path at the " \
        "public proxy."
      )
    end
  end
end
