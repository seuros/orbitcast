# frozen_string_literal: true

require "rails/generators/base"

module Orbitcast
  module Generators
    # `bin/rails generate orbitcast:install`
    #
    # Applies everything an application needs to talk to the relay,
    # idempotently — a second run changes nothing:
    #
    #   1. config/cable.yml                  — `adapter: orbitcast`, namespace, cable DSN
    #   2. config/initializers/orbitcast.rb  — mode and signing keys
    #   3. config/environments/{development,production}.rb
    #                                        — mount_path = nil, url, allowed origins, mode
    #   4. app/views/layouts/application.html.erb
    #                                        — `action_cable_meta_tag` in <head>, if missing
    #   5. Procfile.dev                      — a `cable:` process (created when absent)
    #   6. db/orbitcast_migrate/             — the overflow relay table, with --overflow
    #
    # It touches nothing else: importmap pins, `@rails/actioncable` imports,
    # Solid Queue/Cache configuration, and any environment setting already
    # present are left exactly as they are.
    class InstallGenerator < Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)

      desc "Configure Rails to publish through Orbitcast and let the Rust relay serve /cable."

      class_option :overflow,
                   type: :boolean,
                   default: false,
                   desc: "Copy the overflow-payload migration (db/orbitcast_migrate) and enable " \
                         "the relay for Turbo broadcasts larger than PostgreSQL's 8000-byte " \
                         "NOTIFY limit"

      # gem/lib/generators/orbitcast/install -> gem/db/orbitcast_migrate
      MIGRATION_SOURCE_DIR = File.expand_path("../../../../db/orbitcast_migrate", __dir__)
      MIGRATION_FILENAME = "001_create_orbitcast_payloads.rb"

      PROCFILE = "Procfile.dev"
      CABLE_PROCESS = "cable: bundle exec orbitcast serve"
      WEB_PROCESS = "web: bin/rails server"

      LAYOUT = "app/views/layouts/application.html.erb"
      META_TAG = "<%= action_cable_meta_tag %>"

      ENVIRONMENT_ANCHOR = /^Rails\.application\.configure do[ \t]*\n/

      # The settings injected per environment, keyed by the configuration
      # attribute they set. A line whose attribute is already assigned in the
      # file (uncommented) is skipped, so an application's own value wins.
      ENVIRONMENT_SETTINGS = {
        "development" => {
          "config.action_cable.mount_path" => "nil",
          "config.action_cable.url" => '"ws://localhost:28080/cable"',
          "config.action_cable.allowed_request_origins" =>
            '["http://localhost:3000", "http://127.0.0.1:3000"]',
          "config.orbitcast.mode" => ":signed"
        },
        "production" => {
          "config.action_cable.mount_path" => "nil",
          "config.action_cable.url" => 'ENV.fetch("ORBITCAST_CABLE_URL") # wss://app.example/cable',
          "config.action_cable.allowed_request_origins" => '[ENV.fetch("APP_ORIGIN")]',
          "config.orbitcast.mode" => ":signed"
        }
      }.freeze

      def create_cable_yml
        template "cable.yml.tt", "config/cable.yml"
      end

      def create_initializer
        template "orbitcast.rb.tt", "config/initializers/orbitcast.rb"
      end

      # `mount_path = nil` is what stops Rails serving a second /cable; the
      # URL and origins are what the relay and `action_cable_meta_tag` agree
      # on. Applied, not printed — and only for the lines not already there.
      def configure_environments
        ENVIRONMENT_SETTINGS.each do |environment, settings|
          path = "config/environments/#{environment}.rb"
          absolute = File.join(destination_root, path)

          unless File.file?(absolute)
            say_status :skip, "#{path} does not exist; add the Orbitcast settings by hand", :yellow
            next
          end

          content = File.read(absolute)
          unless content.match?(ENVIRONMENT_ANCHOR)
            say_status :skip, "#{path} has no `Rails.application.configure do` block to inject into", :yellow
            next
          end

          missing = settings.reject { |attribute, _| content.match?(/^\s*#{Regexp.escape(attribute)}\s*=/) }
          if missing.empty?
            say_status :identical, path, :blue
            next
          end

          block = +"  # Orbitcast: the Rust relay serves /cable; Rails must not mount a second one.\n"
          missing.each { |attribute, value| block << "  #{attribute} = #{value}\n" }
          block << "\n"

          inject_into_file path, block, after: ENVIRONMENT_ANCHOR
        end
      end

      # `createConsumer()` reads the cable URL from this tag. Without it the
      # browser falls back to /cable on the Rails host, which no longer serves
      # WebSockets.
      def add_action_cable_meta_tag
        absolute = File.join(destination_root, LAYOUT)

        unless File.file?(absolute)
          say_status :skip, "#{LAYOUT} does not exist; add #{META_TAG} to your layout's <head>", :yellow
          return
        end

        content = File.read(absolute)
        if content.include?("action_cable_meta_tag")
          say_status :identical, "#{LAYOUT} (action_cable_meta_tag)", :blue
          return
        end

        unless content.match?(%r{^\s*</head>})
          say_status :skip, "#{LAYOUT} has no </head> to insert #{META_TAG} before", :yellow
          return
        end

        inject_into_file LAYOUT, "    #{META_TAG}\n", before: %r{^\s*</head>}
      end

      # Development uses the application's existing process manager. Never start
      # the relay from an engine initializer: that would spawn one per
      # app-server worker, console, and rake task.
      def add_cable_process
        absolute = File.join(destination_root, PROCFILE)

        unless File.file?(absolute)
          create_file PROCFILE, "#{WEB_PROCESS}\n#{CABLE_PROCESS}\n"
          return
        end

        if File.read(absolute).match?(/^cable:/)
          say_status :skip, "#{PROCFILE} already defines a cable process", :yellow
          return
        end

        append_to_file PROCFILE, "#{CABLE_PROCESS}\n"
      end

      def copy_overflow_migration
        return unless options[:overflow]

        source = File.join(MIGRATION_SOURCE_DIR, MIGRATION_FILENAME)

        unless File.file?(source)
          say_status :skip,
                     "overflow migration is not bundled with this build (#{MIGRATION_FILENAME})",
                     :yellow
          return
        end

        create_file "db/orbitcast_migrate/#{MIGRATION_FILENAME}", File.read(source)
      end

      def print_post_install_instructions
        say ""
        say "Orbitcast installed.", :green
        say ""
        say "What was applied:"
        say "  config/cable.yml, config/initializers/orbitcast.rb,"
        say "  config/environments/development.rb + production.rb (mount_path = nil, url,"
        say "  allowed_request_origins, config.orbitcast.mode = :signed; existing values kept),"
        say "  action_cable_meta_tag in the application layout, and a Procfile.dev cable process."
        say "  Importmap pins, @rails/actioncable imports, Solid Queue and Solid Cache were not touched."
        if options[:overflow]
          say "  db/orbitcast_migrate/#{MIGRATION_FILENAME} (run bin/rails db:migrate) and"
          say "  overflow_enabled: true in cable.yml and the initializer."
        end
        say ""
        say "Next:"
        say "  1. Production: set ORBITCAST_CABLE_URL (wss://app.example/cable), APP_ORIGIN,"
        say "     ORBITCAST_DATABASE_URL and ORBITCAST_LISTEN_ADDR for the relay process."
        say "  2. Run the relay as a separate, supervised process:"
        say "       #{CABLE_PROCESS}"
        say ""
        say "Signed-mode capability boundary: signed streams only.", :yellow
        say "In signed mode NO connection or channel callbacks run:"
        say "ApplicationCable::Connection#connect, Channel#subscribed/#unsubscribed, and"
        say "`perform` actions are NOT executed by the relay. turbo_stream_from and"
        say "ActionCable.server.broadcast work; custom channel classes need mode: :dispatch."
        say ""
      end

      private

      # Namespace every publication and NOTIFY channel is scoped to. Derived
      # from the application module name so two applications sharing one
      # PostgreSQL database never share channels.
      def application_namespace
        base =
          if defined?(::Rails) && ::Rails.respond_to?(:application) && ::Rails.application
            ::Rails.application.class.module_parent_name.to_s
          else
            File.basename(destination_root.to_s)
          end

        sanitized = base.gsub(/([a-z\d])([A-Z])/, '\1_\2').downcase.gsub(/[^a-z0-9_]+/, "_")
        sanitized = sanitized.sub(/\A[^a-z0-9]+/, "").sub(/_+\z/, "")
        sanitized = "app" if sanitized.empty?
        # Configuration::NAMESPACE_PATTERN allows 64 characters; leave room for
        # the "_development" / "_production" suffix.
        sanitized[0, 40]
      end

      def overflow?
        options[:overflow]
      end
    end
  end
end
