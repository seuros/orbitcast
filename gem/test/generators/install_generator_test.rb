# frozen_string_literal: true

require "test_helper"

require "erb"
require "fileutils"
require "yaml"

# The plain-ruby suite (`rake test:gem`, CI) runs with no Bundler and no Rails.
# Load the generator machinery only if railties is actually available, so this
# file never turns the unit suite red on a Rails-less checkout.
begin
  require "rails/generators"
  require "rails/generators/test_case"
  require "generators/orbitcast/install/install_generator"
  RAILS_GENERATORS_AVAILABLE = true
rescue LoadError
  RAILS_GENERATORS_AVAILABLE = false
end

if RAILS_GENERATORS_AVAILABLE
  class InstallGeneratorTest < Rails::Generators::TestCase
    tests Orbitcast::Generators::InstallGenerator

    # The basename becomes the derived namespace when no Rails application is
    # booted, which is exactly the case in this suite.
    destination File.expand_path("../../tmp/generators/my_app", __dir__)

    setup :prepare_destination

    def test_generates_a_cable_yml_for_the_orbitcast_adapter
      run_generator

      assert_file "config/cable.yml" do |content|
        assert_match(/^development:\n  adapter: orbitcast$/, content)
        assert_match(/^production:\n  adapter: orbitcast$/, content)
        assert_match(/namespace: "my_app_development"/, content)
        assert_match(/namespace: "my_app_production"/, content)
        assert_match(/publish_via: postgres/, content)
        # ERB in the *generated* file, evaluated by Rails at load time.
        assert_match(/database_url: <%= ENV\.fetch\("ORBITCAST_DATABASE_URL"\) %>/, content)
        # Rails' own adapter stays available for application tests.
        assert_match(/^test:\n  adapter: test$/, content)
      end
    end

    def test_generated_cable_yml_assigns_cleanly_into_the_configuration
      run_generator

      raw = File.read(File.join(destination_root, "config/cable.yml"))
      ENV["ORBITCAST_DATABASE_URL"] = "postgres://cable@localhost/my_app_cable"
      rendered = YAML.safe_load(ERB.new(raw).result(binding), aliases: true)

      %w[development production].each do |environment|
        settings = rendered.fetch(environment)

        assert_equal "orbitcast", settings.fetch("adapter")

        # Configuration#assign rejects unknown keys and invalid values, so this
        # proves the template emits nothing the gem would refuse at boot.
        config = Orbitcast::Configuration.new.assign(settings)

        assert_equal "my_app_#{environment}", config.namespace
        assert_equal :postgres, config.publish_via
        config.validate!
      end
    ensure
      ENV.delete("ORBITCAST_DATABASE_URL")
    end

    def test_generates_an_initializer_pinning_signed_mode_and_the_turbo_key
      run_generator

      assert_file "config/initializers/orbitcast.rb" do |content|
        assert_match(/Orbitcast\.configure do \|config\|/, content)
        assert_match(/config\.mode = :signed/, content)
        assert_match(/credentials\.dig\(:orbitcast, :streams_secret\)/, content)
        assert_match(/ENV\["ORBITCAST_STREAMS_SECRET"\]/, content)
        # Turbo's key only exists after initialization.
        assert_match(/config\.after_initialize do/, content)
        assert_match(/config\.turbo_secret = Turbo\.signed_stream_verifier_key/, content)
        # Only a comment may mention secret_key_base; no executable line may use it.
        code = content.lines.reject { |line| line.strip.start_with?("#") }.join

        refute_match(/secret_key_base|RAILS_MASTER_KEY/, code)
      end
    end

    def test_appends_a_cable_process_to_an_existing_procfile_dev
      File.write(File.join(destination_root, "Procfile.dev"), "web: bin/rails server\n")

      run_generator

      assert_file "Procfile.dev" do |content|
        assert_equal "web: bin/rails server\ncable: bundle exec orbitcast serve\n", content
      end
    end

    def test_creates_a_procfile_dev_when_the_application_has_none
      run_generator

      assert_file "Procfile.dev" do |content|
        assert_equal "web: bin/rails server\ncable: bundle exec orbitcast serve\n", content
      end
    end

    def test_does_not_duplicate_an_existing_cable_process
      File.write(File.join(destination_root, "Procfile.dev"), "web: bin/rails server\ncable: something else\n")

      output = run_generator

      assert_match(/skip/, output)
      assert_file "Procfile.dev" do |content|
        assert_equal 1, content.scan(/^cable:/).size
      end
    end

    # --- environment files ----------------------------------------------------

    ENVIRONMENT_STUB = <<~RUBY
      # frozen_string_literal: true

      require "active_support/core_ext/integer/time"

      Rails.application.configure do
        config.eager_load = true
        config.solid_queue.connects_to = { database: { writing: :queue } }
        config.cache_store = :solid_cache_store
      end
    RUBY

    def test_applies_the_cable_settings_to_development_and_production
      write_environment("development")
      write_environment("production")

      run_generator

      assert_file "config/environments/production.rb" do |content|
        assert_match(/^  config\.action_cable\.mount_path = nil$/, content)
        assert_match(%r{^  config\.action_cable\.url = ENV\.fetch\("ORBITCAST_CABLE_URL"\) # wss://app\.example/cable$}, content)
        assert_match(/^  config\.action_cable\.allowed_request_origins = \[ENV\.fetch\("APP_ORIGIN"\)\]$/, content)
        assert_match(/^  config\.orbitcast\.mode = :signed$/, content)
        # Injected right after the configure line, before the application's own settings.
        assert_operator content.index("config.action_cable.mount_path"), :<, content.index("config.eager_load")
        # Solid Queue / Solid Cache untouched.
        assert_includes content, "config.solid_queue.connects_to = { database: { writing: :queue } }"
        assert_includes content, "config.cache_store = :solid_cache_store"
      end

      assert_file "config/environments/development.rb" do |content|
        assert_match(/^  config\.action_cable\.mount_path = nil$/, content)
        assert_match(%r{^  config\.action_cable\.url = "ws://localhost:28080/cable"$}, content)
        assert_match(%r{^  config\.action_cable\.allowed_request_origins = \["http://localhost:3000", "http://127\.0\.0\.1:3000"\]$}, content)
        assert_match(/^  config\.orbitcast\.mode = :signed$/, content)
      end
    end

    def test_environment_injection_is_idempotent
      write_environment("production")

      run_generator
      first = File.read(File.join(destination_root, "config/environments/production.rb"))
      output = run_generator

      assert_equal first, File.read(File.join(destination_root, "config/environments/production.rb"))
      assert_match(/identical\s+config\/environments\/production\.rb/, output)
    end

    def test_existing_environment_settings_are_kept_not_overwritten
      write_environment(
        "production",
        body: "  config.action_cable.url = ENV.fetch(\"CABLE_URL\")\n" \
              "  config.action_cable.allowed_request_origins = [\"https://mine.example\"]\n"
      )

      run_generator

      assert_file "config/environments/production.rb" do |content|
        assert_equal 1, content.scan(/config\.action_cable\.url =/).size
        assert_includes content, 'config.action_cable.url = ENV.fetch("CABLE_URL")'
        assert_includes content, 'allowed_request_origins = ["https://mine.example"]'
        refute_includes content, "APP_ORIGIN"
        # The two missing settings are still added.
        assert_match(/^  config\.action_cable\.mount_path = nil$/, content)
        assert_match(/^  config\.orbitcast\.mode = :signed$/, content)
      end
    end

    def test_a_missing_environment_file_is_skipped_loudly
      output = run_generator

      assert_no_file "config/environments/production.rb"
      assert_match(%r{skip\s+config/environments/production\.rb does not exist}, output)
    end

    def test_an_environment_file_without_a_configure_block_is_skipped
      path = File.join(destination_root, "config/environments/production.rb")
      FileUtils.mkdir_p(File.dirname(path))
      File.write(path, "# nothing here\n")

      output = run_generator

      assert_match(/no `Rails\.application\.configure do` block/, output)
      assert_equal "# nothing here\n", File.read(path)
    end

    # --- layout ------------------------------------------------------------------

    LAYOUT_WITHOUT_TAG = <<~HTML
      <!DOCTYPE html>
      <html>
        <head>
          <title>App</title>
          <%= csrf_meta_tags %>
          <%= javascript_importmap_tags %>
        </head>
        <body><%= yield %></body>
      </html>
    HTML

    def test_inserts_the_action_cable_meta_tag_into_the_layout_head
      write_layout(LAYOUT_WITHOUT_TAG)

      run_generator

      assert_file "app/views/layouts/application.html.erb" do |content|
        assert_equal 1, content.scan("action_cable_meta_tag").size
        assert_operator content.index("action_cable_meta_tag"), :<, content.index("</head>")
        assert_operator content.index("javascript_importmap_tags"), :<, content.index("action_cable_meta_tag")
      end
    end

    def test_leaves_a_layout_that_already_has_the_meta_tag_alone
      layout = LAYOUT_WITHOUT_TAG.sub("<%= csrf_meta_tags %>", "<%= csrf_meta_tags %>\n    <%= action_cable_meta_tag %>")
      write_layout(layout)

      output = run_generator

      assert_equal layout, File.read(File.join(destination_root, "app/views/layouts/application.html.erb"))
      assert_match(/identical/, output)
    end

    def test_a_missing_layout_is_skipped_loudly
      output = run_generator

      assert_match(%r{skip\s+app/views/layouts/application\.html\.erb does not exist}, output)
    end

    # The installer never rewrites JavaScript wiring.
    def test_importmap_pins_are_left_untouched
      importmap = "pin \"application\"\npin \"@rails/actioncable\", to: \"actioncable.esm.js\"\n"
      FileUtils.mkdir_p(File.join(destination_root, "config"))
      File.write(File.join(destination_root, "config/importmap.rb"), importmap)

      run_generator

      assert_equal importmap, File.read(File.join(destination_root, "config/importmap.rb"))
    end

    def test_prints_the_signed_only_capability_boundary
      output = run_generator

      assert_match(/signed streams only/i, output)
      assert_match(/NO connection or channel callbacks/, output)
      assert_match(/perform/, output)
      assert_match(/ApplicationCable::Connection#connect/, output)
    end

    def test_overflow_migration_is_opt_in
      run_generator

      assert_no_file "db/orbitcast_migrate/001_create_orbitcast_payloads.rb"
      assert_file "config/cable.yml" do |content|
        refute_match(/^\s+overflow_enabled:/, content)
      end
      assert_file "config/initializers/orbitcast.rb" do |content|
        assert_match(/^  # config\.overflow_enabled = true$/, content)
        refute_match(/^  config\.overflow_enabled = true$/, content)
      end
    end

    def test_overflow_flag_enables_the_relay_in_cable_yml_and_the_initializer
      run_generator(["--overflow"])

      assert_file "config/cable.yml" do |content|
        assert_equal 2, content.scan(/^  overflow_enabled: true$/).size, "development and production"
      end
      assert_file "config/initializers/orbitcast.rb" do |content|
        assert_match(/^  config\.overflow_enabled = true$/, content)
      end

      raw = File.read(File.join(destination_root, "config/cable.yml"))
      ENV["ORBITCAST_DATABASE_URL"] = "postgres://cable@localhost/my_app_cable"
      rendered = YAML.safe_load(ERB.new(raw).result(binding), aliases: true)

      assert_predicate Orbitcast::Configuration.new.assign(rendered.fetch("production")), :overflow_enabled?
    ensure
      ENV.delete("ORBITCAST_DATABASE_URL")
    end

    def test_overflow_flag_copies_the_bundled_migration_when_present
      source = File.join(
        Orbitcast::Generators::InstallGenerator::MIGRATION_SOURCE_DIR,
        Orbitcast::Generators::InstallGenerator::MIGRATION_FILENAME
      )

      output = run_generator(["--overflow"])

      if File.file?(source)
        assert_file "db/orbitcast_migrate/001_create_orbitcast_payloads.rb" do |content|
          assert_equal File.read(source), content
        end
      else
        # The migration ships from gem/db/orbitcast_migrate/; a build without it
        # must skip loudly rather than write a broken migration.
        assert_match(/skip/, output)
        assert_no_file "db/orbitcast_migrate/001_create_orbitcast_payloads.rb"
      end
    end

    private

    def write_environment(name, body: "")
      path = File.join(destination_root, "config/environments/#{name}.rb")
      FileUtils.mkdir_p(File.dirname(path))
      File.write(path, ENVIRONMENT_STUB.sub("Rails.application.configure do\n", "Rails.application.configure do\n#{body}"))
    end

    def write_layout(content)
      path = File.join(destination_root, "app/views/layouts/application.html.erb")
      FileUtils.mkdir_p(File.dirname(path))
      File.write(path, content)
    end
  end
else
  # Keeps the plain-ruby suite honest about what it did not run.
  class InstallGeneratorUnavailableTest < Minitest::Test
    def test_generator_tests_require_railties
      skip "railties is not available; run the generator tests with BUNDLE_GEMFILE=gem/Gemfile"
    end
  end
end
