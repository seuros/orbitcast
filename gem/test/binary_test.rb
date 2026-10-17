# frozen_string_literal: true

require "test_helper"
require "json"
require "tmpdir"

class BinaryTest < Minitest::Test
  def test_override_resolves_to_an_absolute_path
    with_fake_binary do |path|
      assert_equal path, Orbitcast::Binary.path(env: { "ORBITCAST_BINARY" => path })
    end
  end

  def test_override_expands_relative_paths
    with_fake_binary do |path|
      Dir.chdir(File.dirname(path)) do
        assert_equal path, Orbitcast::Binary.path(env: { "ORBITCAST_BINARY" => "./orbitcast" })
      end
    end
  end

  def test_override_pointing_at_a_missing_file_is_actionable
    error = assert_raises(Orbitcast::BinaryNotFoundError) do
      Orbitcast::Binary.path(env: { "ORBITCAST_BINARY" => "/nonexistent/orbitcast" })
    end

    assert_includes error.message, "ORBITCAST_BINARY=/nonexistent/orbitcast"
    assert_includes error.message, "does not point at a file"
  end

  def test_override_pointing_at_a_non_executable_file_is_actionable
    with_fake_binary(mode: 0o644) do |path|
      error = assert_raises(Orbitcast::BinaryNotFoundError) do
        Orbitcast::Binary.path(env: { "ORBITCAST_BINARY" => path })
      end

      assert_includes error.message, "is not executable"
      assert_includes error.message, "chmod +x"
    end
  end

  def test_empty_override_falls_back_to_the_vendored_binary
    error = assert_raises(Orbitcast::BinaryNotFoundError) do
      Orbitcast::Binary.path(env: { "ORBITCAST_BINARY" => "" })
    end

    # No binary is vendored in a source checkout, so this is the expected path.
    assert_includes error.message, "No orbitcast executable for platform"
    assert_includes error.message, "ORBITCAST_BINARY=/path/to/target/release/orbitcast"
    assert_includes error.message, "never downloads or compiles"
  end

  def test_vendored_path_is_relative_to_the_gem
    expected = File.expand_path("../vendor", __dir__)

    assert_equal expected, Orbitcast::Binary.vendor_root
    assert_equal(
      File.join(expected, Orbitcast::Binary.gem_platform, "bin", "orbitcast"),
      Orbitcast::Binary.vendored_binary
    )
  end

  def test_gem_platform_labels
    {
      # macOS: the OS version is dropped, we do not build per darwin release.
      "arm64-darwin-23" => "arm64-darwin",
      "x86_64-darwin-22" => "x86_64-darwin",
      # Linux: a bare `*-linux` is a libc wildcard in RubyGems, so it is
      # resolved to gnu explicitly here.
      "x86_64-linux" => "x86_64-linux-gnu",
      "x86_64-linux-gnu" => "x86_64-linux-gnu",
      "aarch64-linux" => "aarch64-linux-gnu",
      "aarch64-linux-gnu" => "aarch64-linux-gnu",
      "x86_64-linux-musl" => "x86_64-linux-musl",
      "aarch64-linux-musl" => "aarch64-linux-musl"
    }.each do |local, expected|
      assert_equal expected, Orbitcast::Binary.gem_platform(Gem::Platform.new(local)),
                   "Gem::Platform.new(#{local.inspect})"
    end
  end

  def test_local_platform_is_one_of_the_supported_shapes
    assert_match(/\A[a-z0-9_]+-(darwin|linux-(gnu|musl))\z/, Orbitcast::Binary.gem_platform)
  end

  def test_vendored_predicate_is_false_in_a_source_checkout
    refute Orbitcast::Binary.vendored?
  end

  # --- override verification (`doctor --json`) -----------------------------------

  def test_a_matching_override_passes_verification
    with_doctor_binary(report(version: Orbitcast::VERSION)) do |path|
      resolved = Orbitcast::Binary.path(env: override_env(path), verify: true, require_features: ["postgres"])

      assert_equal path, resolved
    end
  end

  def test_verification_is_skipped_unless_asked_for
    with_doctor_binary(report(version: "0.0.1")) do |path|
      assert_equal path, Orbitcast::Binary.path(env: override_env(path))
      assert_equal path, Orbitcast::Binary.path(env: override_env(path), verify: false)
    end
  end

  def test_a_version_mismatch_is_named_precisely
    with_doctor_binary(report(version: "9.9.9")) do |path|
      error = assert_raises(Orbitcast::BinaryError) do
        Orbitcast::Binary.path(env: override_env(path), verify: true)
      end

      assert_includes error.message, "is orbitcast 9.9.9"
      assert_includes error.message, "this gem is orbitcast #{Orbitcast::VERSION}"
      assert_includes error.message, "ORBITCAST_BINARY=#{path}"
    end
  end

  def test_a_missing_required_feature_is_named_precisely
    with_doctor_binary(report(version: Orbitcast::VERSION, features: ["memory"])) do |path|
      error = assert_raises(Orbitcast::BinaryError) do
        Orbitcast::Binary.path(env: override_env(path), verify: true, require_features: ["postgres"])
      end

      assert_includes error.message, "without the postgres feature"
      assert_includes error.message, "--features postgres"

      # Nothing required: a memory-only build is acceptable for publish_via :http.
      assert_equal path, Orbitcast::Binary.path(env: override_env(path), verify: true, require_features: [])
    end
  end

  def test_a_binary_without_envelope_version_one_is_refused
    with_doctor_binary(report(version: Orbitcast::VERSION, envelope_versions: [2])) do |path|
      error = assert_raises(Orbitcast::BinaryError) do
        Orbitcast::Binary.path(env: override_env(path), verify: true)
      end

      assert_includes error.message, "envelope versions [2]"
      assert_includes error.message, "requires version 1"
    end
  end

  def test_a_binary_that_prints_no_json_is_refused
    with_fake_binary(body: "#!/bin/sh\necho 'orbitcast 0.1.0' >&2\nexit 2\n") do |path|
      error = assert_raises(Orbitcast::BinaryError) do
        Orbitcast::Binary.path(env: override_env(path), verify: true)
      end

      assert_includes error.message, "did not print a JSON report"
      assert_includes error.message, "exit status 2"
      assert_includes error.message, "stderr: orbitcast 0.1.0"
    end
  end

  def test_a_hung_doctor_is_killed_after_the_timeout
    with_fake_binary(body: "#!/bin/sh\nsleep 30\n") do |path|
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      error = assert_raises(Orbitcast::BinaryError) do
        Orbitcast::Binary.path(env: override_env(path), verify: true, timeout: 0.3)
      end

      assert_includes error.message, "did not finish within 0.3s"
      assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :<, 5
    end
  end

  # The doctor subprocess gets a minimal environment: the parent's secrets are
  # not something a build check needs.
  def test_doctor_runs_without_the_parents_secrets
    body = <<~SH
      #!/bin/sh
      if [ -n "$RAILS_MASTER_KEY" ] || [ -n "$SECRET_KEY_BASE" ] || [ -n "$DATABASE_URL" ]; then
        echo '{"version":"leaked"}'
      else
        echo '#{report(version: Orbitcast::VERSION)}'
      fi
    SH
    with_fake_binary(body: body) do |path|
      env = override_env(path).merge(
        "RAILS_MASTER_KEY" => "m", "SECRET_KEY_BASE" => "s", "DATABASE_URL" => "postgres://x"
      )

      assert_equal path, Orbitcast::Binary.path(env: env, verify: true)
    end
  end

  private

  def report(version:, features: ["postgres"], envelope_versions: [1])
    JSON.generate(
      version: version, target: "x86_64-unknown-linux-gnu",
      features: features, envelope_versions: envelope_versions
    )
  end

  def override_env(path)
    { "ORBITCAST_BINARY" => path, "PATH" => ENV.fetch("PATH"), "HOME" => ENV["HOME"] }
  end

  def with_doctor_binary(json, &block)
    with_fake_binary(body: "#!/bin/sh\n[ \"$1\" = doctor ] || exit 1\necho '#{json}'\n", &block)
  end

  def with_fake_binary(mode: 0o755, body: "#!/bin/sh\nexit 0\n")
    Dir.mktmpdir("orbitcast-binary") do |dir|
      path = File.join(dir, "orbitcast")
      File.write(path, body)
      File.chmod(mode, path)
      yield File.realpath(path)
    end
  end
end
