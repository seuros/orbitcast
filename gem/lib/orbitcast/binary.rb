# frozen_string_literal: true

require "json"
require "open3"
require "rbconfig"
require "rubygems"

require_relative "error"
require_relative "version"

module Orbitcast
  # Resolves the vendored Orbitcast executable.
  #
  # Resolution is relative to *this* file, i.e. the activated gem, never a
  # recursive `PATH` search: a native gem ships exactly one binary and that is
  # the one it must run. `ORBITCAST_BINARY` overrides it for source builds and
  # for platforms without a published artifact. There is no download, no
  # compiler invocation, and no architecture guessing.
  #
  # An override is *verified* before it is used: `<binary> doctor
  # --json` must report this gem's
  # version, envelope version 1, and every feature the configuration needs.
  # The vendored binary was packaged with this exact gem version and skips the
  # check.
  module Binary
    EXECUTABLE = "orbitcast"
    OVERRIDE_ENV = "ORBITCAST_BINARY"

    # Envelope version this gem speaks; the binary must list it.
    ENVELOPE_VERSION = 1

    # Upper bound on `doctor --json`. A binary that cannot describe itself in
    # this long is not one to hand the listener socket to.
    DOCTOR_TIMEOUT = 5.0

    class << self
      # @param env [Hash] process environment (ENV, or a Hash in tests)
      # @param verify [Boolean] run `doctor --json` on an override
      # @param require_features [Array<String>] features the override must
      #   have been built with (e.g. `["postgres"]` for `publish_via: postgres`)
      # @param timeout [Numeric] seconds to wait for `doctor --json`
      # @return [String] absolute path to an existing executable file
      def path(env: ENV, verify: false, require_features: [], timeout: DOCTOR_TIMEOUT)
        override = env[OVERRIDE_ENV]
        return vendored_path if override.nil? || override.empty?

        resolved = override_path(override)
        verify!(resolved, require_features: require_features, timeout: timeout, env: env) if verify
        resolved
      end

      # Run `<binary> doctor --json` and check what it reports against this
      # gem. Public so `orbitcast doctor` style tooling can reuse it.
      #
      # @raise [BinaryError] with a message naming the exact mismatch
      # @return [Hash] the parsed doctor report
      def verify!(binary, require_features: [], timeout: DOCTOR_TIMEOUT, env: ENV)
        report = doctor_report(binary, timeout: timeout, env: env)

        version = report["version"].to_s
        unless version == VERSION
          raise BinaryError,
                "#{OVERRIDE_ENV}=#{binary} is orbitcast #{version.empty? ? "(unknown version)" : version}, " \
                "but this gem is orbitcast #{VERSION}. Build the matching release " \
                "(git checkout v#{VERSION} && cargo build --release --features postgres) or install " \
                "the platform gem instead of overriding the binary."
        end

        envelopes = Array(report["envelope_versions"]).map { |value| Integer(value, exception: false) }
        unless envelopes.include?(ENVELOPE_VERSION)
          raise BinaryError,
                "#{OVERRIDE_ENV}=#{binary} speaks envelope versions " \
                "#{envelopes.compact.inspect}, but this gem requires version #{ENVELOPE_VERSION}."
        end

        features = Array(report["features"]).map(&:to_s)
        missing = Array(require_features).map(&:to_s) - features
        unless missing.empty?
          raise BinaryError,
                "#{OVERRIDE_ENV}=#{binary} was built without the #{missing.join(", ")} " \
                "feature#{"s" if missing.size > 1} (reported features: #{features.inspect}). " \
                "Rebuild with: cargo build --release --features #{missing.join(",")}"
        end

        report
      end

      # @return [Boolean] whether a binary is available without an override
      def vendored?
        File.file?(vendored_binary) && File.executable?(vendored_binary)
      end

      def vendored_binary(platform = gem_platform)
        File.join(vendor_root, platform, "bin", EXECUTABLE)
      end

      def manifest_path(platform = gem_platform)
        File.join(vendor_root, platform, "manifest.json")
      end

      # gem/vendor -- `__dir__` is gem/lib/orbitcast.
      def vendor_root
        File.expand_path("../../vendor", __dir__)
      end

      # Gem platform label used for the vendor directory.
      #
      # `Gem::Platform.local` carries an OS version we do not build against
      # (`arm64-darwin-23`), so darwin is truncated to `<cpu>-darwin`. On Linux
      # the version field is the libc: RubyGems treats a bare `*-linux` as a
      # libc wildcard that can also match musl, so we resolve it explicitly to
      # `gnu` here and only use `-musl` for an actual musl build.
      def gem_platform(platform = Gem::Platform.local)
        cpu = platform.cpu || "unknown"

        case platform.os
        when "linux"
          libc = platform.version.to_s
          libc = "gnu" if libc.empty? || libc == "linux"
          "#{cpu}-linux-#{libc}"
        when "darwin"
          "#{cpu}-darwin"
        else
          [cpu, platform.os, platform.version].compact.reject(&:empty?).join("-")
        end
      end

      private

      # `doctor --json` in a minimal environment: it needs no configuration to
      # report its version and build, and must not see application secrets.
      def doctor_report(binary, timeout:, env:)
        child_env = { "PATH" => env["PATH"], "HOME" => env["HOME"], "ORBITCAST_LOG" => "error" }.compact
        stdout = stderr = ""
        status = nil

        Open3.popen3(child_env, [binary, binary], "doctor", "--json",
                     unsetenv_others: true, close_others: true) do |stdin, out, err, waiter|
          stdin.close
          out_reader = Thread.new { out.read }
          err_reader = Thread.new { err.read }
          # popen3 closes the pipes when this block unwinds; a reader still
          # blocked in IO#read would then die with IOError and Ruby would print
          # it. The readers are joined on every exit path below instead.
          [out_reader, err_reader].each { |thread| thread.report_on_exception = false }

          unless waiter.join(timeout)
            begin
              Process.kill("KILL", waiter.pid)
            rescue SystemCallError
              nil
            end
            waiter.join(1)
            [out_reader, err_reader].each { |thread| thread.join(1) }
            raise BinaryError,
                  "#{OVERRIDE_ENV}=#{binary}: `doctor --json` did not finish within #{timeout}s; " \
                  "refusing to use a binary that cannot describe itself."
          end

          status = waiter.value
          stdout = out_reader.value.to_s
          stderr = err_reader.value.to_s
        end

        parse_report(binary, stdout, stderr, status)
      rescue SystemCallError => e
        raise BinaryError, "#{OVERRIDE_ENV}=#{binary}: could not run `doctor --json` (#{e.message})"
      end

      def parse_report(binary, stdout, stderr, status)
        report = JSON.parse(stdout)
        raise JSON::ParserError, "not a JSON object" unless report.is_a?(Hash)

        report
      rescue JSON::ParserError => e
        detail = stderr.strip.lines.first(3).join.strip
        raise BinaryError,
              "#{OVERRIDE_ENV}=#{binary}: `doctor --json` did not print a JSON report " \
              "(#{e.message}; exit status #{status&.exitstatus.inspect}" \
              "#{detail.empty? ? "" : "; stderr: #{detail}"}). Is this an orbitcast #{VERSION} build?"
      end

      def override_path(override)
        expanded = File.expand_path(override)

        unless File.file?(expanded)
          raise BinaryNotFoundError,
                "#{OVERRIDE_ENV}=#{override} does not point at a file " \
                "(resolved to #{expanded}). Set it to an orbitcast #{VERSION} executable, " \
                "or unset it to use the binary vendored in this gem."
        end

        unless File.executable?(expanded)
          raise BinaryNotFoundError,
                "#{OVERRIDE_ENV}=#{override} is not executable (resolved to #{expanded}). " \
                "Run: chmod +x #{expanded}"
        end

        expanded
      end

      def vendored_path
        candidate = vendored_binary

        unless File.file?(candidate)
          raise BinaryNotFoundError, missing_vendored_message(candidate)
        end

        unless File.executable?(candidate)
          raise BinaryNotFoundError,
                "vendored orbitcast binary at #{candidate} is not executable. " \
                "Reinstall the gem, or run: chmod +x #{candidate}"
        end

        candidate
      end

      def missing_vendored_message(candidate)
        <<~MESSAGE.strip
          No orbitcast executable for platform #{gem_platform} (expected #{candidate}).

          This gem installation has no binary for your platform. Either:
            1. install the platform gem for #{gem_platform}, e.g.
               bundle lock --add-platform #{gem_platform}
            2. build orbitcast #{VERSION} from source and point at it:
               cargo build --release --features postgres
               export #{OVERRIDE_ENV}=/path/to/target/release/#{EXECUTABLE}

          Orbitcast never downloads or compiles a binary for you.
        MESSAGE
      end
    end
  end
end
