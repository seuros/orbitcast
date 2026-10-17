#!/usr/bin/env ruby
# frozen_string_literal: true

# Native-gem smoke test.
#
# Proves, for ONE packaged platform gem on a matching host:
#
#   1. it installs offline (`gem install --local --ignore-dependencies`);
#   2. optionally, that a gem for a *foreign* platform is refused on this host
#      (`--refuse-gem`: a missing musl artifact must never select a GNU binary);
#   3. the vendored manifest matches the expected version/target and carries
#      envelope versions, toolchain, and the platform floor;
#   4. the binary respects that floor and links no host OpenSSL:
#        linux-gnu  max GLIBC_x.y symbol version <= glibc_floor, ldd clean
#        linux-musl static, no GLIBC_ symbols, no dynamic loader
#        darwin     otool -L shows only /usr/lib + /System frameworks,
#                   LC_BUILD_VERSION minos == macos_deployment_target
#   5. `orbitcast --version` through the gem wrapper reports the version;
#   6. `orbitcast doctor --json` reports version/target/features/envelope_versions
#      that agree with the manifest (tolerated as TODO until the subcommand
#      ships; SMOKE_REQUIRE_DOCTOR=1 makes it mandatory);
#   7. with SMOKE_DATABASE_URL: the relay starts, /readyz turns 200, a real
#      WebSocket client subscribes with a Turbo-signed stream name, a
#      `pg_notify` publication in the v1 inline envelope arrives as an
#      ActionCable frame, and SIGTERM exits 0 within the shutdown timeout;
#   8. without SMOKE_DATABASE_URL: the relay exits non-zero quickly with a
#      "database" error — invariant 7, no false readiness.
#
# Plain Ruby: stdlib plus the `pg` gem for step 7. The same script runs from
# `rake native:smoke[<platform>]` and inside the Alpine container in
# release.yml, so nothing here may depend on rake, Bundler, cargo, or git.
#
#   ruby tasks/smoke/run.rb --gem pkg/orbitcast-0.2.0-arm64-darwin.gem \
#        --platform arm64-darwin --target aarch64-apple-darwin --expect-version 0.2.0 \
#        [--refuse-gem pkg/orbitcast-0.2.0-x86_64-linux-gnu.gem]
#
# Environment: SMOKE_DATABASE_URL, SMOKE_LISTEN_ADDR (127.0.0.1:28090),
# SMOKE_SHUTDOWN_TIMEOUT_MS (5000), SMOKE_REQUIRE_DOCTOR (0/1).

require "base64"
require "digest"
require "fileutils"
require "json"
require "net/http"
require "openssl"
require "optparse"
require "securerandom"
require "tmpdir"

require_relative "ws_client"

module OrbitcastSmoke
  class Failure < StandardError; end

  # Turbo::StreamsChannel.signed_stream_name, byte for byte:
  # base64_standard(JSON(stream)) + "--" + hex(HMAC-SHA256(key, encoded)).
  # Pinned to the real turbo-rails vector in
  # tests/fixtures/signed_streams/turbo/turbo_real.json so the signer cannot
  # drift from the format the relay verifies.
  module Turbo
    FIXTURE_KEY_BASE64 = "AP8QgHR1cmJvLWZpeHR1cmUta2V5LTMyLWJ5dGVzLTA="
    FIXTURE_STREAM = "demo"
    FIXTURE_SIGNED = "ImRlbW8i--7023077ee18639e8ab5c492f6a02840dcb8bd4d19db905330c9f79fe9fb2a8e0"

    module_function

    def signed_stream_name(stream, key)
      encoded = Base64.strict_encode64(JSON.generate(stream))
      "#{encoded}--#{OpenSSL::HMAC.hexdigest("SHA256", key, encoded)}"
    end

    def self_check!
      got = signed_stream_name(FIXTURE_STREAM, Base64.strict_decode64(FIXTURE_KEY_BASE64))
      return if got == FIXTURE_SIGNED

      raise Failure, "Turbo signer self-check failed: #{got} != fixture #{FIXTURE_SIGNED}"
    end
  end

  # Channel derivation, pinned to its golden vector.
  module Channel
    VECTOR = ["example_production", "chat:42", "orbitcast_75ba535bd0c12e8f0990bf756607b4f5ddb148f2e22070e1"].freeze

    module_function

    def for(namespace, stream)
      ns = namespace.b
      st = stream.b
      digest = Digest::SHA256.hexdigest([ns.bytesize].pack("N") + ns + [st.bytesize].pack("N") + st)
      "orbitcast_#{digest[0, 48]}"
    end

    def self_check!
      namespace, stream, expected = VECTOR
      got = self.for(namespace, stream)
      raise Failure, "channel derivation self-check failed: #{got} != #{expected}" unless got == expected
    end
  end

  class Runner
    SUBPROTOCOL = "actioncable-v1-json"
    FORBIDDEN_DARWIN = %w[/opt/homebrew /usr/local/opt /usr/local/lib /usr/local/Cellar libssl libcrypto @rpath @executable_path].freeze

    def initialize(options)
      @gem = File.expand_path(options.fetch(:gem))
      @platform = options.fetch(:platform)
      @target = options.fetch(:target)
      @expect_version = options.fetch(:expect_version)
      @refuse_gems = options.fetch(:refuse_gems, []).map { |g| File.expand_path(g) }
      @listen_addr = ENV.fetch("SMOKE_LISTEN_ADDR", "127.0.0.1:28090")
      @database_url = ENV["SMOKE_DATABASE_URL"].to_s.empty? ? nil : ENV["SMOKE_DATABASE_URL"]
      @shutdown_ms = Integer(ENV.fetch("SMOKE_SHUTDOWN_TIMEOUT_MS", "5000"))
      @require_doctor = ENV["SMOKE_REQUIRE_DOCTOR"] == "1"
      @gem_home = nil
      @relay_pid = nil
      @relay_log = nil
    end

    def run
      raise Failure, "no such gem file: #{@gem}" unless File.file?(@gem)

      Turbo.self_check!
      Channel.self_check!

      @gem_home = Dir.mktmpdir("orbitcast-smoke-")
      step("install #{File.basename(@gem)} offline into #{@gem_home}") { install! }
      @refuse_gems.each do |foreign|
        step("refuse foreign-platform gem #{File.basename(foreign)}") { refuse!(foreign) }
      end
      step("manifest") { check_manifest! }
      step("binary inspection (#{os_family})") { inspect_binary! }
      step("orbitcast --version") { check_version! }
      step("orbitcast doctor --json") { check_doctor! }

      if @database_url
        step("serve + WebSocket subscribe + pg_notify + SIGTERM") { serve_round_trip! }
      else
        step("serve without a database must fail closed (invariant 7)") { serve_without_database! }
      end

      puts "smoke OK: #{@platform} (#{@target}) version #{@expect_version}" \
           "#{@database_url ? ", PostgreSQL round trip verified" : ", no-database variant"}"
    ensure
      stop_relay!
      FileUtils.rm_rf(@gem_home) if @gem_home && ENV["SMOKE_KEEP"] != "1"
    end

    private

    # ----------------------------------------------------------------- steps

    def step(title)
      puts "==> #{title}"
      yield
    end

    def gem_env
      {
        "GEM_HOME" => @gem_home,
        "GEM_PATH" => @gem_home,
        # Belt and braces: even with --local, never let a stray rc file add
        # sources or flags.
        "GEMRC" => File::NULL
      }
    end

    def install!
      # --local: no network, ever. --ignore-dependencies: this proves the
      # packaged artifact, not the Rails bundle (which is pre-provisioned
      # separately by the release workflow).
      out, status = capture(gem_env, "gem", "install", "--local", "--ignore-dependencies", "--no-document", @gem)
      puts out
      raise Failure, "gem install exited #{status.exitstatus}" unless status.success?

      dirs = Dir[File.join(@gem_home, "gems", "orbitcast-*")]
      raise Failure, "expected exactly one installed orbitcast gem, found #{dirs.inspect}" unless dirs.length == 1

      @install_dir = dirs.first
      @vendored = File.join(@install_dir, "vendor", @platform, "bin", "orbitcast")
      raise Failure, "installed gem is missing #{@vendored}" unless File.file?(@vendored)
      raise Failure, "#{@vendored} is not executable after install" unless File.executable?(@vendored)

      other = Dir[File.join(@install_dir, "vendor", "*")].map { |d| File.basename(d) } - [@platform]
      raise Failure, "gem vendors other platforms too: #{other.inspect}" unless other.empty?
    end

    def refuse!(foreign)
      raise Failure, "no such gem file: #{foreign}" unless File.file?(foreign)

      foreign_platform = File.basename(foreign, ".gem").split("-", 3).last
      out, status = capture(gem_env, "gem", "install", "--local", "--ignore-dependencies", "--no-document", foreign)
      puts out.lines.last(3).join

      if status.success?
        raise Failure, "RubyGems accepted #{File.basename(foreign)} on a #{@platform} host — " \
                       "a #{foreign_platform} binary must never be selected here"
      end

      leaked = Dir[File.join(@gem_home, "gems", "orbitcast-*", "vendor", foreign_platform)]
      raise Failure, "refused install still left #{leaked.inspect} behind" unless leaked.empty?

      dirs = Dir[File.join(@gem_home, "gems", "orbitcast-*")]
      raise Failure, "expected one installed gem after refusal, found #{dirs.inspect}" unless dirs.length == 1

      puts "refused as expected (exit #{status.exitstatus})"
    end

    def manifest
      @manifest ||= begin
        path = File.join(@install_dir, "vendor", @platform, "manifest.json")
        raise Failure, "missing #{path}" unless File.file?(path)

        JSON.parse(File.read(path))
      end
    end

    def check_manifest!
      m = manifest
      expect(m["version"] == @expect_version, "manifest version #{m["version"].inspect} != #{@expect_version}")
      expect(m["target"] == @target, "manifest target #{m["target"].inspect} != #{@target}")
      expect(Array(m["features"]).include?("postgres"), "manifest features #{m["features"].inspect} lack postgres")
      expect(m["envelope_versions"].is_a?(Array) && m["envelope_versions"].include?(1),
             "manifest envelope_versions #{m["envelope_versions"].inspect} must include 1")
      expect(m["rust_toolchain"].is_a?(String) && !m["rust_toolchain"].empty?, "manifest has no rust_toolchain")
      expect(m["commit"].to_s.match?(/\A[0-9a-f]{40}\z/), "manifest commit #{m["commit"].inspect} is not a SHA")

      case os_family
      when :linux_gnu
        expect(m["glibc_floor"].to_s.match?(/\A\d+\.\d+\z/), "linux-gnu manifest needs glibc_floor, got #{m["glibc_floor"].inspect}")
      when :linux_musl
        expect(m["libc"] == "musl", "linux-musl manifest needs libc: musl, got #{m["libc"].inspect}")
      when :darwin
        expect(m["macos_deployment_target"].to_s.match?(/\A\d+\.\d+\z/),
               "darwin manifest needs macos_deployment_target, got #{m["macos_deployment_target"].inspect}")
      end

      puts m.reject { |k, _| k == "commit" }.to_json
    end

    def os_family
      if @target.end_with?("-apple-darwin") then :darwin
      elsif @target.end_with?("-linux-gnu") then :linux_gnu
      elsif @target.end_with?("-linux-musl") then :linux_musl
      else
        raise Failure, "unknown target family: #{@target}"
      end
    end

    def inspect_binary!
      case os_family
      when :darwin then inspect_darwin!
      when :linux_gnu then inspect_linux_gnu!
      when :linux_musl then inspect_linux_musl!
      end
    end

    def inspect_darwin!
      libs, status = capture({}, "otool", "-L", @vendored)
      raise Failure, "otool -L failed: #{libs}" unless status.success?

      puts libs
      lines = libs.lines.drop(1).map(&:strip).reject(&:empty?)
      bad = lines.select do |line|
        FORBIDDEN_DARWIN.any? { |needle| line.include?(needle) } ||
          !(line.start_with?("/usr/lib/") || line.start_with?("/System/Library/"))
      end
      raise Failure, "binary links non-system libraries (Homebrew/OpenSSL?): #{bad.inspect}" unless bad.empty?

      load_cmds, status = capture({}, "otool", "-l", @vendored)
      raise Failure, "otool -l failed" unless status.success?

      block = load_cmds[/cmd LC_BUILD_VERSION.*?(?=\n\s*cmd |\z)/m] or
        raise Failure, "binary has no LC_BUILD_VERSION load command"
      minos = block[/minos\s+(\S+)/, 1]
      platform = block[/platform\s+(\S+)/, 1]
      floor = manifest["macos_deployment_target"]
      expect(platform == "1", "LC_BUILD_VERSION platform #{platform.inspect} is not macOS (1)")
      expect(Gem::Version.new(minos) == Gem::Version.new(floor),
             "LC_BUILD_VERSION minos #{minos} != manifest macos_deployment_target #{floor} " \
             "(native:build must export MACOSX_DEPLOYMENT_TARGET)")
      puts "LC_BUILD_VERSION minos #{minos} == deployment target #{floor}"
    end

    def inspect_linux_gnu!
      floor = Gem::Version.new(manifest["glibc_floor"])
      versions = glibc_symbol_versions
      highest = versions.max
      raise Failure, "no GLIBC_ symbol versions found in #{@vendored} — not a glibc binary?" if highest.nil?

      puts "GLIBC symbol versions: max #{highest} (floor #{floor}); #{versions.length} distinct"
      expect(highest <= floor, "binary needs GLIBC_#{highest} > declared floor #{floor}")

      ldd, status = capture({}, "ldd", @vendored)
      puts ldd
      raise Failure, "ldd failed (#{status.exitstatus})" unless status.success?
      expect(!ldd.match?(/libssl|libcrypto/), "binary links a host OpenSSL: #{ldd.lines.grep(/libssl|libcrypto/).join}")
      expect(!ldd.include?("not found"), "ldd reports missing libraries: #{ldd.lines.grep(/not found/).join}")
    end

    def inspect_linux_musl!
      bytes = File.binread(@vendored)
      expect(!bytes.include?("GLIBC_"), "musl binary references GLIBC_ symbol versions")
      expect(!bytes.include?("ld-linux"), "musl binary names the glibc dynamic loader")
      expect(!bytes.match?(/libssl\.so|libcrypto\.so/), "musl binary names a shared OpenSSL")

      ldd, status = capture({}, "ldd", @vendored)
      puts ldd.strip.empty? ? "ldd: (no output, exit #{status.exitstatus})" : ldd
      # musl's ldd exits non-zero for a static executable; a *dynamic* musl
      # executable would list libc.so and the shared libraries it needs.
      expect(!ldd.match?(/libssl|libcrypto/), "binary links a host OpenSSL: #{ldd}")
      expect(!ldd.include?("=>") || !ldd.match?(/libc\.so/), "musl binary is dynamically linked: #{ldd}")
    end

    # Highest glibc symbol version the binary needs: objdump -T, then readelf,
    # then a byte scan of the version-needed strings as the last resort.
    def glibc_symbol_versions
      text =
        if command?("objdump")
          capture({}, "objdump", "-T", @vendored).first
        elsif command?("readelf")
          capture({}, "readelf", "--dyn-syms", "-W", @vendored).first
        else
          warn "note: neither objdump nor readelf is installed; scanning .dynstr bytes instead"
          File.binread(@vendored)
        end

      text.b.scan(/GLIBC_(\d+\.\d+(?:\.\d+)?)/).flatten.uniq.map { |v| Gem::Version.new(v) }
    end

    def command?(name)
      ENV.fetch("PATH", "").split(File::PATH_SEPARATOR).any? { |dir| File.executable?(File.join(dir, name)) }
    end

    def check_version!
      out, status = capture(gem_env, Gem.ruby, "-I", lib_dir, exe, "--version")
      puts out
      raise Failure, "orbitcast --version exited #{status.exitstatus}" unless status.success?
      expect(out.include?(@expect_version), "--version reported #{out.strip.inspect}, expected #{@expect_version}")

      direct, status = capture({}, @vendored, "--version")
      raise Failure, "vendored binary --version exited #{status.exitstatus}: #{direct}" unless status.success?
      expect(direct.include?(@expect_version), "vendored binary reported #{direct.strip.inspect}")
    end

    def check_doctor!
      out, status = capture(gem_env, Gem.ruby, "-I", lib_dir, exe, "doctor", "--json")

      unless status.success?
        message = "`orbitcast doctor --json` exited #{status.exitstatus}: #{out.strip.lines.first.to_s.strip}"
        raise Failure, "#{message} (SMOKE_REQUIRE_DOCTOR=1)" if @require_doctor

        warn "TODO(doctor): #{message} — the doctor subcommand is not in this binary yet; " \
             "skipping its assertions (set SMOKE_REQUIRE_DOCTOR=1 to make this fatal)"
        return
      end

      report = begin
        JSON.parse(out)
      rescue JSON::ParserError => e
        raise Failure, "doctor --json did not print JSON (#{e.message}): #{out.strip[0, 200].inspect}"
      end

      %w[version target features envelope_versions].each do |key|
        expect(report.key?(key), "doctor --json lacks #{key.inspect}: #{report.keys.inspect}")
      end
      expect(report["version"] == manifest["version"], "doctor version #{report["version"].inspect} != manifest #{manifest["version"]}")
      expect(report["target"] == manifest["target"], "doctor target #{report["target"].inspect} != manifest #{manifest["target"]}")
      expect(Array(report["features"]).sort == Array(manifest["features"]).sort,
             "doctor features #{report["features"].inspect} != manifest #{manifest["features"].inspect}")
      expect(Array(report["envelope_versions"]).sort == Array(manifest["envelope_versions"]).sort,
             "doctor envelope_versions #{report["envelope_versions"].inspect} != manifest #{manifest["envelope_versions"].inspect}")
      puts "doctor agrees with manifest: #{report.slice("version", "target", "features", "envelope_versions").to_json}"
    end

    # ----------------------------------------------------------------- serve

    def relay_env(with_database:)
      key = SecureRandom.random_bytes(32)
      @turbo_key = key
      env = gem_env.merge(
        "ORBITCAST_LISTEN_ADDR" => @listen_addr,
        "ORBITCAST_NAMESPACE" => "smoke_#{SecureRandom.hex(4)}",
        "ORBITCAST_TURBO_STREAMS" => "true",
        "ORBITCAST_TURBO_SECRET_BASE64" => Base64.strict_encode64(key),
        "ORBITCAST_SHUTDOWN_TIMEOUT_MS" => @shutdown_ms.to_s,
        "ORBITCAST_LOG" => "info"
      )
      env["ORBITCAST_DATABASE_URL"] = @database_url if with_database
      env
    end

    def start_relay!(env)
      @relay_log = File.join(@gem_home, "relay.log")
      # Through the gem wrapper, exactly as an operator runs it. cwd is the
      # temp dir so no config/environment.rb can turn this into a Rails boot.
      @relay_pid = Process.spawn(
        env, Gem.ruby, "-I", lib_dir, exe, "serve",
        chdir: @gem_home, out: @relay_log, err: @relay_log, in: File::NULL
      )
    end

    def stop_relay!
      return unless @relay_pid

      pid = @relay_pid
      @relay_pid = nil
      begin
        Process.kill("KILL", pid)
        Process.wait(pid)
      rescue Errno::ESRCH, Errno::ECHILD
        nil
      end
    end

    def relay_exited?
      return true unless @relay_pid

      _, status = Process.wait2(@relay_pid, Process::WNOHANG)
      return false if status.nil?

      @relay_status = status
      @relay_pid = nil
      true
    end

    def relay_log_tail(lines = 40)
      return "(no log)" unless @relay_log && File.file?(@relay_log)

      File.read(@relay_log).lines.last(lines).join
    end

    def http_get(path, timeout: 2)
      host, port = @listen_addr.split(":")
      Net::HTTP.start(host, Integer(port), open_timeout: timeout, read_timeout: timeout) do |http|
        http.get(path)
      end
    rescue SystemCallError, IOError, Net::OpenTimeout, Net::ReadTimeout
      nil
    end

    def wait_for_ready!(timeout: 30)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        if relay_exited?
          raise Failure, "relay exited (#{@relay_status.inspect}) before /readyz turned 200; log:\n#{relay_log_tail}"
        end

        response = http_get("/readyz")
        return if response && response.code == "200"

        sleep 0.2
      end
      raise Failure, "/readyz did not turn 200 within #{timeout}s; log:\n#{relay_log_tail}"
    end

    def serve_round_trip!
      begin
        require "pg"
      rescue LoadError => e
        raise Failure, "SMOKE_DATABASE_URL is set but the pg gem is not installed (#{e.message}); " \
                       "pre-provision it before disabling the network"
      end

      env = relay_env(with_database: true)
      namespace = env["ORBITCAST_NAMESPACE"]
      stream = "smoke:#{SecureRandom.hex(4)}"
      start_relay!(env)
      wait_for_ready!
      healthz = http_get("/healthz")
      expect(healthz && healthz.code == "200", "/healthz did not answer 200")
      puts "relay pid #{@relay_pid} ready on #{@listen_addr} (namespace #{namespace})"

      signed = Turbo.signed_stream_name(stream, @turbo_key)
      identifier = JSON.generate("channel" => "Turbo::StreamsChannel", "signed_stream_name" => signed)

      socket = WebSocket.new("ws://#{@listen_addr}/cable", subprotocol: SUBPROTOCOL)
      expect(socket.subprotocol == SUBPROTOCOL, "negotiated subprotocol #{socket.subprotocol.inspect}")

      welcome = next_frame(socket, "welcome")
      expect(welcome["type"] == "welcome", "first frame was not welcome: #{welcome.inspect}")

      socket.send_text(JSON.generate("command" => "subscribe", "identifier" => identifier))
      confirm = next_frame(socket, "confirm_subscription")
      expect(confirm["type"] == "confirm_subscription", "subscription not confirmed: #{confirm.inspect}")
      expect(confirm["identifier"] == identifier, "confirmation carries a different identifier: #{confirm["identifier"].inspect}")
      puts "subscribed to #{stream} with a Turbo-signed name"

      payload = JSON.generate("smoke" => SecureRandom.uuid, "html" => "<turbo-stream action=\"append\"></turbo-stream>")
      notice = JSON.generate(
        "version" => 1,
        "kind" => "inline",
        "publication" => {
          "version" => 1,
          "namespace" => namespace,
          "id" => SecureRandom.uuid,
          "stream" => stream,
          "json_payload" => payload
        }
      )
      channel = Channel.for(namespace, stream)
      conn = PG.connect(@database_url)
      begin
        conn.exec_params("SELECT pg_notify($1, $2)", [channel, notice])
      ensure
        conn.close
      end
      puts "pg_notify(#{channel}) sent (#{notice.bytesize} bytes)"

      frame = next_frame(socket, "message")
      expect(frame["identifier"] == identifier, "delivered frame has a different identifier: #{frame.inspect}")
      expect(frame["message"] == JSON.parse(payload), "delivered message differs: #{frame["message"].inspect}")
      puts "publication delivered to the subscriber"

      Process.kill("TERM", @relay_pid)
      # The drain must tell the client to go away before the socket closes.
      disconnect = next_frame(socket, "disconnect", timeout: @shutdown_ms / 1000.0 + 5, allow_closed: true)
      if disconnect.is_a?(Hash) && disconnect["type"] == "disconnect"
        expect(disconnect["reconnect"] == true, "disconnect frame did not ask for reconnect: #{disconnect.inspect}")
        puts "drain sent disconnect(reconnect: true, reason: #{disconnect["reason"].inspect})"
      else
        warn "note: no disconnect frame observed before the socket closed (#{disconnect.inspect})"
      end
      socket.close

      wait_for_exit!(@shutdown_ms / 1000.0 + 10)
      expect(@relay_status.exitstatus == 0,
             "relay exited #{@relay_status.inspect} after SIGTERM (expected 0); log:\n#{relay_log_tail}")
      puts "SIGTERM: clean exit 0 within the shutdown timeout"
    ensure
      socket&.close
    end

    def serve_without_database!
      env = relay_env(with_database: false)
      start_relay!(env)

      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
      until relay_exited?
        if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
          ready = http_get("/readyz")
          stop_relay!
          raise Failure, "relay stayed up for 15s without a database (/readyz #{ready&.code.inspect}); " \
                         "invariant 7: PostgreSQL startup failure must be fatal, not an unready idle. Log:\n#{relay_log_tail}"
        end
        sleep 0.2
      end

      log = relay_log_tail
      expect(!@relay_status.success?, "relay exited 0 without a database; expected a fatal error. Log:\n#{log}")
      expect(log.match?(/database/i), "relay exit #{@relay_status.exitstatus} but the log does not mention the database:\n#{log}")
      puts "relay refused to start without a database (exit #{@relay_status.exitstatus}): " \
           "#{log.lines.grep(/database/i).last.to_s.strip[0, 200]}"
    end

    def wait_for_exit!(timeout)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      until relay_exited?
        if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
          stop_relay!
          raise Failure, "relay did not exit within #{timeout}s of SIGTERM (killed); log:\n#{relay_log_tail}"
        end
        sleep 0.1
      end
    end

    # Next ActionCable frame that is not a ping; raises on timeout. Returns
    # :closed (when allowed) if the server closed first.
    def next_frame(socket, expected, timeout: 10, allow_closed: false)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      loop do
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        raise Failure, "timed out waiting for #{expected}; log:\n#{relay_log_tail}" if remaining <= 0

        text = socket.read_text(timeout: remaining)
        raise Failure, "timed out waiting for #{expected}; log:\n#{relay_log_tail}" if text.nil?

        if text == :closed
          return :closed if allow_closed

          raise Failure, "socket closed while waiting for #{expected}; log:\n#{relay_log_tail}"
        end

        frame = JSON.parse(text)
        next if frame["type"] == "ping"
        raise Failure, "subscription rejected: #{frame.inspect}" if frame["type"] == "reject_subscription"

        return frame
      end
    end

    # --------------------------------------------------------------- helpers

    def exe = File.join(@install_dir, "exe", "orbitcast")
    def lib_dir = File.join(@install_dir, "lib")

    def capture(env, *command)
      out = IO.popen(env, command, err: %i[child out], &:read)
      [out.to_s, $?]
    end

    def expect(condition, message)
      raise Failure, message unless condition
    end
  end
end

if $PROGRAM_NAME == __FILE__
  options = { refuse_gems: [] }
  OptionParser.new do |opts|
    opts.banner = "usage: ruby tasks/smoke/run.rb --gem FILE --platform P --target T --expect-version V [--refuse-gem FILE]..."
    opts.on("--gem FILE", "packaged platform gem to smoke") { |v| options[:gem] = v }
    opts.on("--platform PLATFORM", "gem platform label (e.g. x86_64-linux-musl)") { |v| options[:platform] = v }
    opts.on("--target TARGET", "Rust target triple the gem must vendor") { |v| options[:target] = v }
    opts.on("--expect-version VERSION", "version every surface must report") { |v| options[:expect_version] = v }
    opts.on("--refuse-gem FILE", "gem for another platform that must be refused here") { |v| options[:refuse_gems] << v }
  end.parse!(ARGV)

  missing = %i[gem platform target expect_version].reject { |k| options[k] }
  abort "missing: #{missing.map { |k| "--#{k.to_s.tr("_", "-")}" }.join(", ")}" unless missing.empty?

  begin
    OrbitcastSmoke::Runner.new(options).run
  rescue OrbitcastSmoke::Failure, OrbitcastSmoke::WebSocketError => e
    abort "smoke FAILED: #{e.message}"
  end
end
