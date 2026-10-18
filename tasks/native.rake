# frozen_string_literal: true

# Native gem packaging.
#
# One gem name + version per platform; each platform gem vendors exactly one
# executable under vendor/<gem-platform>/bin/orbitcast plus a manifest.json.
# `Cargo.toml` is the version authority; release-please keeps
# gem/lib/orbitcast/version.rb in lockstep. Release verification lives in
# tasks/release.rake; this file is build / package / smoke.
#
# These tasks must load (`rake -T`) with plain system ruby + rake: no Bundler,
# no Rails, no cargo. Everything expensive happens inside task bodies.

require "fileutils"
require "json"
require "tmpdir"

module OrbitcastRelease
  ROOT = File.expand_path("..", __dir__)
  GEM_DIR = File.join(ROOT, "gem")
  GEMSPEC = File.join(GEM_DIR, "orbitcast.gemspec")
  PKG_DIR = File.join(ROOT, "pkg")
  TOOLCHAIN_FILE = File.join(ROOT, "rust-toolchain.toml")
  SMOKE_SCRIPT = File.join(ROOT, "tasks", "smoke", "run.rb")

  # Platform table. GNU labels are spelled out because
  # RubyGems treats a bare `*-linux` as a libc wildcard that can match musl.
  PLATFORM_TARGETS = {
    "arm64-darwin" => "aarch64-apple-darwin",
    "x86_64-darwin" => "x86_64-apple-darwin",
    "x86_64-linux-gnu" => "x86_64-unknown-linux-gnu",
    "aarch64-linux-gnu" => "aarch64-unknown-linux-gnu",
    "x86_64-linux-musl" => "x86_64-unknown-linux-musl"
  }.freeze

  # Features baked into every released binary: postgres is the default
  # backend; the memory backend is a development build, never shipped.
  FEATURES = %w[postgres].freeze

  # Wire envelope versions this binary speaks. Recorded in
  # every manifest so a rolling deploy can tell what a node will accept
  # without starting it. Bump here when src/protocol gains a version.
  ENVELOPE_VERSIONS = [1].freeze

  # glibc floor for the *-linux-gnu gems. 2.28 (RHEL 8) was the original
  # target; 2.34 is the
  # lowest this dependency graph links against: rama-dns 0.4
  # binds the unprefixed `res_nsearch` from libresolv, which glibc only
  # exports from 2.34 (older releases export `__res_nsearch` alone), so a
  # 2.28..2.33 link fails with "undefined symbol: res_nsearch" — verified
  # with cargo-zigbuild. RHEL 9 / Ubuntu 22.04 / Debian 12 / Amazon Linux
  # 2023 all ship >= 2.34; RHEL 8 / Ubuntu 20.04 / Debian 11 need the musl
  # gem. Built with cargo-zigbuild (`native:build[<target>,zigbuild]`) so the
  # floor is real: a native build on a GitHub runner links the host's glibc.
  LINUX_GLIBC_VERSION = ENV.fetch("ORBITCAST_GLIBC_VERSION", "2.34")

  # Minimum macOS for the *-darwin gems (macOS 13). Exported
  # as MACOSX_DEPLOYMENT_TARGET for Darwin builds and asserted by the smoke
  # test against the binary's LC_BUILD_VERSION.
  MACOS_DEPLOYMENT_TARGET = ENV.fetch("ORBITCAST_MACOS_DEPLOYMENT_TARGET", "13.0")

  module_function

  # Version from Cargo.toml [package] — the release authority.
  def crate_version
    manifest = File.read(File.join(ROOT, "Cargo.toml"))
    package = manifest[/\[package\](.*?)(?=^\[|\z)/m, 1] or
      abort "Cargo.toml has no [package] section"
    package[/^version\s*=\s*"([^"]+)"/, 1] or
      abort "Cargo.toml [package] has no version"
  end

  # Version constant from gem/lib/orbitcast/version.rb. Parsed, not required,
  # so verification never executes gem code.
  def gem_version
    source = File.read(File.join(GEM_DIR, "lib", "orbitcast", "version.rb"))
    source[/VERSION\s*=\s*"([^"]+)"/, 1] or
      abort "gem/lib/orbitcast/version.rb has no VERSION constant"
  end

  # Pinned channel from rust-toolchain.toml (what CI and release.yml install).
  def rust_toolchain
    abort "missing #{TOOLCHAIN_FILE}" unless File.file?(TOOLCHAIN_FILE)
    File.read(TOOLCHAIN_FILE)[/^channel\s*=\s*"([^"]+)"/, 1] or
      abort "#{TOOLCHAIN_FILE} has no [toolchain] channel"
  end

  def target_for(platform)
    PLATFORM_TARGETS.fetch(platform) do
      abort "unknown gem platform #{platform.inspect}; " \
            "supported: #{PLATFORM_TARGETS.keys.join(", ")}"
    end
  end

  def darwin?(target) = target.end_with?("-apple-darwin")
  def linux_gnu?(target) = target.end_with?("-linux-gnu")
  def linux_musl?(target) = target.end_with?("-linux-musl")

  def commit
    out = `git -C #{ROOT} rev-parse HEAD`.strip
    abort "git rev-parse HEAD failed" unless $?.success? && !out.empty?
    out
  end

  # Cargo's target directory. This crate is now its own workspace root, so the
  # output lands in ./target — but ask cargo instead of hardcoding it, so an
  # overridden CARGO_TARGET_DIR or a future vendored checkout still resolves.
  def cargo_target_dir
    @cargo_target_dir ||= begin
      out = IO.popen(
        %w[cargo metadata --format-version=1 --no-deps --offline],
        chdir: ROOT, err: File::NULL, &:read
      )
      dir = $?.success? ? JSON.parse(out)["target_directory"] : nil
      dir || File.join(ROOT, "target")
    end
  end

  def release_binary(target)
    File.join(cargo_target_dir, target, "release", "orbitcast")
  end

  def gem_file(platform)
    File.join(PKG_DIR, "orbitcast-#{crate_version}-#{platform}.gem")
  end

  # Gem platform label of the machine we are running on, using the gem's own
  # resolution logic (gem/lib/orbitcast/binary.rb is dependency-free).
  def host_platform
    require File.join(GEM_DIR, "lib", "orbitcast", "binary")
    Orbitcast::Binary.gem_platform
  end

  # vendor/<platform>/manifest.json: what release:verify checks, what the
  # smoke test asserts the binary against, and what `orbitcast doctor` and
  # the launcher report. Additive only — tooling reads named keys.
  def manifest_for(platform, target)
    manifest = {
      version: crate_version,
      target: target,
      features: FEATURES,
      envelope_versions: ENVELOPE_VERSIONS,
      rust_toolchain: rust_toolchain,
      commit: commit
    }
    manifest[:glibc_floor] = LINUX_GLIBC_VERSION if linux_gnu?(target)
    manifest[:libc] = "musl" if linux_musl?(target)
    manifest[:macos_deployment_target] = MACOS_DEPLOYMENT_TARGET if darwin?(target)
    manifest[:gem_platform] = platform
    manifest
  end

  def ensure_lockstep!
    return if crate_version == gem_version

    abort "version drift: Cargo.toml=#{crate_version} gem/lib/orbitcast/version.rb=#{gem_version} " \
          "(release-please keeps these in lockstep; do not hand-edit one side)"
  end

  # Build pkg/<gem> for `gem_platform` from an isolated staging tree: never
  # the working checkout, so stray vendor/ leftovers or other platforms'
  # binaries cannot leak into the gem. The block adds files to the tree and
  # returns their paths.
  def build_gem(gem_platform)
    require "rubygems/package"

    Dir.mktmpdir("orbitcast-package-") do |staging|
      base_spec = Gem::Specification.load(GEMSPEC) or abort "failed to load #{GEMSPEC}"

      FileUtils.cp(GEMSPEC, File.join(staging, "orbitcast.gemspec"))
      base_spec.files.each do |rel|
        dest = File.join(staging, rel)
        FileUtils.mkdir_p(File.dirname(dest))
        FileUtils.cp(File.join(GEM_DIR, rel), dest)
      end

      # Reuse the already-loaded spec rather than evaluating the staged
      # gemspec a second time (which re-requires version.rb from another
      # path and warns about a redefined constant). Files are relative, so
      # building from the staging directory picks up the added tree.
      spec = base_spec.dup
      spec.files += block_given? ? yield(staging) : []
      spec.platform = gem_platform

      Dir.chdir(staging) do
        file_name = Gem::Package.build(spec)
        FileUtils.mkdir_p(PKG_DIR)
        FileUtils.mv(file_name, File.join(PKG_DIR, file_name))
        file_name
      end
    end
  end
end

namespace :native do
  desc "Build a release binary for a Rust target (optional builder: build or zigbuild; zigbuild pins the glibc floor for *-linux-gnu)"
  task :build, [:target, :builder] do |_t, args|
    target = args[:target] or
      abort "usage: rake 'native:build[<rust-target>[,build|zigbuild]]' " \
            "(targets: #{OrbitcastRelease::PLATFORM_TARGETS.values.join(", ")})"

    unless OrbitcastRelease::PLATFORM_TARGETS.value?(target)
      warn "note: #{target} is not in the release platform table (building anyway)"
    end

    builder = args[:builder] || "build"
    abort "unknown builder #{builder.inspect}; use build or zigbuild" unless %w[build zigbuild].include?(builder)

    build_target = target
    env = {}

    if OrbitcastRelease.linux_gnu?(target)
      if builder == "zigbuild"
        build_target = "#{target}.#{OrbitcastRelease::LINUX_GLIBC_VERSION}"
      else
        warn "note: native `cargo build` for #{target} links the host glibc, not the " \
             "#{OrbitcastRelease::LINUX_GLIBC_VERSION} floor; release builds use " \
             "native:build[#{target},zigbuild]"
      end
    end

    if OrbitcastRelease.darwin?(target)
      # The deployment target is a build input, not a runner property: the
      # smoke test asserts LC_BUILD_VERSION minos against the manifest.
      env["MACOSX_DEPLOYMENT_TARGET"] = OrbitcastRelease::MACOS_DEPLOYMENT_TARGET
    end

    sh env, "cargo", builder, "--locked", "--release",
       "--target", build_target,
       "--features", OrbitcastRelease::FEATURES.join(","),
       chdir: OrbitcastRelease::ROOT
  end

  desc "Stage gem tree + vendored binary and build the platform .gem into pkg/ (e.g. native:package[arm64-darwin])"
  task :package, [:platform] do |_t, args|
    require "rubygems/package"

    platform = args[:platform] or
      abort "usage: rake 'native:package[<gem-platform>]' " \
            "(platforms: #{OrbitcastRelease::PLATFORM_TARGETS.keys.join(", ")})"
    target = OrbitcastRelease.target_for(platform)
    OrbitcastRelease.ensure_lockstep!

    binary = OrbitcastRelease.release_binary(target)
    unless File.file?(binary)
      abort "no release binary at #{binary}; run: rake 'native:build[#{target}]'"
    end

    # Guard against a RubyGems old enough to collapse `x86_64-linux-gnu` into
    # the `x86_64-linux` libc wildcard (which would also match musl).
    gem_platform = Gem::Platform.new(platform)
    unless gem_platform.to_s == platform
      abort "Gem::Platform.new(#{platform.inspect}).to_s == #{gem_platform.to_s.inspect}; " \
            "this RubyGems (#{Gem::VERSION}) does not preserve explicit libc labels — upgrade RubyGems"
    end

    manifest = OrbitcastRelease.manifest_for(platform, target)

    built = OrbitcastRelease.build_gem(gem_platform) do |staging|
      vendor_bin = File.join("vendor", platform, "bin", "orbitcast")
      vendor_manifest = File.join("vendor", platform, "manifest.json")
      FileUtils.mkdir_p(File.join(staging, "vendor", platform, "bin"))
      FileUtils.cp(binary, File.join(staging, vendor_bin))
      FileUtils.chmod(0o755, File.join(staging, vendor_bin))
      File.write(File.join(staging, vendor_manifest), JSON.pretty_generate(manifest) + "\n")
      [vendor_bin, vendor_manifest]
    end

    puts "built pkg/#{built} (manifest: #{manifest.reject { |k, _| k == :commit }.to_json})"
  end

  # Lockfiles that list the `ruby` platform (or a `force_ruby_platform`
  # bundle) resolve only if this gem exists; it runs with ORBITCAST_BINARY.
  desc "Build the ruby-platform gem into pkg/: the Ruby code, no vendored binary"
  task :stub do
    OrbitcastRelease.ensure_lockstep!
    built = OrbitcastRelease.build_gem(Gem::Platform::RUBY)
    puts "built pkg/#{built} (no vendored binary)"
  end

  desc "Offline install of the built platform gem + binary inspection + --version/doctor + real WebSocket/PostgreSQL round trip when SMOKE_DATABASE_URL is set (e.g. native:smoke[arm64-darwin])"
  task :smoke, [:platform] do |_t, args|
    platform = args[:platform] or
      abort "usage: rake 'native:smoke[<gem-platform>]'"
    target = OrbitcastRelease.target_for(platform)

    host = OrbitcastRelease.host_platform
    unless host == platform
      abort "native:smoke[#{platform}] must run on a matching host (this host is #{host}); " \
            "release.yml runs the musl smoke inside an Alpine container (tasks/smoke/run.rb)"
    end

    gem_file = OrbitcastRelease.gem_file(platform)
    abort "no built gem at #{gem_file}; run: rake 'native:package[#{platform}]'" unless File.file?(gem_file)

    # Everything happens in tasks/smoke/run.rb (plain Ruby, stdlib + pg) so
    # the exact same script runs inside the Alpine container in release.yml.
    ruby OrbitcastRelease::SMOKE_SCRIPT,
         "--gem", gem_file,
         "--platform", platform,
         "--target", target,
         "--expect-version", OrbitcastRelease.crate_version
  end

  desc "Build and package every supported platform gem, then verify (alias of release:package)"
  task all: "release:package"
end

namespace :platforms do
  desc "Build every platform gem (macOS SDK for Darwin, cargo-zigbuild and Zig for Linux) — alias of release:package"
  task build: "release:package"
end
