# frozen_string_literal: true

# Release tasks: crate, gem, and tag versions move in lockstep.
#
#   release:verify   crate version == gem version (== TAG), every pkg/*.gem
#                    vendors exactly one matching platform binary with a
#                    complete manifest, all gems share one version, and — when
#                    TAG is set (the publish job) — all five platforms exist.
#   release:package  build + package every platform, then verify. Needs a
#                    macOS host with the Apple SDK for the Darwin targets and
#                    cargo-zigbuild + Zig for the Linux targets.
#   gems:fetch       download the gems release.yml attached to a GitHub
#                    release into pkg/ — CI never pushes to rubygems.org.
#   gems:push        fetch, then `gem push` each one by hand.
#
# Loads with plain system ruby + rake; tasks/native.rake defines
# OrbitcastRelease and is imported alongside this file.

require "json"
require "tmpdir"

namespace :release do
  desc "Verify version lockstep, manifests, and one binary per pkg/*.gem (TAG=vX.Y.Z also requires the complete platform matrix)"
  task :verify do
    require "rubygems/package"

    errors = []
    crate = OrbitcastRelease.crate_version
    gem_v = OrbitcastRelease.gem_version
    toolchain = OrbitcastRelease.rust_toolchain
    tag = ENV["TAG"]
    require_complete = tag || ENV["RELEASE_VERIFY_COMPLETE"] == "1"

    errors << "Cargo.toml version #{crate} != gem version #{gem_v}" unless crate == gem_v
    errors << "TAG=#{tag} does not match crate version (expected v#{crate})" if tag && tag != "v#{crate}"

    gems = Dir[File.join(OrbitcastRelease::PKG_DIR, "*.gem")].sort
    seen_platforms = []
    seen_versions = []

    gems.each do |path|
      name = File.basename(path)
      package = Gem::Package.new(path)
      spec = package.spec
      seen_versions << spec.version.to_s

      errors << "#{name}: gem version #{spec.version} != crate version #{crate}" unless spec.version.to_s == crate

      Dir.mktmpdir("orbitcast-verify-") do |dir|
        package.extract_files(dir)
        binaries = Dir[File.join(dir, "vendor", "*", "bin", "orbitcast")]

        unless binaries.length == 1
          found = binaries.map { |b| b.delete_prefix("#{dir}/") }
          errors << "#{name}: expected exactly one vendored binary, found #{binaries.length} (#{found.join(", ")})"
          next
        end

        unless File.executable?(binaries.first)
          errors << "#{name}: vendored binary is not executable (mode #{File.stat(binaries.first).mode.to_s(8)})"
        end

        platform_dir = File.basename(File.dirname(File.dirname(binaries.first)))
        seen_platforms << platform_dir

        unless OrbitcastRelease::PLATFORM_TARGETS.key?(platform_dir)
          errors << "#{name}: vendor platform #{platform_dir} is not in the release table"
        end
        unless spec.platform.to_s == platform_dir
          errors << "#{name}: spec.platform #{spec.platform} != vendor directory #{platform_dir}"
        end

        manifest_path = File.join(dir, "vendor", platform_dir, "manifest.json")
        unless File.file?(manifest_path)
          errors << "#{name}: missing vendor/#{platform_dir}/manifest.json"
          next
        end

        manifest = begin
          JSON.parse(File.read(manifest_path))
        rescue JSON::ParserError => e
          errors << "#{name}: manifest.json is not valid JSON (#{e.message})"
          next
        end

        expected_target = OrbitcastRelease::PLATFORM_TARGETS[platform_dir]
        errors.concat(
          OrbitcastReleaseVerify.manifest_errors(manifest, name: name, crate: crate, target: expected_target,
                                                       toolchain: toolchain, platform: platform_dir)
        )
      end
    rescue StandardError => e
      errors << "#{name}: unreadable gem package (#{e.class}: #{e.message})"
    end

    distinct = seen_versions.uniq
    errors << "pkg/ mixes gem versions: #{distinct.join(", ")}" if distinct.length > 1

    missing = OrbitcastRelease::PLATFORM_TARGETS.keys - seen_platforms
    if require_complete && !missing.empty?
      errors << "pkg/ is missing platform gems: #{missing.join(", ")} " \
                "(the complete matrix must build before any gem is published)"
    end

    dupes = seen_platforms.tally.select { |_, count| count > 1 }.keys
    errors << "duplicate platform gems in pkg/: #{dupes.join(", ")}" unless dupes.empty?

    abort errors.map { |e| "release:verify: #{e}" }.join("\n") unless errors.empty?

    puts "release:verify OK: version #{crate}#{" (tag #{tag})" if tag}, toolchain #{toolchain}, " \
         "#{gems.length} gem(s)#{": #{seen_platforms.sort.join(", ")}" unless seen_platforms.empty?}"
    puts "note: pkg/ is empty — only version lockstep was checked" if gems.empty?
    puts "note: platforms not in pkg/: #{missing.join(", ")} (set TAG or RELEASE_VERIFY_COMPLETE=1 to require them)" if !missing.empty? && !require_complete
  end

  desc "Build, package, and verify every supported platform gem (Darwin natively, Linux via cargo-zigbuild)"
  task :package do
    OrbitcastRelease::PLATFORM_TARGETS.each do |platform, target|
      sh "rustup", "target", "add", target
      builder = target.include?("-linux-") ? "zigbuild" : "build"
      Rake::Task["native:build"].reenable
      Rake::Task["native:build"].invoke(target, builder)
      Rake::Task["native:package"].reenable
      Rake::Task["native:package"].invoke(platform)
    end
    ENV["RELEASE_VERIFY_COMPLETE"] = "1"
    Rake::Task["release:verify"].reenable
    Rake::Task["release:verify"].invoke
  end
end

# release.yml builds, smoke-tests, verifies, and attaches every platform gem
# to the GitHub release — it never pushes to rubygems.org. Publishing is a
# deliberate, by-hand step: download what CI built, then push it yourself.
namespace :gems do
  release_version = ->(tag) { (tag || "v#{OrbitcastRelease.crate_version}").delete_prefix("v") }

  desc "Download the gems CI attached to a GitHub release into pkg/ (default tag: v<version>)"
  task :fetch, [:tag] do |_task, args|
    version = release_version.call(args[:tag])
    sh "gh", "release", "download", "v#{version}", "--repo", "seuros/orbitcast",
       "--pattern", "orbitcast-#{version}-*.gem", "--dir", OrbitcastRelease::PKG_DIR, "--clobber"
  end

  desc "Fetch a release's gems, then push each to rubygems.org"
  task :push, [:tag] => :fetch do |_task, args|
    version = release_version.call(args[:tag])
    gems = Dir[File.join(OrbitcastRelease::PKG_DIR, "orbitcast-#{version}-*.gem")]
    abort "no gems in pkg/ for this release" if gems.empty?

    failed = gems.reject { |gem| system("gem", "push", gem) }
    abort "not pushed: #{failed.join(", ")}" unless failed.empty?
  end
end

# Manifest validation shared by release:verify; kept as a pure function so it
# can be exercised without building a gem.
module OrbitcastReleaseVerify
  module_function

  def manifest_errors(manifest, name:, crate:, target:, toolchain:, platform:)
    errors = []
    errors << "#{name}: manifest version #{manifest["version"].inspect} != crate version #{crate}" unless manifest["version"] == crate
    errors << "#{name}: manifest target #{manifest["target"].inspect} != #{target}" if target && manifest["target"] != target

    features = manifest["features"]
    unless features.is_a?(Array) && features.sort == OrbitcastRelease::FEATURES.sort
      errors << "#{name}: manifest features #{features.inspect} != #{OrbitcastRelease::FEATURES.inspect}"
    end

    envelopes = manifest["envelope_versions"]
    unless envelopes.is_a?(Array) && !envelopes.empty? && envelopes.all?(Integer) && envelopes.include?(1)
      errors << "#{name}: manifest envelope_versions #{envelopes.inspect} must be a non-empty integer list including 1"
    end

    unless manifest["rust_toolchain"] == toolchain
      errors << "#{name}: manifest rust_toolchain #{manifest["rust_toolchain"].inspect} != rust-toolchain.toml channel #{toolchain}"
    end

    commit = manifest["commit"]
    errors << "#{name}: manifest commit #{commit.inspect} is not a full SHA" unless commit.is_a?(String) && commit.match?(/\A[0-9a-f]{40}\z/)

    if target && OrbitcastRelease.linux_gnu?(target)
      floor = manifest["glibc_floor"]
      unless floor.is_a?(String) && floor.match?(/\A\d+\.\d+\z/)
        errors << "#{name}: linux-gnu manifest needs glibc_floor (got #{floor.inspect})"
      end
      if floor.is_a?(String) && floor != OrbitcastRelease::LINUX_GLIBC_VERSION
        errors << "#{name}: manifest glibc_floor #{floor} != LINUX_GLIBC_VERSION #{OrbitcastRelease::LINUX_GLIBC_VERSION}"
      end
    end

    if target && OrbitcastRelease.linux_musl?(target) && manifest["libc"] != "musl"
      errors << "#{name}: linux-musl manifest needs libc: \"musl\" (got #{manifest["libc"].inspect})"
    end

    if target && OrbitcastRelease.darwin?(target)
      minos = manifest["macos_deployment_target"]
      unless minos.is_a?(String) && minos.match?(/\A\d+\.\d+\z/)
        errors << "#{name}: darwin manifest needs macos_deployment_target (got #{minos.inspect})"
      end
      if minos.is_a?(String) && minos != OrbitcastRelease::MACOS_DEPLOYMENT_TARGET
        errors << "#{name}: manifest macos_deployment_target #{minos} != MACOS_DEPLOYMENT_TARGET #{OrbitcastRelease::MACOS_DEPLOYMENT_TARGET}"
      end
    end

    if manifest.key?("gem_platform") && manifest["gem_platform"] != platform
      errors << "#{name}: manifest gem_platform #{manifest["gem_platform"].inspect} != #{platform}"
    end

    errors
  end
end
