# frozen_string_literal: true

# Test tasks.
#
# `rake test` runs both language suites with nothing but system ruby + cargo.
# `rake contract:test` runs the two cross-language contract suites against the
# same checked-in fixtures under tests/fixtures/: the Rust
# half validates, the Ruby half produces. The Ruby half runs under Bundler with
# gem/Gemfile because that is the environment the gem is released from.

ROOT_DIR = File.expand_path("..", __dir__) unless defined?(ROOT_DIR)

namespace :test do
  desc "Rust suite: cargo test with default features (postgres code paths, no live database)"
  task :rust do
    sh "cargo", "test", "--locked", chdir: ROOT_DIR
  end

  desc "Gem suite: plain-ruby Minitest, no Rails boot, no ActionCable, no PostgreSQL"
  task :gem do
    gem_dir = File.join(ROOT_DIR, "gem")
    files = Dir[File.join(gem_dir, "test", "**", "*_test.rb")].sort
    abort "no gem tests found under #{gem_dir}/test" if files.empty?

    ruby "-I", File.join(gem_dir, "lib"),
         "-I", File.join(gem_dir, "test"),
         "-e", "ARGV.each { |file| require file }",
         *files
  end
end

desc "Run the Rust and gem test suites"
task test: %w[test:rust test:gem]

namespace :contract do
  desc "Cross-language contract suites over tests/fixtures (cargo --test contract + gem/test/contract_test.rb under BUNDLE_GEMFILE=gem/Gemfile)"
  task :test do
    sh "cargo", "test", "--locked", "--test", "contract", chdir: ROOT_DIR

    gem_dir = File.join(ROOT_DIR, "gem")
    env = { "BUNDLE_GEMFILE" => File.join(gem_dir, "Gemfile") }
    sh env, "bundle", "exec", "ruby",
       "-I", File.join(gem_dir, "lib"),
       "-I", File.join(gem_dir, "test"),
       File.join(gem_dir, "test", "contract_test.rb"),
       chdir: ROOT_DIR
  end
end
