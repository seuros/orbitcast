# frozen_string_literal: true

# The dummy application runs on the gem's own bundle (gem/Gemfile), where Rails,
# Turbo, importmap, Propshaft, and sqlite3 live in the :development/:test group.
# The plain-ruby unit suite (`ruby -Ilib -Itest`) never loads any of it.
ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../../../Gemfile", __dir__)

require "bundler/setup"
