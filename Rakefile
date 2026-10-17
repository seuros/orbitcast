# frozen_string_literal: true

# Root Rakefile: imports tasks/*.rake and sets the default. Must load with
# plain system ruby + rake — no Bundler, no Rails, no cargo at load time.
#
#   tasks/test.rake     test, test:rust, test:gem, contract:test
#   tasks/native.rake   native:build[target], native:package[platform],
#                       native:smoke[platform], native:all, platforms:build
#   tasks/release.rake  release:verify, release:package, gems:fetch, gems:push

Dir.glob(File.expand_path("tasks/*.rake", __dir__)).sort.each { |file| import file }

task default: :test
