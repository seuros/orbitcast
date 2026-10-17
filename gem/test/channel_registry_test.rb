# frozen_string_literal: true

require "test_helper"
require "fileutils"

begin
  require "orbitcast/channel"
  require "orbitcast/channel_registry"
  CHANNEL_REGISTRY_AVAILABLE = true
rescue LoadError
  # Resolution is against Orbitcast::Channel, which needs ActionCable.
  CHANNEL_REGISTRY_AVAILABLE = false
end

# The channel allowlist: a client-supplied channel name outside it is
# never constantized.
if CHANNEL_REGISTRY_AVAILABLE
  class ChannelRegistryTest < Minitest::Test
    Registry = Orbitcast::ChannelRegistry

    class AllowedChannel < Orbitcast::Channel; end
    class UnlistedChannel < Orbitcast::Channel; end

    ALLOWED = "ChannelRegistryTest::AllowedChannel"
    UNLISTED = "ChannelRegistryTest::UnlistedChannel"

    def test_an_allowlisted_channel_resolves
      assert_equal AllowedChannel, Registry.new([ALLOWED]).resolve(ALLOWED)
    end

    def test_an_unlisted_channel_never_resolves_even_though_the_constant_exists
      registry = Registry.new([ALLOWED])

      assert_nil registry.resolve(UNLISTED)
      assert defined?(UnlistedChannel), "the constant exists; the allowlist is what refuses it"
    end

    # The interesting half: a name that is a real constant but not a channel.
    def test_a_listed_name_that_is_not_a_channel_does_not_resolve
      assert_nil Registry.new(["Kernel"]).resolve("Kernel")
      assert_nil Registry.new(["Orbitcast"]).resolve("Orbitcast")
      assert_nil Registry.new(["ActionCable::Channel::Base"]).resolve("ActionCable::Channel::Base")
    end

    def test_hostile_names_are_refused_without_being_constantized
      registry = Registry.new([ALLOWED])

      ["", "Kernel", "Object", "File", "ChatChannel; system('rm -rf /')", nil, 42,
       "ChannelRegistryTest::AllowedChannel "].each do |name|
        assert_nil registry.resolve(name), name.inspect
      end
    end

    def test_an_empty_registry_resolves_nothing
      assert_predicate Registry.new([]), :empty?
      assert_nil Registry.new([]).resolve(ALLOWED)
    end

    def test_validate_refuses_a_typo_at_boot
      error = assert_raises(Orbitcast::ConfigurationError) do
        Registry.new(["ChatChannell"]).validate!
      end

      assert_includes error.message, "ChatChannell"

      registry = Registry.new([ALLOWED])

      assert_same registry, registry.validate!
    end

    def test_build_accepts_an_array_or_auto
      assert_equal [ALLOWED], Registry.build([ALLOWED]).names
      assert_raises(Orbitcast::ConfigurationError) { Registry.build("ChatChannel") }
    end

    # :auto derivation reads the application's app/channels tree and turns each
    # file into the constant Zeitwerk would define for it.
    def test_auto_derivation_reads_the_application_channel_tree
      root = File.expand_path("../tmp/registry_app", __dir__)
      FileUtils.mkdir_p(File.join(root, "app", "channels", "chat"))
      FileUtils.touch(File.join(root, "app", "channels", "chat_channel.rb"))
      FileUtils.touch(File.join(root, "app", "channels", "chat", "room_channel.rb"))
      FileUtils.touch(File.join(root, "app", "channels", "application_cable", "channel.rb").tap do |path|
        FileUtils.mkdir_p(File.dirname(path))
      end)

      names = Registry.auto(root).names

      assert_includes names, "ChatChannel"
      assert_includes names, "Chat::RoomChannel"
      refute_includes names, "ApplicationCable::Channel", "the base class is not subscribable"
    ensure
      FileUtils.rm_rf(root)
    end

    def test_auto_derivation_of_an_application_without_channels_is_empty
      root = File.expand_path("../tmp/registry_empty_app", __dir__)
      FileUtils.mkdir_p(File.join(root, "app"))

      assert_predicate Registry.auto(root), :empty?
    ensure
      FileUtils.rm_rf(root)
    end
  end
end
