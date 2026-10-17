# frozen_string_literal: true

require "test_helper"

begin
  # globalid's URI parser reaches for CGI, which Rails loads for it. Outside a
  # Rails boot nothing has, so require it here rather than skip the whole file.
  require "cgi"
  require "global_id"
  GLOBAL_ID_AVAILABLE = true
rescue LoadError
  # The primitive half of the codec is plain Ruby and always runs; the record
  # references need the globalid gem, which arrives with Rails.
  GLOBAL_ID_AVAILABLE = false
end

# Declared state serialization.
class StateCodecTest < Minitest::Test
  Codec = Orbitcast::StateCodec

  # MOCK: an in-memory record with a real GlobalID identity. No Rails, no
  # database — `find` is a Hash lookup, which is all the locator needs.
  if GLOBAL_ID_AVAILABLE
    class Widget
      include ::GlobalID::Identification

      STORE = {}

      class << self
        def create(id)
          STORE[id.to_s] = new(id)
        end

        def find(id)
          STORE.fetch(id.to_s) { raise "record not found: #{id}" }
        end

        def delete_all
          STORE.clear
        end
      end

      attr_reader :id

      def initialize(id)
        @id = id
      end

      def ==(other)
        other.is_a?(Widget) && other.id == id
      end
    end
  end

  # A declaring object that is neither a Connection nor a Channel: the codec is
  # independent of both.
  class Holder
    extend Orbitcast::StateCodec::Declaration
    include Orbitcast::StateCodec::Instance

    orbitcast_state :cursor, :widget
  end

  class Inheriting < Holder
    orbitcast_state :extra
  end

  def setup
    return unless GLOBAL_ID_AVAILABLE

    ::GlobalID.app = "orbitcast-test"
    Widget.delete_all
  end

  # --- primitives -----------------------------------------------------------

  def test_json_primitives_round_trip_unchanged
    assert_nil Codec.load(Codec.dump(nil))

    [true, false, "text", 42, 1.5, [1, "two", nil], { "a" => [1, { "b" => 2 }] }].each do |value|
      assert_equal value, Codec.load(Codec.dump(value)), value.inspect
    end
  end

  def test_symbols_are_dumped_as_strings
    assert_equal "fr", Codec.dump(:fr)
  end

  def test_values_that_are_not_state_material_are_refused
    [Object.new, -> { 1 }, StringIO.new("x"), Float::INFINITY].each do |value|
      assert_raises(Orbitcast::ProtocolError, value.class.to_s) { Codec.dump(value) }
    end
  end

  # A Marshal blob or an arbitrary ivar would be the easy way to make state
  # "just work"; it is also how a deserialization gadget arrives.
  def test_the_reserved_gid_key_cannot_be_forged_by_a_plain_hash
    error = assert_raises(Orbitcast::ProtocolError) do
      Codec.dump({ "_gid" => "gid://app/User/1" })
    end

    assert_includes error.message, "_gid"
  end

  # --- declarations ---------------------------------------------------------

  def test_only_declared_keys_are_dumped
    holder = Holder.new
    holder.instance_variable_set(:@cursor, 19)
    holder.instance_variable_set(:@undeclared, "secret")

    assert_equal({ "cursor" => 19 }, holder.orbitcast_dump_state)
  end

  def test_an_unset_key_is_omitted_rather_than_dumped_as_null
    assert_empty Holder.new.orbitcast_dump_state
  end

  def test_declarations_inherit
    assert_equal %i[cursor widget], Holder.orbitcast_state_keys
    assert_equal %i[cursor widget extra], Inheriting.orbitcast_state_keys
  end

  # The accessors exist for the application, not for a client-dispatched
  # action: ActionCable's action allowlist is every *public* method a channel
  # adds, so a public `cursor=` would be remotely callable.
  def test_declared_accessors_are_private
    assert_includes Holder.private_instance_methods, :cursor
    assert_includes Holder.private_instance_methods, :cursor=
    refute_includes Holder.public_instance_methods, :cursor=
  end

  def test_restore_only_sets_declared_keys
    holder = Holder.new
    holder.orbitcast_restore_state({ "cursor" => 7, "injected" => "no" })

    assert_equal 7, holder.instance_variable_get(:@cursor)
    refute holder.instance_variable_defined?(:@injected)
  end

  def test_dump_map_validates_against_the_wire_limits
    assert_raises(Orbitcast::ProtocolError) do
      Codec.dump_map({ "k" * (Orbitcast::Protocol::V1::MAX_STATE_KEY_LEN + 1) => 1 })
    end
  end

  # --- records --------------------------------------------------------------

  def test_a_record_is_dumped_as_an_explicitly_tagged_reference
    skip "globalid is not installed" unless GLOBAL_ID_AVAILABLE

    widget = Widget.create(7)

    assert_equal({ "_gid" => "gid://orbitcast-test/StateCodecTest::Widget/7" }, Codec.dump(widget))
  end

  def test_a_record_round_trips_through_declared_state
    skip "globalid is not installed" unless GLOBAL_ID_AVAILABLE

    widget = Widget.create(7)
    holder = Holder.new
    holder.instance_variable_set(:@widget, widget)

    dumped = holder.orbitcast_dump_state

    assert_equal({ "_gid" => "gid://orbitcast-test/StateCodecTest::Widget/7" }, dumped["widget"])

    restored = Holder.new
    restored.orbitcast_restore_state(JSON.parse(JSON.generate(dumped)))

    assert_equal widget, restored.instance_variable_get(:@widget)
  end

  # The relay cannot replay a callback that has already run, so a reference to
  # a record deleted between two requests restores as nil. A channel that needs
  # the record checks for it; nothing raises out of the codec.
  def test_a_deleted_record_restores_as_nil
    skip "globalid is not installed" unless GLOBAL_ID_AVAILABLE

    widget = Widget.create(7)
    dumped = Codec.dump(widget)
    Widget.delete_all

    assert_nil Codec.load(dumped)
  end

  def test_a_malformed_reference_restores_as_nil
    skip "globalid is not installed" unless GLOBAL_ID_AVAILABLE

    assert_nil Codec.load({ "_gid" => "not-a-gid" })
    assert_nil Codec.load({ "_gid" => "gid://orbitcast-test/NoSuchModel/1" })
  end

  def test_a_hash_that_is_not_a_reference_is_loaded_as_a_hash
    assert_equal({ "_gid" => "x", "extra" => 1 }, Codec.load({ "_gid" => "x", "extra" => 1 }))
  end
end
