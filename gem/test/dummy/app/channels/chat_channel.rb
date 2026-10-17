# frozen_string_literal: true

# An ordinary ActionCable channel: nothing here is Orbitcast-specific except
# the declared state, which is what makes a stateless request model possible.
class ChatChannel < ApplicationCable::Channel
  orbitcast_state :room, :cursor

  # ActionCable's own subscribe filter, running unmodified around `subscribed`.
  before_subscribe :journal_filter

  class << self
    attr_accessor :journal
  end
  self.journal = []

  def subscribed
    room = Widget.find_by(id: params[:room])
    return reject unless room && current_user

    self.class.journal << [:subscribed, Rails.application.executor.active?, room.name]

    self.room = room
    self.cursor = 0

    stream_for room
    stream_from "chat:all"
    transmit({ "type" => "backlog", "user" => current_user.name, "room" => room.name })
  end

  def unsubscribed
    self.class.journal << [:unsubscribed, identifier]
  end

  # The declared state comes back as a record, not as an id the channel has to
  # re-find by hand.
  def speak(data)
    self.cursor = cursor.to_i + 1
    transmit({ "said" => data["text"], "in" => room&.name, "by" => current_user.name })
  end

  def leave
    stop_stream_from("chat:all")
  end

  def leave_all
    stop_all_streams
  end

  # A stream transformation block: Ruby that would have to run inside the
  # relay per delivered message. Refused explicitly, never silently ignored.
  def transform
    stream_from("chat:transformed") { |message| message }
  end

  def close_everything
    connection.close(reason: "remote", reconnect: false)
  end

  private

  def journal_filter
    self.class.journal << [:before_subscribe, params[:room]]
  end
end
