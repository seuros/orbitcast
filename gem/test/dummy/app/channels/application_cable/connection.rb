# frozen_string_literal: true

module ApplicationCable
  # In Rails-dispatch mode an application changes the inheritance of its two
  # ApplicationCable base classes and keeps everything else.
  # This is that change, in a real Rails 8.1 application.
  class Connection < Orbitcast::Connection
    identified_by :current_user

    # Makes the connection addressable by Orbitcast.disconnect. Without it the
    # session is anonymous as far as remote control is concerned.
    orbitcast_identity_key :current_user

    orbitcast_state :locale

    # ActionCable's own command filter, wrapping every subscribe/action the
    # relay dispatches here — the place an application sets `Current.account`
    # or a request tag for the duration of a command.
    around_command :journal_command

    # Observability for the integration test: what ran, and under which
    # execution context.
    class << self
      attr_accessor :journal
    end
    self.journal = []

    def connect
      self.current_user = find_verified_user
      self.locale = "fr"

      # Per-request state: the executor is what resets it afterwards.
      Current.user = current_user

      self.class.journal << [:connect, Rails.application.executor.active?, Current.user&.id]
    end

    def disconnect
      self.class.journal << [:disconnect, current_user&.id]
    end

    private

    def journal_command
      self.class.journal << [:around_command, :before]
      yield
      self.class.journal << [:around_command, :after]
    end

    def find_verified_user
      Widget.find_by(id: cookies.signed[:user_id]) || reject_unauthorized_connection
    end
  end
end
