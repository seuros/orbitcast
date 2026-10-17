# frozen_string_literal: true

# Per-request state, reset by the Rails executor. The Rails-dispatch endpoint
# runs every call inside the executor, so a connection callback that sets
# `Current.user` must not leak it into the next request — which is what
# gem/test/rails_contract_test.rb asserts.
class Current < ActiveSupport::CurrentAttributes
  attribute :user
end
