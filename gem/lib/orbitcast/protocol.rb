# frozen_string_literal: true

require_relative "protocol/v1"

module Orbitcast
  # Handwritten JSON envelopes for the Orbitcast wire protocol.
  #
  # There is no schema compiler and no generated code: `src/protocol/` is the
  # authority, these classes are the Ruby reading of it, and
  # `tests/fixtures/**/*.json` is what keeps the two honest. Both suites consume
  # the same fixtures; neither regenerates them.
  #
  # Everything here must load without Rails: the RPC endpoint needs Rails, the
  # envelopes do not.
  module Protocol
    # The only envelope version this release speaks.
    VERSION = V1::VERSION
  end
end
