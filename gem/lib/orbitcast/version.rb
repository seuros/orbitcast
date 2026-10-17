# frozen_string_literal: true

module Orbitcast
  # Release version.
  #
  # `Cargo.toml` is the release authority; release-please keeps this constant in
  # lockstep through its extra-file updater. The trailing annotation below is the
  # updater marker -- do not reformat the line or the bump will silently no-op.
  VERSION = "0.2.0" # x-release-please-version
end
