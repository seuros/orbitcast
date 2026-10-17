# frozen_string_literal: true

# Presence leases for authorized, distributed presence.
#
# Contract shared with the Rust server (`src/presence/postgres.rs`); the column
# names and types below are wire contract, not an implementation detail:
#
#   orbitcast_presence(namespace text not null, stream text not null,
#                      subject_id text not null, session_id text not null,
#                      node_id text not null, metadata text not null,
#                      expires_at timestamptz not null,
#                      primary key (namespace, stream, subject_id, session_id))
#
# One row is one *session's* lease: a subject with three tabs holds three rows,
# so one tab's refresh can never preserve another tab's lease. Clients see the
# aggregate - the first live lease announces a join, the last one to go
# announces a leave - and that decision is serialized per (namespace, stream,
# subject_id) with a PostgreSQL advisory transaction lock.
#
# `metadata` is text, not jsonb: it holds the exact JSON the Rails
# `presence_grant` effect produced, byte for byte, and is never queried into.
# The identity in `subject_id` and everything in `metadata` are *public to the
# stream's granted subscribers* - never put a session token, an email, or any
# other sensitive connection attribute there.
#
# Rows are removed by whichever node sweeps first; `expires_at` is compared
# against the database clock (`now()`), never an application clock, which is
# what bounds staleness when a node dies without releasing its leases. Presence
# is eventually consistent and is not an authorization database.
#
# Install it in the cable database (which may be separate from the primary
# application database). Only needed for Rails-dispatch deployments that grant
# presence; a signed-only deployment never writes here.
class CreateOrbitcastPresence < ActiveRecord::Migration[8.1]
  def change
    create_table :orbitcast_presence, id: false, primary_key: %i[namespace stream subject_id session_id] do |t|
      t.text :namespace, null: false
      t.text :stream, null: false
      t.text :subject_id, null: false
      t.text :session_id, null: false
      t.text :node_id, null: false
      t.text :metadata, null: false
      # Explicitly timestamptz rather than `t.datetime`: expiry is compared
      # against the database clock and must not depend on the app's
      # `datetime_type` default.
      t.column :expires_at, :timestamptz, null: false
    end

    # The sweeper's scan: expired rows first, bounded per statement.
    add_index :orbitcast_presence, :expires_at
    # Roster reads: snapshots and the per-subject counts a transition takes.
    add_index :orbitcast_presence, %i[namespace stream]
  end
end
