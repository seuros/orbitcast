# frozen_string_literal: true

# Retained publications, per-stream ordering, and recovery sessions.
#
# Contract shared with the Rust server (`src/broker/postgres.rs`); the column
# names and types below are wire contract, not an implementation detail:
#
#   orbitcast_messages(namespace text, stream text, generation bigint,
#                      seq bigint, publication_id uuid, json_payload text,
#                      created_at timestamptz, expires_at timestamptz,
#                      primary key (namespace, stream, generation, seq))
#   orbitcast_stream_positions(namespace text, stream text, generation bigint,
#                              next_seq bigint, updated_at timestamptz,
#                              primary key (namespace, stream))
#   orbitcast_sessions(namespace text, session_token_hash text, snapshot text,
#                      fencing_generation bigint, expires_at timestamptz,
#                      primary key (namespace, session_token_hash))
#
# Ordering. `seq` is assigned inside the transaction that inserts the message,
# while holding `SELECT ... FOR UPDATE` on that stream's
# `orbitcast_stream_positions` row. The lock is held until commit, so sequence
# order *is* commit order and a reader can treat "seq > last_delivered" as
# "everything I have not seen". A global BIGSERIAL cannot do this: two
# transactions can take values in one order and commit in the other, which
# would let a reader skip history permanently. There is deliberately no
# sequence, no serial column, and no ordering by `created_at`.
#
# Generations. `generation` is bumped when retention wipes a stream's history,
# so an old delivery cursor is recognizably stale instead of silently matching
# sequence numbers from a new era.
#
# Deduplication. The unique index on (namespace, stream, publication_id) makes a
# repeat publication id a no-op within the retention window: first write wins,
# and the conflict is counted and warned by the server. After expiry the id is
# forgotten and may be stored again.
#
# Sessions. `session_token_hash` is the SHA-256 hex digest of the recovery
# token - never the token itself - so a database leak cannot be replayed as a
# capability. `snapshot` is versioned JSON holding delivery cursors and an
# identity *reference*: never browser cookies, never Ruby Marshal data, never
# sensitive connection attributes. `fencing_generation` is bumped atomically by
# every resume, so two nodes can never restore one session at once.
#
# Retention is bounded by time (`expires_at`, swept in batches), by message
# count per stream (enforced in the publish transaction), and by bytes per
# stream (swept). Expiry cleanup belongs to the Rust server, which is why both
# `expires_at` columns are indexed. Install in the cable database (which may be
# separate from the primary application database); only needed when
# `broker_enabled` is set on the server.
class CreateOrbitcastBroker < ActiveRecord::Migration[8.1]
  def change
    create_table :orbitcast_messages, id: false,
                                      primary_key: %i[namespace stream generation seq] do |t|
      t.text :namespace, null: false
      t.text :stream, null: false
      # The history era this sequence belongs to; bumped on a retention wipe.
      t.bigint :generation, null: false
      # Per-stream commit order, assigned under the stream's position lock.
      t.bigint :seq, null: false
      # The publisher-supplied publication UUID, which is also the client's
      # deduplication key across a replay-to-live handoff.
      t.column :publication_id, :uuid, null: false
      t.text :json_payload, null: false
      # Explicitly timestamptz rather than `t.datetime`: expiry and retention
      # are compared against the database clock (`now()`), never an application
      # clock, and must not depend on the app's `datetime_type` default.
      t.column :created_at, :timestamptz, null: false, default: -> { "now()" }
      t.column :expires_at, :timestamptz, null: false
    end

    # Deduplication within the retention window.
    add_index :orbitcast_messages, %i[namespace stream publication_id],
              unique: true, name: "index_orbitcast_messages_on_publication"
    # The sweeper's scan: expired rows first, bounded per statement.
    add_index :orbitcast_messages, :expires_at

    create_table :orbitcast_stream_positions, id: false, primary_key: %i[namespace stream] do |t|
      t.text :namespace, null: false
      t.text :stream, null: false
      t.bigint :generation, null: false, default: 1
      # The sequence the *next* publication will take. The row is locked
      # FOR UPDATE for the whole publish transaction; that lock is the ordering
      # mechanism, so this table is never written outside it.
      t.bigint :next_seq, null: false, default: 1
      t.column :updated_at, :timestamptz, null: false, default: -> { "now()" }
    end

    create_table :orbitcast_sessions, id: false,
                                      primary_key: %i[namespace session_token_hash] do |t|
      t.text :namespace, null: false
      # SHA-256 hex of the recovery token. The token itself is never stored.
      t.text :session_token_hash, null: false
      # Versioned JSON: {"version":1,"session_id":"...","cursors":{...}}.
      t.text :snapshot, null: false
      # Bumped atomically by every resume; the highest holder owns the session.
      t.bigint :fencing_generation, null: false, default: 0
      t.column :expires_at, :timestamptz, null: false
    end

    add_index :orbitcast_sessions, :expires_at
  end
end
