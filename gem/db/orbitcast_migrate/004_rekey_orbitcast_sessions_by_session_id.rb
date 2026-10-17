# frozen_string_literal: true

# Recovery sessions are fenced per *session*, not per token.
#
# Migration 003 keyed `orbitcast_sessions` by the SHA-256 of the recovery
# token. That made two tokens minted for one session (a refreshed token, a
# second tab holding a newer one) independent rows that never fenced each
# other, so two restores of the same session could run at once. The row is now
# keyed by the `session_id` claim: the identity *reference* the application put
# in the token, already stored in the snapshot JSON and never a cookie, never
# Marshal data, never a secret. The newest resume of a session wins wherever it
# lands and whichever token it carries.
#
#   orbitcast_sessions(namespace text, session_id text, snapshot text,
#                      fencing_generation bigint, expires_at timestamptz,
#                      primary key (namespace, session_id))
#
# The table is dropped and recreated rather than altered: a session row is
# ephemeral (`broker_session_ttl_secs`, default 1 h, from the session's last
# activity), and rows keyed by a token hash are unreachable under the new key.
# Clients that were mid-recovery across this migration resume with a fresh
# lease on their next `resume`, exactly as they would after the TTL.
#
# The expiry indexes also become namespace-scoped: every sweep, the namespace
# quota trim, and the growth aggregate the relay exports as
# `orbitcast_broker_messages_current` / `orbitcast_broker_bytes_current` run
# `WHERE namespace = $1`, so `(namespace, expires_at)` serves them and the
# single-column index would only be a second write per row.
#
# Install in the cable database; only needed when `broker_enabled` is set on
# the server. The relay refuses to start the broker against the 003 shape and
# names this migration.
class RekeyOrbitcastSessionsBySessionId < ActiveRecord::Migration[8.1]
  def up
    drop_table :orbitcast_sessions

    create_table :orbitcast_sessions, id: false, primary_key: %i[namespace session_id] do |t|
      t.text :namespace, null: false
      # The `session_id` claim of the recovery token: an application-chosen
      # identity reference, bounded to 256 bytes by the relay.
      t.text :session_id, null: false
      # Versioned JSON: {"version":1,"session_id":"...","cursors":{...}}.
      # Cursors only, so there is nothing sensitive to encrypt.
      t.text :snapshot, null: false
      # Bumped atomically by every resume; the highest holder owns the session.
      t.bigint :fencing_generation, null: false, default: 0
      # Extended by every resume, acknowledgement, and live tail.
      t.column :expires_at, :timestamptz, null: false
    end

    add_index :orbitcast_sessions, %i[namespace expires_at]

    remove_index :orbitcast_messages, :expires_at
    add_index :orbitcast_messages, %i[namespace expires_at]
  end

  def down
    remove_index :orbitcast_messages, %i[namespace expires_at]
    add_index :orbitcast_messages, :expires_at

    drop_table :orbitcast_sessions

    create_table :orbitcast_sessions, id: false,
                                      primary_key: %i[namespace session_token_hash] do |t|
      t.text :namespace, null: false
      t.text :session_token_hash, null: false
      t.text :snapshot, null: false
      t.bigint :fencing_generation, null: false, default: 0
      t.column :expires_at, :timestamptz, null: false
    end

    add_index :orbitcast_sessions, :expires_at
  end
end
