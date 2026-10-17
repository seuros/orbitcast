# frozen_string_literal: true

# Overflow relay table for publications too large for a PostgreSQL NOTIFY
# payload (8000 bytes, envelope and escaping included).
#
# Contract shared with the Rust server (`src/pubsub/postgres.rs`); the column
# names and types below are wire contract, not an implementation detail:
#
#   orbitcast_payloads(id uuid primary key, namespace text not null,
#                      stream text not null, json_payload text not null,
#                      expires_at timestamptz not null)
#
# The publisher inserts one row and emits its `kind: "overflow"` reference in
# the *same transaction*, so no node can observe a reference to a row it cannot
# read. Every listening node fetches the row from the same primary with a fresh
# READ COMMITTED snapshot; nobody deletes it after their own delivery. Expiry
# cleanup belongs to the Rust server, which is why `expires_at` is indexed.
#
# This is a relay, not durable history: it is not replay, not offline delivery,
# and not a guarantee of delivery during a listener outage. Install it in the
# cable database (which may be separate from the primary application database).
class CreateOrbitcastPayloads < ActiveRecord::Migration[8.1]
  def change
    create_table :orbitcast_payloads, id: false do |t|
      # `default: nil`: the id is always the publication UUID supplied by the
      # publisher, so no gen_random_uuid()/pgcrypto default is needed.
      t.primary_key :id, :uuid, default: nil
      t.text :namespace, null: false
      t.text :stream, null: false
      t.text :json_payload, null: false
      # Explicitly timestamptz rather than `t.datetime`: expiry is compared
      # against the database clock (`now()`), never an application clock, and
      # must not depend on the app's `datetime_type` default.
      t.column :expires_at, :timestamptz, null: false
    end

    add_index :orbitcast_payloads, :expires_at
  end
end
