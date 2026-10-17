-- The broker tables exactly as gem/db/orbitcast_migrate/003 + 004 leave them,
-- for throwaway test databases only. Production never issues DDL; the
-- migrations are the contract, this file mirrors them so the
-- crate's own #[ignore] tests and the tests/ integration crate share one copy.
-- Parallel `CREATE ... IF NOT EXISTS` can still collide in pg_type, so callers
-- check the postcondition (the tables exist) rather than this script's result.
CREATE TABLE IF NOT EXISTS orbitcast_messages (
    namespace text NOT NULL,
    stream text NOT NULL,
    generation bigint NOT NULL,
    seq bigint NOT NULL,
    publication_id uuid NOT NULL,
    json_payload text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (namespace, stream, generation, seq));
CREATE UNIQUE INDEX IF NOT EXISTS index_orbitcast_messages_on_publication
    ON orbitcast_messages (namespace, stream, publication_id);
CREATE INDEX IF NOT EXISTS index_orbitcast_messages_on_namespace_and_expires_at
    ON orbitcast_messages (namespace, expires_at);
CREATE TABLE IF NOT EXISTS orbitcast_stream_positions (
    namespace text NOT NULL,
    stream text NOT NULL,
    generation bigint NOT NULL DEFAULT 1,
    next_seq bigint NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, stream));
CREATE TABLE IF NOT EXISTS orbitcast_sessions (
    namespace text NOT NULL,
    session_id text NOT NULL,
    snapshot text NOT NULL,
    fencing_generation bigint NOT NULL DEFAULT 0,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (namespace, session_id));
CREATE INDEX IF NOT EXISTS index_orbitcast_sessions_on_namespace_and_expires_at
    ON orbitcast_sessions (namespace, expires_at);
CREATE TABLE IF NOT EXISTS orbitcast_payloads (
    id uuid PRIMARY KEY,
    namespace text NOT NULL,
    stream text NOT NULL,
    json_payload text NOT NULL,
    expires_at timestamptz NOT NULL);
CREATE INDEX IF NOT EXISTS index_orbitcast_payloads_on_expires_at
    ON orbitcast_payloads (expires_at);
