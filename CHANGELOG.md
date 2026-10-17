# Changelog

## [0.3.0](https://github.com/seuros/orbitcast/compare/v0.2.0...v0.3.0) (2026-10-17)


### ⚠ BREAKING CHANGES

* `streams_whisper`/`streams_presence` config keys removed; signed-mode node-local presence tracker gone; `public_streams` requires `public_streams_insecure = true`; a PostgreSQL build exits without `database_url`/`namespace` or an unreachable database; unknown docking handshake keys are startup errors; broker users must run migration 004.

### Features

* Rails real-time stack — native gem, signed/dispatch modes, PostgreSQL transport ([3269f6f](https://github.com/seuros/orbitcast/commit/3269f6fc9f7b26d15b38fd9e15583897febd5e43))

## [0.2.0](https://github.com/seuros/orbitcast/compare/v0.1.10...v0.2.0) (2026-05-12)


### Features

* add AnyCable RPC protocol support ([29fee22](https://github.com/seuros/orbitcast/commit/29fee22a7970005c023f6b611f70497548034372))
* add TLS support for PostgreSQL connections ([39b5453](https://github.com/seuros/orbitcast/commit/39b54538c3848064ebb346cd1896704b4479e075))
* docking protocol GitHub builds ([#1](https://github.com/seuros/orbitcast/issues/1)) ([c801baa](https://github.com/seuros/orbitcast/commit/c801baaec046e04436e6d0d088e98e848ffdb1ba))
* OrbitCast - ActionCable WebSocket server for Mothership ([e9b460e](https://github.com/seuros/orbitcast/commit/e9b460e9b53526cbf932c26a57ca49cec53917d5))
