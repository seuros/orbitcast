//! Keyed token-bucket admission control: connection/subscription/action rate
//! limits.
//!
//! The decision itself comes from the fleet's `throttle-machines` crate, whose
//! gates read no clock and keep no state but their own: a `TokenBucket` is
//! told the time and answers. That is the whole reason this module is small -
//! it stores one bucket per key in a `DashMap` and does nothing else.
//!
//! # Two limiters, one shape
//!
//! - **Per remote address, per minute** (`rate_connect_per_ip_per_min`):
//!   checked in [`crate::transport::http`] before a WebSocket upgrade. Over
//!   the limit is `429`.
//! - **Per session, per second** (`rate_commands_per_conn_per_sec`): checked
//!   in [`crate::server::Connections::handle_command`]. Over the limit closes
//!   the session with a reconnectable `disconnect`.
//!
//! Burst equals rate in both cases: a bucket starts full, so a client may open
//! its whole minute's worth of connections at once and then has to wait. That
//! is deliberate - a page that subscribes ten channels on load, or a client
//! that reconnects once after a network blip, must not be punished for
//! arriving all at once.
//!
//! # Bounded memory
//!
//! One entry per live key would be unbounded under a distributed flood, so the
//! table has a hard [`KeyedRateLimit::capacity`]. A full table is first swept
//! of *idle* keys - a fully refilled bucket has no memory, so dropping it
//! changes no decision. If every tracked key is still actively limited, a new
//! key is **denied**: the alternative is letting an attacker widen the table
//! until the process runs out of memory. This is reported through the caller's
//! rejection counter like any other refusal.
//!
//! # Clock
//!
//! A monotonic [`Instant`] captured at construction, read as the time since
//! then. Never the wall clock: a time step must not hand out a free burst or stall every
//! bucket.

use std::hash::Hash;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use throttle_machines::{Gate as _, TokenBucket, TokenBucketPolicy};

/// Default ceiling on tracked keys, for both limiters.
///
/// 64 Ki entries of a key and one bucket is a few megabytes - a fixed,
/// documented bound rather than a tuning knob.
pub const DEFAULT_CAPACITY: usize = 65_536;

/// A token bucket per key, with a bounded table.
pub struct KeyedRateLimit<K: Eq + Hash + Clone> {
    policy: TokenBucketPolicy,
    capacity: usize,
    buckets: DashMap<K, TokenBucket>,
    origin: Instant,
}

impl<K: Eq + Hash + Clone> KeyedRateLimit<K> {
    /// A limiter admitting `rate` events per minute, per key.
    ///
    /// `None` when `rate` is zero, which is the documented "disabled"
    /// spelling: callers hold an `Option` and skip the check entirely, so a
    /// disabled limiter costs no map and no clock read.
    #[must_use]
    pub fn per_minute(rate: u32, capacity: usize) -> Option<Self> {
        Self::new(rate, Duration::from_mins(1), capacity)
    }

    /// A limiter admitting `rate` events per second, per key.
    #[must_use]
    pub fn per_second(rate: u32, capacity: usize) -> Option<Self> {
        Self::new(rate, Duration::from_secs(1), capacity)
    }

    fn new(rate: u32, period: Duration, capacity: usize) -> Option<Self> {
        if capacity == 0 {
            return None;
        }
        let policy = TokenBucketPolicy::per(period, NonZeroU64::new(u64::from(rate))?)?;

        Some(Self { policy, capacity, buckets: DashMap::new(), origin: Instant::now() })
    }

    /// Whether one event for `key` is admitted, consuming a token if so.
    pub fn allow(&self, key: K) -> bool {
        let now = self.origin.elapsed();

        // A key already in the table never grows it, so the capacity check only
        // guards genuinely new keys.
        if !self.buckets.contains_key(&key) && self.buckets.len() >= self.capacity {
            self.sweep_idle(now);
            if self.buckets.len() >= self.capacity {
                return false;
            }
        }

        // The entry guard spans exactly this key's read-modify-write, so two
        // events for one key can never spend the same token.
        self.buckets.entry(key).or_insert(TokenBucket::new(self.policy)).check(now).is_ok()
    }

    /// Drop a key's bucket (a session that ended).
    ///
    /// Only ever called for keys whose lifetime this process owns - the
    /// per-address table is swept instead, because a remote address has no
    /// end.
    pub fn forget(&self, key: &K) {
        self.buckets.remove(key);
    }

    /// Keys currently tracked.
    #[must_use]
    pub fn tracked(&self) -> usize {
        self.buckets.len()
    }

    /// The table's hard ceiling.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Drop every fully refilled bucket.
    ///
    /// A full bucket is indistinguishable from a key that was never seen, so
    /// this frees memory without changing a single future decision.
    fn sweep_idle(&self, now: Duration) {
        let full = self.policy.capacity().get();
        self.buckets.retain(|_, bucket| !bucket.peek(now).is_ok_and(|tokens| tokens >= full));
    }
}

impl<K: Eq + Hash + Clone> std::fmt::Debug for KeyedRateLimit<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyedRateLimit")
            .field("burst", &self.policy.capacity())
            .field("refill_every", &self.policy.refill_every())
            .field("tracked", &self.buckets.len())
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
