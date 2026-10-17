//! Lock helpers shared across the crate.

use std::sync::{Mutex, MutexGuard, PoisonError};

/// Lock a std mutex, recovering the guard if a previous holder panicked.
///
/// Crate policy for every std `Mutex` here: each guards plain data that one
/// short critical section updates in a single step (a weak registry, a dedupe
/// window, a cursor map, a set of task handles). A panic elsewhere cannot leave
/// that data half-written, so poisoning carries nothing worth turning into a
/// second panic on an unrelated task.
pub fn lock<T: ?Sized>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
