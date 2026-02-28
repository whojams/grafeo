//! Crash injection for testing recovery paths.
//!
//! When the `testing-crash-injection` feature is enabled, [`maybe_crash`]
//! counts down a global counter and panics when it reaches zero. Tests use
//! [`with_crash_at`] to run a closure that crashes at a deterministic point,
//! then verify that recovery produces a consistent state.
//!
//! When the feature is **disabled**, all functions compile to no-ops with zero
//! runtime overhead.
//!
//! # Example
//!
//! ```ignore
//! use grafeo_core::testing::crash::{with_crash_at, CrashResult};
//!
//! for point in 1..20 {
//!     let result = with_crash_at(point, || {
//!         // operations that call maybe_crash() internally
//!     });
//!     match result {
//!         CrashResult::Completed(value) => { /* ran to completion */ }
//!         CrashResult::Crashed => { /* verify recovery */ }
//!     }
//! }
//! ```

#[cfg(feature = "testing-crash-injection")]
mod inner {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    static CRASH_COUNTER: AtomicU64 = AtomicU64::new(u64::MAX);
    static CRASH_ENABLED: AtomicBool = AtomicBool::new(false);

    /// Conditionally panic when the crash counter reaches zero.
    ///
    /// Insert this at interesting recovery boundaries (before/after WAL
    /// writes, flushes, checkpoints). When crash injection is disabled,
    /// this compiles to nothing.
    #[inline]
    pub fn maybe_crash(point: &'static str) {
        if !CRASH_ENABLED.load(Ordering::Relaxed) {
            return;
        }
        let prev = CRASH_COUNTER.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            panic!("crash injection at: {point}");
        }
    }

    /// Enable crash injection to fire after `count` calls to [`maybe_crash`].
    pub fn enable_crash_at(count: u64) {
        CRASH_COUNTER.store(count, Ordering::SeqCst);
        CRASH_ENABLED.store(true, Ordering::SeqCst);
    }

    /// Disable crash injection (reset to no-op behavior).
    pub fn disable_crash() {
        CRASH_ENABLED.store(false, Ordering::SeqCst);
        CRASH_COUNTER.store(u64::MAX, Ordering::SeqCst);
    }
}

#[cfg(not(feature = "testing-crash-injection"))]
mod inner {
    #[inline(always)]
    pub fn maybe_crash(_point: &'static str) {}

    pub fn enable_crash_at(_count: u64) {}

    pub fn disable_crash() {}
}

pub use inner::*;

/// Outcome of a crash-injected run.
pub enum CrashResult<T> {
    /// The closure completed without crashing.
    Completed(T),
    /// A crash was injected (panic caught).
    Crashed,
}

/// Run `f` with crash injection armed to fire after `crash_after` calls to
/// [`maybe_crash`]. Returns [`CrashResult::Crashed`] if the injected panic
/// was caught, or [`CrashResult::Completed`] with the return value otherwise.
///
/// Crash injection is automatically disabled after the closure returns
/// (whether normally or via panic).
pub fn with_crash_at<F, T>(crash_after: u64, f: F) -> CrashResult<T>
where
    F: FnOnce() -> T + std::panic::UnwindSafe,
{
    enable_crash_at(crash_after);
    let result = std::panic::catch_unwind(f);
    disable_crash();

    match result {
        Ok(value) => CrashResult::Completed(value),
        Err(_) => CrashResult::Crashed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crash_at_exact_count() {
        let result = with_crash_at(3, || {
            maybe_crash("point_1");
            maybe_crash("point_2");
            maybe_crash("point_3"); // should crash here
            42 // should not reach
        });
        assert!(matches!(result, CrashResult::Crashed));
    }

    #[test]
    fn completes_when_count_exceeds_calls() {
        let result = with_crash_at(100, || {
            maybe_crash("a");
            maybe_crash("b");
            42
        });
        match result {
            CrashResult::Completed(v) => assert_eq!(v, 42),
            CrashResult::Crashed => panic!("should not crash"),
        }
    }

    #[test]
    fn disabled_by_default() {
        // Without enabling, maybe_crash is a no-op
        maybe_crash("should_not_crash");
    }

    #[test]
    fn disable_resets_state() {
        enable_crash_at(2);
        disable_crash();
        // After disable, crash should not fire
        maybe_crash("a");
        maybe_crash("b");
        maybe_crash("c");
    }
}
