//! Crash injection tests for the single-file `.grafeo` format.
//!
//! These tests simulate crashes at deterministic points during checkpoint and
//! verify that the database recovers correctly on the next open.
//!
//! Requires both `grafeo-file` and `testing-crash-injection` features.

#![cfg(all(feature = "grafeo-file", feature = "testing-crash-injection"))]

use std::panic::AssertUnwindSafe;

use grafeo_common::types::Value;
use grafeo_core::testing::crash::{CrashResult, with_crash_at};
use grafeo_engine::{Config, GrafeoDB};

/// Helper: extract sorted string values from column 0 of query result rows.
fn extract_strings(rows: &[Vec<Value>]) -> Vec<String> {
    let mut names: Vec<String> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            Value::String(s) => Some(s.to_string()),
            _ => None,
        })
        .collect();
    names.sort();
    names
}

/// Helper: build the sidecar WAL path for a given `.grafeo` file.
fn sidecar_wal_path(path: &std::path::Path) -> std::path::PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".wal");
    std::path::PathBuf::from(p)
}

/// Assert that at least the main file or the sidecar WAL exists, so recovery
/// is possible. If neither exists, the crash destroyed all data.
fn assert_recoverable(path: &std::path::Path, context: &str) {
    let wal = sidecar_wal_path(path);
    assert!(
        path.exists() || wal.exists(),
        "{context}: neither the main file ({}) nor the sidecar WAL ({}) exist, \
         crash destroyed all data",
        path.display(),
        wal.display(),
    );
}

// =========================================================================
// Crash during checkpoint_to_file (close path)
// =========================================================================

#[test]
fn crash_during_close_checkpoint_preserves_data_via_sidecar_wal() {
    // There are 8 crash points in the checkpoint path:
    //   checkpoint_to_file: before_export, after_export, after_write_snapshot
    //   write_snapshot: before_data_write, after_data_write, after_truncate,
    //                   after_header_write, after_fsync
    //
    // For each crash point, we:
    // 1. Create a DB, insert data
    // 2. Crash during close
    // 3. Reopen and verify data survived (via sidecar WAL replay)

    for crash_point in 1..=8 {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("crash_test.grafeo");

        // Phase 1: Create and populate
        {
            let db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
            let session = db.session();
            session.execute("INSERT (:Person {name: 'Alix'})").unwrap();
            session.execute("INSERT (:Person {name: 'Gus'})").unwrap();
            session
                .execute(
                    "MATCH (a:Person {name: 'Alix'}), (b:Person {name: 'Gus'}) \
                     INSERT (a)-[:KNOWS]->(b)",
                )
                .unwrap();

            // Crash during close
            let db = AssertUnwindSafe(db);
            let result = with_crash_at(crash_point, move || {
                let _ = db.close();
            });

            // Some crash points may complete normally (if the crash counter
            // exceeds the number of maybe_crash calls before close finishes)
            match result {
                CrashResult::Crashed => {
                    // Sidecar WAL should exist (crash prevented cleanup)
                    // The file may or may not have a valid snapshot
                }
                CrashResult::Completed(()) => {
                    // Close completed: the crash point was past all crash
                    // injection calls. Data should be in the .grafeo file.
                }
            }
        }

        // Phase 2: Reopen and verify data survived
        assert_recoverable(&path, &format!("crash_point={crash_point}"));

        let db = GrafeoDB::open(&path).unwrap();
        let session = db.session();

        let result = session.execute("MATCH (p:Person) RETURN p.name").unwrap();
        let names = extract_strings(&result.rows);
        assert_eq!(
            names,
            vec!["Alix", "Gus"],
            "crash_point={crash_point}: data lost after crash"
        );

        assert_eq!(
            db.edge_count(),
            1,
            "crash_point={crash_point}: edge lost after crash"
        );

        db.close().unwrap();
    }
}

// =========================================================================
// Crash during explicit wal_checkpoint
// =========================================================================

#[test]
fn crash_during_wal_checkpoint_leaves_db_usable() {
    for crash_point in 1..=8 {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("wal_crash.grafeo");

        // Create and populate
        let db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
        let session = db.session();
        session
            .execute("INSERT (:City {name: 'Amsterdam'})")
            .unwrap();
        session.execute("INSERT (:City {name: 'Berlin'})").unwrap();

        // Crash during explicit checkpoint (db stays open)
        let db_ref = AssertUnwindSafe(&db);
        let result = with_crash_at(crash_point, move || {
            let _ = db_ref.wal_checkpoint();
        });

        match result {
            CrashResult::Crashed => {
                // DB may be in an inconsistent internal state after panic,
                // but the in-memory data should still be present
            }
            CrashResult::Completed(()) => {
                // Checkpoint completed successfully
            }
        }

        // Drop without close to simulate process exit
        drop(db);

        // Reopen: data should survive via sidecar WAL replay
        assert_recoverable(&path, &format!("crash_point={crash_point}"));

        let db2 = GrafeoDB::open(&path).unwrap();
        let session2 = db2.session();

        let result = session2.execute("MATCH (c:City) RETURN c.name").unwrap();
        let names = extract_strings(&result.rows);
        assert_eq!(
            names,
            vec!["Amsterdam", "Berlin"],
            "crash_point={crash_point}: data lost after checkpoint crash"
        );

        db2.close().unwrap();
    }
}

// =========================================================================
// Crash after first checkpoint, then more writes
// =========================================================================

#[test]
fn crash_after_successful_checkpoint_with_new_writes() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("incremental.grafeo");

    // Phase 1: Create, populate, and successfully checkpoint
    {
        let db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
        let session = db.session();
        session.execute("INSERT (:Person {name: 'Alix'})").unwrap();
        db.wal_checkpoint().unwrap();

        // Phase 2: Add more data
        session.execute("INSERT (:Person {name: 'Gus'})").unwrap();

        // Phase 3: Crash during close (after more writes)
        let db = AssertUnwindSafe(db);
        let _result = with_crash_at(1, move || {
            let _ = db.close();
        });
    }

    // Reopen: at minimum, pre-checkpoint data should survive.
    // If sidecar WAL was written before crash, post-checkpoint data may also survive.
    let db = GrafeoDB::open(&path).unwrap();
    let session = db.session();

    let result = session.execute("MATCH (p:Person) RETURN p.name").unwrap();
    let names = extract_strings(&result.rows);

    // Pre-checkpoint data must survive
    assert!(
        names.contains(&"Alix".to_string()),
        "pre-checkpoint data lost"
    );

    db.close().unwrap();
}

// =========================================================================
// Multiple checkpoint-crash-recover cycles
// =========================================================================

#[test]
fn repeated_crash_recovery_cycles() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("cycles.grafeo");

    let people = ["Alix", "Gus", "Vincent", "Jules", "Mia"];

    for (i, name) in people.iter().enumerate() {
        // Open (or create on first iteration)
        let db = if i == 0 {
            GrafeoDB::with_config(Config::persistent(&path)).unwrap()
        } else {
            GrafeoDB::open(&path).unwrap()
        };

        let session = db.session();
        session
            .execute(&format!("INSERT (:Person {{name: '{name}'}})"))
            .unwrap();

        // Alternate between clean close and crash
        if i % 2 == 0 {
            db.close().unwrap();
        } else {
            let db = AssertUnwindSafe(db);
            let _result = with_crash_at(2, move || {
                let _ = db.close();
            });
        }
    }

    // Final verification
    let db = GrafeoDB::open(&path).unwrap();
    let count = db.node_count();
    // At minimum the cleanly-closed sessions' data should persist
    assert!(count >= 3, "expected at least 3 nodes, got {count}");
    db.close().unwrap();
}

// =========================================================================
// Crash during sidecar WAL removal (close:before_remove_sidecar_wal)
// =========================================================================

/// Crashing between writing the snapshot and removing the sidecar WAL must
/// be safe: on the next open the sidecar WAL still exists, is replayed, and
/// then cleaned up on proper close.
#[test]
fn crash_before_sidecar_wal_removal_recovered_on_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("sidecar_crash.grafeo");

    // Phase 1: Populate, then crash exactly before remove_sidecar_wal
    {
        let db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
        let session = db.session();
        session
            .execute("INSERT (:Person {name: 'Django'})")
            .unwrap();
        session
            .execute("INSERT (:Person {name: 'Beatrix'})")
            .unwrap();

        // Crash point 9 = close:before_remove_sidecar_wal (added after the existing 8)
        let db = AssertUnwindSafe(db);
        let result = with_crash_at(9, move || {
            let _ = db.close();
        });

        match result {
            CrashResult::Crashed => {
                // Expected: snapshot was written, sidecar WAL was NOT removed
            }
            CrashResult::Completed(()) => {
                // Crash point exceeded the injection count - close completed normally.
                // This is fine; the test still verifies reopen works.
            }
        }
    }

    // Phase 2: Reopen - sidecar WAL may still exist (if crash happened); must recover
    assert_recoverable(&path, "crash before sidecar WAL removal");

    let db = GrafeoDB::open(&path).unwrap();
    let count = db.node_count();
    assert_eq!(
        count, 2,
        "both nodes must survive crash before sidecar removal"
    );

    let result = db
        .session()
        .execute("MATCH (p:Person) RETURN p.name ORDER BY p.name")
        .unwrap();
    let names = extract_strings(&result.rows);
    assert!(
        names.contains(&"Beatrix".to_string()),
        "Beatrix missing after crash"
    );
    assert!(
        names.contains(&"Django".to_string()),
        "Django missing after crash"
    );

    // Proper close must now clean up the sidecar WAL
    db.close().unwrap();

    let wal_path = sidecar_wal_path(&path);
    assert!(
        !wal_path.exists(),
        "sidecar WAL must be removed after the second (clean) close"
    );
}
