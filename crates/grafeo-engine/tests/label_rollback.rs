//! Tests for label mutation rollback behavior.
//!
//! Verifies that ADD/REMOVE label operations are correctly undone
//! when a transaction is rolled back.

use grafeo_engine::GrafeoDB;

#[test]
fn test_add_label_rollback_removes_label() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();

    session.execute("INSERT (:Person {name: 'Alix'})").unwrap();

    // Verify initial labels
    let result = session
        .execute("MATCH (p:Person {name: 'Alix'}) RETURN labels(p)")
        .unwrap();
    assert_eq!(result.row_count(), 1);

    // Begin transaction, add a label, then rollback
    session.begin_transaction().unwrap();
    session
        .execute("MATCH (p:Person {name: 'Alix'}) SET p:Employee")
        .unwrap();

    // Verify label was added within transaction
    let result = session
        .execute("MATCH (p:Employee {name: 'Alix'}) RETURN p.name")
        .unwrap();
    assert_eq!(
        result.row_count(),
        1,
        "should find node with Employee label in tx"
    );

    session.rollback().unwrap();

    // Label should be gone after rollback
    let result = session
        .execute("MATCH (p:Employee {name: 'Alix'}) RETURN p.name")
        .unwrap();
    assert_eq!(
        result.row_count(),
        0,
        "Employee label should not exist after rollback"
    );

    // Original label should still be there
    let result = session
        .execute("MATCH (p:Person {name: 'Alix'}) RETURN p.name")
        .unwrap();
    assert_eq!(result.row_count(), 1, "Person label should still exist");
}

#[test]
fn test_remove_label_rollback_restores_label() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();

    session
        .execute("INSERT (:Person:Employee {name: 'Gus'})")
        .unwrap();

    // Verify both labels exist
    let result = session
        .execute("MATCH (p:Employee {name: 'Gus'}) RETURN p.name")
        .unwrap();
    assert_eq!(result.row_count(), 1);

    // Begin transaction, remove a label, then rollback
    session.begin_transaction().unwrap();
    session
        .execute("MATCH (p:Person {name: 'Gus'}) REMOVE p:Employee")
        .unwrap();

    // Verify label was removed within transaction
    let result = session
        .execute("MATCH (p:Employee {name: 'Gus'}) RETURN p.name")
        .unwrap();
    assert_eq!(result.row_count(), 0, "Employee label should be gone in tx");

    session.rollback().unwrap();

    // Label should be restored after rollback
    let result = session
        .execute("MATCH (p:Employee {name: 'Gus'}) RETURN p.name")
        .unwrap();
    assert_eq!(
        result.row_count(),
        1,
        "Employee label should be restored after rollback"
    );
}

#[test]
fn test_add_label_committed_stays() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();

    session
        .execute("INSERT (:Person {name: 'Vincent'})")
        .unwrap();

    // Add label in a committed transaction
    session.begin_transaction().unwrap();
    session
        .execute("MATCH (p:Person {name: 'Vincent'}) SET p:VIP")
        .unwrap();
    session.commit().unwrap();

    // Label should persist
    let result = session
        .execute("MATCH (p:VIP {name: 'Vincent'}) RETURN p.name")
        .unwrap();
    assert_eq!(
        result.row_count(),
        1,
        "VIP label should persist after commit"
    );
}
