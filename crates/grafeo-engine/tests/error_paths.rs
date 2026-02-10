//! Error path integration tests.
//!
//! Tests that exercise error variants for NodeNotFound, EdgeNotFound,
//! invalid queries, double-commit, transaction state violations,
//! and other edge cases that should produce clear errors rather than panics.

use grafeo_common::types::{EdgeId, NodeId, Value};
use grafeo_engine::GrafeoDB;

// ============================================================================
// Entity Not Found
// ============================================================================

#[test]
fn test_get_nonexistent_node() {
    let db = GrafeoDB::new_in_memory();
    assert!(db.get_node(NodeId::new(999)).is_none());
}

#[test]
fn test_get_nonexistent_edge() {
    let db = GrafeoDB::new_in_memory();
    assert!(db.get_edge(EdgeId::new(999)).is_none());
}

#[test]
fn test_set_property_on_nonexistent_node() {
    let db = GrafeoDB::new_in_memory();
    // Setting a property on a missing node should not panic
    db.set_node_property(NodeId::new(999), "key", Value::Int64(1));
    // Node still doesn't exist
    assert!(db.get_node(NodeId::new(999)).is_none());
}

#[test]
fn test_delete_nonexistent_node() {
    let db = GrafeoDB::new_in_memory();
    assert!(!db.delete_node(NodeId::new(999)));
}

#[test]
fn test_delete_nonexistent_edge() {
    let db = GrafeoDB::new_in_memory();
    assert!(!db.delete_edge(EdgeId::new(999)));
}

#[test]
fn test_get_labels_nonexistent_node() {
    let db = GrafeoDB::new_in_memory();
    assert!(db.get_node_labels(NodeId::new(999)).is_none());
}

#[test]
fn test_add_label_nonexistent_node() {
    let db = GrafeoDB::new_in_memory();
    // Should not panic - returns false since node doesn't exist
    assert!(!db.add_node_label(NodeId::new(999), "Label"));
}

#[test]
fn test_remove_label_nonexistent_node() {
    let db = GrafeoDB::new_in_memory();
    assert!(!db.remove_node_label(NodeId::new(999), "Label"));
}

// ============================================================================
// Query Errors
// ============================================================================

#[test]
fn test_query_syntax_error() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let result = session.execute("THIS IS NOT VALID GQL");
    assert!(result.is_err(), "Invalid query should return error");

    let err = result.unwrap_err();
    let err_str = err.to_string();
    // Should be a query error, not a panic
    assert!(!err_str.is_empty(), "Error message should not be empty");
}

#[test]
fn test_query_empty_string() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let result = session.execute("");
    assert!(result.is_err(), "Empty query should return error");
}

#[test]
fn test_query_unclosed_parenthesis() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let result = session.execute("MATCH (n:Person RETURN n");
    assert!(result.is_err(), "Unclosed parenthesis should fail");
}

#[test]
fn test_query_undefined_variable() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // Reference a variable that was never matched
    let result = session.execute("MATCH (n:Person) RETURN x.name");
    assert!(result.is_err(), "Undefined variable should fail");
}

#[test]
fn test_query_return_without_match() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // RETURN without MATCH is valid for constants
    let result = session.execute("RETURN 1 + 2");
    // This may or may not work depending on implementation; just check no panic
    let _ = result;
}

// ============================================================================
// Transaction State Violations
// ============================================================================

#[test]
fn test_commit_without_begin() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();

    let result = session.commit();
    assert!(result.is_err(), "Commit without begin should fail");
}

#[test]
fn test_rollback_without_begin() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();

    let result = session.rollback();
    assert!(result.is_err(), "Rollback without begin should fail");
}

#[test]
fn test_double_begin() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();

    session.begin_tx().unwrap();
    let result = session.begin_tx();
    assert!(result.is_err(), "Double begin should fail");
}

#[test]
fn test_begin_after_commit_succeeds() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();

    session.begin_tx().unwrap();
    session.commit().unwrap();

    // Should be able to start new transaction
    let result = session.begin_tx();
    assert!(result.is_ok(), "Begin after commit should succeed");
    session.commit().unwrap();
}

#[test]
fn test_begin_after_rollback_succeeds() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();

    session.begin_tx().unwrap();
    session.rollback().unwrap();

    // Should be able to start new transaction
    let result = session.begin_tx();
    assert!(result.is_ok(), "Begin after rollback should succeed");
    session.commit().unwrap();
}

// ============================================================================
// Edge Cases - Empty Database
// ============================================================================

#[test]
fn test_query_empty_database() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let result = session.execute("MATCH (n) RETURN n").unwrap();
    assert_eq!(
        result.row_count(),
        0,
        "Empty database should return no rows"
    );
}

#[test]
fn test_count_on_empty_database() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let result = session.execute("MATCH (n) RETURN COUNT(n) AS cnt").unwrap();
    assert_eq!(
        result.row_count(),
        1,
        "COUNT on empty db should return one row"
    );
}

#[test]
fn test_validate_empty_database() {
    let db = GrafeoDB::new_in_memory();
    let result = db.validate();
    assert!(result.is_valid(), "Empty database should be valid");
}

#[test]
fn test_schema_empty_database() {
    let db = GrafeoDB::new_in_memory();
    let schema = db.schema();
    match schema {
        grafeo_engine::SchemaInfo::Lpg(lpg) => {
            assert!(lpg.labels.is_empty(), "No labels in empty db");
            assert!(lpg.edge_types.is_empty(), "No edge types in empty db");
        }
        _ => panic!("Expected LPG schema for in-memory db"),
    }
}

#[test]
fn test_info_empty_database() {
    let db = GrafeoDB::new_in_memory();
    let info = db.info();
    assert_eq!(info.node_count, 0);
    assert_eq!(info.edge_count, 0);
    assert!(!info.is_persistent);
}

// ============================================================================
// Edge Cases - Large IDs and Boundary Values
// ============================================================================

#[test]
fn test_property_with_null_value() {
    let db = GrafeoDB::new_in_memory();
    let n = db.create_node(&["Test"]);
    db.set_node_property(n, "key", Value::Null);

    let node = db.get_node(n).unwrap();
    assert_eq!(
        node.get_property("key"),
        Some(&Value::Null),
        "Should store Null value"
    );
}

#[test]
fn test_property_with_empty_string() {
    let db = GrafeoDB::new_in_memory();
    let n = db.create_node(&["Test"]);
    db.set_node_property(n, "key", Value::String("".into()));

    let node = db.get_node(n).unwrap();
    assert_eq!(
        node.get_property("key"),
        Some(&Value::String("".into())),
        "Should store empty string"
    );
}

#[test]
fn test_node_with_no_labels() {
    let db = GrafeoDB::new_in_memory();
    let n = db.create_node(&[]);

    let node = db.get_node(n);
    assert!(node.is_some(), "Node with no labels should exist");
    assert_eq!(db.node_count(), 1);
}

#[test]
fn test_node_with_many_labels() {
    let db = GrafeoDB::new_in_memory();
    let labels: Vec<&str> = (0..10)
        .map(|i| match i {
            0 => "A",
            1 => "B",
            2 => "C",
            3 => "D",
            4 => "E",
            5 => "F",
            6 => "G",
            7 => "H",
            8 => "I",
            _ => "J",
        })
        .collect();
    let n = db.create_node(&labels);

    let stored_labels = db.get_node_labels(n).unwrap();
    assert_eq!(stored_labels.len(), 10);
}

#[test]
fn test_self_referencing_edge() {
    let db = GrafeoDB::new_in_memory();
    let n = db.create_node(&["Node"]);
    let e = db.create_edge(n, n, "SELF");

    let edge = db.get_edge(e).unwrap();
    assert_eq!(edge.src, n);
    assert_eq!(edge.dst, n);
}

// ============================================================================
// Query Language Edge Cases
// ============================================================================

#[test]
fn test_insert_and_match_special_characters_in_properties() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session
        .execute("INSERT (:Test {name: 'Hello World'})")
        .unwrap();

    let result = session.execute("MATCH (t:Test) RETURN t.name").unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn test_match_with_multiple_labels() {
    let db = GrafeoDB::new_in_memory();
    let n = db.create_node(&["Person", "Employee"]);
    db.set_node_property(n, "name", Value::String("Alice".into()));

    let session = db.session();
    let result = session.execute("MATCH (n:Person) RETURN n.name").unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn test_in_operator_with_empty_list() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    db.create_node(&["Person"]);

    let result = session
        .execute("MATCH (n:Person) WHERE n.name IN [] RETURN n")
        .unwrap();
    assert_eq!(
        result.row_count(),
        0,
        "IN with empty list should match nothing"
    );
}
