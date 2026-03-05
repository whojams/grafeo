//! Backward compatibility tests for snapshot format stability.
//!
//! These tests read a pinned v1 snapshot fixture generated from the current
//! format and verify that future code changes don't break deserialization.
//! If the snapshot format changes, these tests will fail — signaling that
//! a migration path or version bump is needed.

use grafeo_common::types::Value;
use grafeo_engine::GrafeoDB;

/// Pinned v1 snapshot containing:
/// - 3 nodes: Alix (Person), Gus (Person+Employee), Acme Corp (Company)
/// - 2 edges: Alix -[KNOWS {since: 2020}]-> Gus, Gus -[WORKS_AT {role: "Engineer"}]-> Acme Corp
const V1_SNAPSHOT: &[u8] = include_bytes!("fixtures/snapshot_v1.bin");

#[test]
fn read_v1_snapshot_preserves_node_count() {
    let db = GrafeoDB::import_snapshot(V1_SNAPSHOT).unwrap();
    assert_eq!(db.node_count(), 3);
}

#[test]
fn read_v1_snapshot_preserves_edge_count() {
    let db = GrafeoDB::import_snapshot(V1_SNAPSHOT).unwrap();
    assert_eq!(db.edge_count(), 2);
}

#[test]
fn read_v1_snapshot_preserves_labels() {
    let db = GrafeoDB::import_snapshot(V1_SNAPSHOT).unwrap();
    let session = db.session();

    let persons = session
        .execute("MATCH (p:Person) RETURN p.name ORDER BY p.name")
        .unwrap();
    assert_eq!(persons.rows.len(), 2);
    assert_eq!(persons.rows[0][0], Value::String("Alix".into()));
    assert_eq!(persons.rows[1][0], Value::String("Gus".into()));

    let companies = session.execute("MATCH (c:Company) RETURN c.name").unwrap();
    assert_eq!(companies.rows.len(), 1);
    assert_eq!(companies.rows[0][0], Value::String("Acme Corp".into()));
}

#[test]
fn read_v1_snapshot_preserves_node_properties() {
    let db = GrafeoDB::import_snapshot(V1_SNAPSHOT).unwrap();
    let session = db.session();

    let result = session
        .execute("MATCH (p:Person) WHERE p.name = 'Alix' RETURN p.age")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(30));
}

#[test]
fn read_v1_snapshot_preserves_edge_properties() {
    let db = GrafeoDB::import_snapshot(V1_SNAPSHOT).unwrap();
    let session = db.session();

    let knows = session
        .execute("MATCH ()-[e:KNOWS]->() RETURN e.since")
        .unwrap();
    assert_eq!(knows.rows.len(), 1);
    assert_eq!(knows.rows[0][0], Value::Int64(2020));

    let works = session
        .execute("MATCH ()-[e:WORKS_AT]->() RETURN e.role")
        .unwrap();
    assert_eq!(works.rows.len(), 1);
    assert_eq!(works.rows[0][0], Value::String("Engineer".into()));
}

#[test]
fn read_v1_snapshot_preserves_multi_labels() {
    let db = GrafeoDB::import_snapshot(V1_SNAPSHOT).unwrap();
    let session = db.session();

    let employees = session.execute("MATCH (e:Employee) RETURN e.name").unwrap();
    assert_eq!(employees.rows.len(), 1);
    assert_eq!(employees.rows[0][0], Value::String("Gus".into()));
}

#[test]
fn read_v1_snapshot_preserves_edge_traversal() {
    let db = GrafeoDB::import_snapshot(V1_SNAPSHOT).unwrap();
    let session = db.session();

    // Two-hop traversal: Alix -> Gus -> Acme Corp
    let result = session
        .execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company) RETURN a.name, c.name",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
    assert_eq!(result.rows[0][1], Value::String("Acme Corp".into()));
}

#[test]
fn v1_snapshot_round_trip_preserves_data() {
    // Import the fixture, re-export, re-import, and verify data integrity.
    // Property iteration order may differ across import cycles, so we compare
    // semantically rather than byte-for-byte.
    let db = GrafeoDB::import_snapshot(V1_SNAPSHOT).unwrap();
    let re_exported = db.export_snapshot().unwrap();

    let db2 = GrafeoDB::import_snapshot(&re_exported).unwrap();
    assert_eq!(db2.node_count(), 3);
    assert_eq!(db2.edge_count(), 2);

    let session = db2.session();
    let result = session
        .execute("MATCH (p:Person) WHERE p.name = 'Alix' RETURN p.age")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int64(30));
}
