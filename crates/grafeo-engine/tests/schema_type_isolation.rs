//! Tests for schema isolation of types (node, edge, graph types).
//!
//! Verifies that SHOW commands and CREATE/DROP/ALTER type commands
//! respect the current schema set via `SESSION SET SCHEMA`.
//!
//! Fixes: <https://github.com/GrafeoDB/grafeo/issues/167>
//!
//! ```bash
//! cargo test -p grafeo-engine --test schema_type_isolation
//! ```

use grafeo_engine::GrafeoDB;

// ---------------------------------------------------------------------------
// SHOW GRAPH TYPES (primary bug from issue #167)
// ---------------------------------------------------------------------------

#[test]
fn show_graph_types_respects_schema() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // Reproduce issue #167 exactly
    session
        .execute("CREATE SCHEMA IF NOT EXISTS my_schema")
        .unwrap();
    session
        .execute(
            "CREATE GRAPH TYPE IF NOT EXISTS social_network (
                NODE TYPE Person (name STRING NOT NULL, age INTEGER),
                EDGE TYPE KNOWS (since INTEGER)
            )",
        )
        .unwrap();

    // Default schema sees the graph type
    let result = session.execute("SHOW GRAPH TYPES").unwrap();
    assert_eq!(
        result.rows.len(),
        1,
        "default schema should see 1 graph type"
    );

    // Switch to a different schema
    session
        .execute("CREATE SCHEMA IF NOT EXISTS my_schema2")
        .unwrap();
    session.execute("SESSION SET SCHEMA my_schema2").unwrap();

    // my_schema2 should see no graph types
    let result = session.execute("SHOW GRAPH TYPES").unwrap();
    assert_eq!(
        result.rows.len(),
        0,
        "my_schema2 should see 0 graph types (issue #167)"
    );
}

// ---------------------------------------------------------------------------
// SHOW NODE TYPES
// ---------------------------------------------------------------------------

#[test]
fn show_node_types_respects_schema() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.execute("CREATE SCHEMA IF NOT EXISTS s1").unwrap();
    session.execute("SESSION SET SCHEMA s1").unwrap();
    session
        .execute("CREATE NODE TYPE Person (name STRING NOT NULL)")
        .unwrap();

    // Visible in s1
    let result = session.execute("SHOW NODE TYPES").unwrap();
    assert_eq!(result.rows.len(), 1);

    // Not visible in default schema
    session.execute("SESSION RESET SCHEMA").unwrap();
    let result = session.execute("SHOW NODE TYPES").unwrap();
    assert_eq!(
        result.rows.len(),
        0,
        "default schema should not see s1 types"
    );
}

// ---------------------------------------------------------------------------
// SHOW EDGE TYPES
// ---------------------------------------------------------------------------

#[test]
fn show_edge_types_respects_schema() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.execute("CREATE SCHEMA IF NOT EXISTS s1").unwrap();
    session.execute("SESSION SET SCHEMA s1").unwrap();
    session
        .execute("CREATE EDGE TYPE KNOWS (since INTEGER)")
        .unwrap();

    let result = session.execute("SHOW EDGE TYPES").unwrap();
    assert_eq!(result.rows.len(), 1);

    session.execute("SESSION RESET SCHEMA").unwrap();
    let result = session.execute("SHOW EDGE TYPES").unwrap();
    assert_eq!(
        result.rows.len(),
        0,
        "default schema should not see s1 edge types"
    );
}

// ---------------------------------------------------------------------------
// Type isolation between schemas
// ---------------------------------------------------------------------------

#[test]
fn types_isolated_between_schemas() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session
        .execute("CREATE SCHEMA IF NOT EXISTS alpha")
        .unwrap();
    session.execute("CREATE SCHEMA IF NOT EXISTS beta").unwrap();

    // Create same-named type in both schemas
    session.execute("SESSION SET SCHEMA alpha").unwrap();
    session
        .execute("CREATE NODE TYPE Item (color STRING)")
        .unwrap();

    session.execute("SESSION SET SCHEMA beta").unwrap();
    session
        .execute("CREATE NODE TYPE Item (weight FLOAT64)")
        .unwrap();

    // Each schema sees exactly one
    session.execute("SESSION SET SCHEMA alpha").unwrap();
    let result = session.execute("SHOW NODE TYPES").unwrap();
    assert_eq!(result.rows.len(), 1);

    session.execute("SESSION SET SCHEMA beta").unwrap();
    let result = session.execute("SHOW NODE TYPES").unwrap();
    assert_eq!(result.rows.len(), 1);

    // Default schema sees none
    session.execute("SESSION RESET SCHEMA").unwrap();
    let result = session.execute("SHOW NODE TYPES").unwrap();
    assert_eq!(result.rows.len(), 0);
}

// ---------------------------------------------------------------------------
// Default schema types hidden in named schema
// ---------------------------------------------------------------------------

#[test]
fn default_schema_types_hidden_in_named_schema() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // Create type in default schema
    session
        .execute("CREATE NODE TYPE GlobalType (value STRING)")
        .unwrap();

    let result = session.execute("SHOW NODE TYPES").unwrap();
    assert_eq!(result.rows.len(), 1);

    // Switch to named schema: default types not visible
    session
        .execute("CREATE SCHEMA IF NOT EXISTS isolated")
        .unwrap();
    session.execute("SESSION SET SCHEMA isolated").unwrap();
    let result = session.execute("SHOW NODE TYPES").unwrap();
    assert_eq!(
        result.rows.len(),
        0,
        "named schema should not see default types"
    );
}

// ---------------------------------------------------------------------------
// DROP type respects schema
// ---------------------------------------------------------------------------

#[test]
fn drop_type_respects_schema() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.execute("CREATE SCHEMA IF NOT EXISTS s1").unwrap();
    session.execute("SESSION SET SCHEMA s1").unwrap();
    session
        .execute("CREATE NODE TYPE Temp (val STRING)")
        .unwrap();

    let result = session.execute("SHOW NODE TYPES").unwrap();
    assert_eq!(result.rows.len(), 1);

    session.execute("DROP NODE TYPE Temp").unwrap();
    let result = session.execute("SHOW NODE TYPES").unwrap();
    assert_eq!(result.rows.len(), 0);
}

// ---------------------------------------------------------------------------
// DROP SCHEMA blocks when types exist
// ---------------------------------------------------------------------------

#[test]
fn drop_schema_blocks_when_types_exist() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session
        .execute("CREATE SCHEMA IF NOT EXISTS blocker")
        .unwrap();
    session.execute("SESSION SET SCHEMA blocker").unwrap();
    session
        .execute("CREATE NODE TYPE Pinned (val STRING)")
        .unwrap();

    // Dropping should fail because types exist
    session.execute("SESSION RESET SCHEMA").unwrap();
    let result = session.execute("DROP SCHEMA blocker");
    assert!(result.is_err(), "DROP SCHEMA should fail when types exist");
}

// ---------------------------------------------------------------------------
// ALTER type respects schema
// ---------------------------------------------------------------------------

#[test]
fn alter_type_respects_schema() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.execute("CREATE SCHEMA IF NOT EXISTS s1").unwrap();
    session.execute("SESSION SET SCHEMA s1").unwrap();
    session
        .execute("CREATE NODE TYPE Mutable (name STRING)")
        .unwrap();
    session
        .execute("ALTER NODE TYPE Mutable ADD PROPERTY extra STRING")
        .unwrap();

    // Verify the type is still visible and was altered in s1
    let result = session.execute("SHOW NODE TYPES").unwrap();
    assert_eq!(result.rows.len(), 1);

    // Not visible in default schema (isolation preserved after alter)
    session.execute("SESSION RESET SCHEMA").unwrap();
    let result = session.execute("SHOW NODE TYPES").unwrap();
    assert_eq!(
        result.rows.len(),
        0,
        "altered schema type should not leak to default"
    );
}
