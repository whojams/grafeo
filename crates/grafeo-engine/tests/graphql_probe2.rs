//! GraphQL Edge Case Probe Tests
//!
//! Tests 10 specific edge cases for GraphQL query execution.
//!
//! Run with:
//! ```bash
//! cargo test -p grafeo-engine --features graphql --test graphql_probe2
//! ```

#![cfg(feature = "graphql")]

use grafeo_common::types::Value;
use grafeo_engine::GrafeoDB;

// ============================================================================
// Test Fixture
// ============================================================================

/// Creates the social network graph for all probe tests.
///
/// Nodes:
/// - Alix (Person, age: 30, city: "Amsterdam")
/// - Gus (Person, age: 25, city: "Berlin")
/// - Vincent (Person, age: 35, city: "Paris")
/// - Acme (Company, name: "Acme")
///
/// Edges:
/// - Alix -KNOWS-> Gus
/// - Gus -KNOWS-> Vincent
fn create_social_network() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let alix = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Alix".into())),
            ("age", Value::Int64(30)),
            ("city", Value::String("Amsterdam".into())),
        ],
    );
    let gus = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Gus".into())),
            ("age", Value::Int64(25)),
            ("city", Value::String("Berlin".into())),
        ],
    );
    let vincent = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Vincent".into())),
            ("age", Value::Int64(35)),
            ("city", Value::String("Paris".into())),
        ],
    );
    let _acme =
        session.create_node_with_props(&["Company"], [("name", Value::String("Acme".into()))]);

    session.create_edge(alix, gus, "KNOWS");
    session.create_edge(gus, vincent, "KNOWS");

    db
}

// ============================================================================
// 1. Aliased root field
// ============================================================================

#[test]
fn probe_01_aliased_root_field() {
    let db = create_social_network();
    let result = db.execute_graphql("{ people: person { name } }");
    match result {
        Ok(r) => {
            println!(
                "PROBE 01 PASS: aliased root field returned {} rows, columns: {:?}",
                r.row_count(),
                r.columns
            );
            assert_eq!(r.row_count(), 3, "Should find 3 Person nodes via alias");
        }
        Err(e) => {
            panic!("PROBE 01 FAIL: aliased root field error: {e}");
        }
    }
}

// ============================================================================
// 2. Multiple operations with operation name
// ============================================================================

#[test]
fn probe_02_multiple_operations() {
    let db = create_social_network();
    let query = r#"
        query GetPeople { person { name } }
        query GetCompanies { company { name } }
    "#;
    let result = db.execute_graphql(query);
    match result {
        Ok(r) => {
            println!(
                "PROBE 02 PASS: multiple operations returned {} rows, columns: {:?}",
                r.row_count(),
                r.columns
            );
            // If it picks the first operation, we should get 3 Person rows
            // If it picks the last, we get 1 Company row
            println!(
                "PROBE 02 INFO: row_count={}, first row={:?}",
                r.row_count(),
                r.rows.first()
            );
        }
        Err(e) => {
            println!("PROBE 02 FAIL: multiple operations error: {e}");
            panic!("PROBE 02 FAIL: {e}");
        }
    }
}

// ============================================================================
// 3. orderBy with multiple fields
// ============================================================================

#[test]
fn probe_03_order_by_multiple_fields() {
    let db = create_social_network();
    let result =
        db.execute_graphql(r#"{ person(orderBy: { city: ASC, age: DESC }) { name city age } }"#);
    match result {
        Ok(r) => {
            println!(
                "PROBE 03 PASS: multi-key orderBy returned {} rows",
                r.row_count()
            );
            for (i, row) in r.rows.iter().enumerate() {
                println!("  row {i}: {:?}", row);
            }
            // Expected order by city ASC then age DESC:
            // Amsterdam(30), Berlin(25), Paris(35)
            assert_eq!(r.row_count(), 3);
            let names: Vec<&str> = r.rows.iter().filter_map(|r| r[0].as_str()).collect();
            println!("PROBE 03 INFO: name order = {:?}", names);
            // Since cities are all unique, secondary sort does not change anything,
            // so order should be: Alix (Amsterdam), Gus (Berlin), Vincent (Paris)
            assert_eq!(names, vec!["Alix", "Gus", "Vincent"]);
        }
        Err(e) => {
            panic!("PROBE 03 FAIL: multi-key orderBy error: {e}");
        }
    }
}

// ============================================================================
// 4. Pagination: skip only (no first/limit)
// ============================================================================

#[test]
fn probe_04_skip_only() {
    let db = create_social_network();
    let result = db.execute_graphql("{ person(skip: 2) { name } }");
    match result {
        Ok(r) => {
            println!(
                "PROBE 04 PASS: skip-only returned {} rows (expected 1)",
                r.row_count()
            );
            assert_eq!(r.row_count(), 1, "skip 2 of 3 should yield 1 result");
        }
        Err(e) => {
            panic!("PROBE 04 FAIL: skip-only error: {e}");
        }
    }
}

// ============================================================================
// 5. where with nested equality (no operator suffix)
// ============================================================================

#[test]
fn probe_05_where_bare_equality() {
    let db = create_social_network();
    let result = db.execute_graphql(r#"{ person(where: { age: 30 }) { name } }"#);
    match result {
        Ok(r) => {
            println!(
                "PROBE 05 PASS: where bare equality returned {} rows",
                r.row_count()
            );
            let names: Vec<&str> = r.rows.iter().filter_map(|r| r[0].as_str()).collect();
            println!("PROBE 05 INFO: matched names = {:?}", names);
            assert_eq!(r.row_count(), 1, "Only Alix has age 30");
            assert_eq!(names, vec!["Alix"]);
        }
        Err(e) => {
            panic!("PROBE 05 FAIL: where bare equality error: {e}");
        }
    }
}

// ============================================================================
// 6. Create mutation returns created data
// ============================================================================

#[test]
fn probe_06_create_mutation_returns_data() {
    let db = GrafeoDB::new_in_memory();
    let result =
        db.execute_graphql(r#"mutation { createPerson(name: "Test", age: 99) { name age } }"#);
    match result {
        Ok(r) => {
            println!(
                "PROBE 06: create mutation returned {} rows, columns: {:?}",
                r.row_count(),
                r.columns
            );
            for (i, row) in r.rows.iter().enumerate() {
                println!("  row {i}: {:?}", row);
            }
            if r.row_count() >= 1 {
                println!("PROBE 06 PASS: create mutation returned data");
                // Check that the returned data contains the created values
                let has_test = r
                    .rows
                    .iter()
                    .any(|row| row.iter().any(|v| v.as_str() == Some("Test")));
                println!("PROBE 06 INFO: returned row contains 'Test': {has_test}");
            } else {
                println!("PROBE 06 INFO: create mutation returned 0 rows (no inline result)");
            }
        }
        Err(e) => {
            panic!("PROBE 06 FAIL: create mutation error: {e}");
        }
    }
}

// ============================================================================
// 7. Update mutation returns updated data
// ============================================================================

#[test]
fn probe_07_update_mutation_returns_data() {
    let db = GrafeoDB::new_in_memory();
    // Create first
    db.execute_graphql(r#"mutation { createPerson(name: "Mia", age: 28) { name } }"#)
        .expect("create should succeed");

    // Update
    let result =
        db.execute_graphql(r#"mutation { updatePerson(name: "Mia", age: 29) { name age } }"#);
    match result {
        Ok(r) => {
            println!(
                "PROBE 07: update mutation returned {} rows, columns: {:?}",
                r.row_count(),
                r.columns
            );
            for (i, row) in r.rows.iter().enumerate() {
                println!("  row {i}: {:?}", row);
            }
            if r.row_count() >= 1 {
                println!("PROBE 07 PASS: update mutation returned data");
                let has_29 = r
                    .rows
                    .iter()
                    .any(|row| row.iter().any(|v| matches!(v, Value::Int64(29))));
                println!("PROBE 07 INFO: returned row contains age=29: {has_29}");
            } else {
                println!("PROBE 07 INFO: update mutation returned 0 rows");
            }

            // Verify update took effect
            let check = db.execute_graphql("{ person(age: 29) { name } }").unwrap();
            assert_eq!(check.row_count(), 1, "Mia should have age 29 after update");
        }
        Err(e) => {
            panic!("PROBE 07 FAIL: update mutation error: {e}");
        }
    }
}

// ============================================================================
// 8. Delete mutation on nonexistent node
// ============================================================================

#[test]
fn probe_08_delete_nonexistent() {
    let db = create_social_network();
    let result = db.execute_graphql(r#"mutation { deletePerson(name: "NonExistent") }"#);
    match result {
        Ok(r) => {
            println!(
                "PROBE 08 PASS (silent success): delete nonexistent returned {} rows, status: {:?}",
                r.row_count(),
                r.status_message
            );
        }
        Err(e) => {
            println!("PROBE 08 RESULT (error): delete nonexistent errored: {e}");
            // This is also a valid behavior, report it
            panic!("PROBE 08 FAIL: delete nonexistent returned error: {e}");
        }
    }
}

// ============================================================================
// 9. Nested traversal with alias on relationship field
// ============================================================================

#[test]
fn probe_09_nested_alias_on_relationship() {
    let db = create_social_network();
    let result = db.execute_graphql(r#"{ person(name: "Alix") { name friends: knows { name } } }"#);
    match result {
        Ok(r) => {
            println!(
                "PROBE 09 PASS: nested alias returned {} rows, columns: {:?}",
                r.row_count(),
                r.columns
            );
            for (i, row) in r.rows.iter().enumerate() {
                println!("  row {i}: {:?}", row);
            }
            assert!(
                r.row_count() >= 1,
                "Alix knows Gus, so at least 1 row expected"
            );
            // Check if the alias column name is present
            let has_friends_col = r.columns.iter().any(|c| c.contains("friends"));
            println!("PROBE 09 INFO: has 'friends' column: {has_friends_col}");
        }
        Err(e) => {
            panic!("PROBE 09 FAIL: nested alias on relationship error: {e}");
        }
    }
}

// ============================================================================
// 10. where combined with direct args
// ============================================================================

#[test]
fn probe_10_where_combined_with_direct_args() {
    let db = create_social_network();
    let result =
        db.execute_graphql(r#"{ person(age_gt: 20, where: { city: "Amsterdam" }) { name } }"#);
    match result {
        Ok(r) => {
            println!(
                "PROBE 10 PASS: combined args returned {} rows",
                r.row_count()
            );
            let names: Vec<&str> = r.rows.iter().filter_map(|r| r[0].as_str()).collect();
            println!("PROBE 10 INFO: matched names = {:?}", names);
            // age_gt: 20 matches all 3, city: Amsterdam matches Alix only
            assert_eq!(
                r.row_count(),
                1,
                "Only Alix has age > 20 AND city Amsterdam"
            );
            assert_eq!(names, vec!["Alix"]);
        }
        Err(e) => {
            panic!("PROBE 10 FAIL: combined where + direct args error: {e}");
        }
    }
}
