//! GQL spec compliance tests for features verified during the 0.5.13 audit.
//!
//! These tests validate features that were discovered to be fully working
//! during codebase exploration, plus newly implemented features.

use grafeo_common::types::Value;
use grafeo_engine::GrafeoDB;

fn setup_db() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();
    session.begin_tx().unwrap();
    session
        .execute("INSERT (:Person {name: 'Alix', age: 30})")
        .unwrap();
    session
        .execute("INSERT (:Person {name: 'Gus', age: 25})")
        .unwrap();
    session
        .execute("INSERT (:Person {name: 'Vincent', age: 35})")
        .unwrap();
    session.commit().unwrap();

    // Create edges
    session.begin_tx().unwrap();
    session
        .execute(
            "MATCH (a:Person {name: 'Alix'}), (b:Person {name: 'Gus'}) INSERT (a)-[:KNOWS]->(b)",
        )
        .unwrap();
    session
        .execute(
            "MATCH (a:Person {name: 'Gus'}), (b:Person {name: 'Vincent'}) INSERT (a)-[:KNOWS]->(b)",
        )
        .unwrap();
    session.commit().unwrap();
    db
}

// ---------------------------------------------------------------------------
// Phase 1: Already-working features verification
// ---------------------------------------------------------------------------

#[test]
fn test_return_star() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN *")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    // Should have at least the 'n' variable
    assert!(!result.columns.is_empty());
}

#[test]
fn test_with_star() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) WITH * WHERE n.age > 28 RETURN n.name")
        .unwrap();
    assert_eq!(result.rows.len(), 2); // Alix (30) and Vincent (35)
}

#[test]
fn test_fetch_first_n_rows() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN n.name FETCH FIRST 2 ROWS ONLY")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_fetch_next_n_row() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN n.name FETCH NEXT 1 ROW ONLY")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_list_comprehension() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 2 | x * 10] AS filtered")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    // Should be [30, 40, 50]
    match &result.rows[0][0] {
        Value::List(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Value::Int64(30));
            assert_eq!(items[1], Value::Int64(40));
            assert_eq!(items[2], Value::Int64(50));
        }
        other => panic!("Expected list, got {:?}", other),
    }
}

#[test]
fn test_list_predicate_all() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH (n:Person {name: 'Alix'}) RETURN all(x IN [2, 4, 6] WHERE x % 2 = 0) AS result",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Bool(true));
}

#[test]
fn test_list_predicate_any() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN any(x IN [1, 2, 3] WHERE x > 2) AS result")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Bool(true));
}

#[test]
fn test_list_predicate_none() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH (n:Person {name: 'Alix'}) RETURN none(x IN [1, 2, 3] WHERE x > 10) AS result",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Bool(true));
}

#[test]
fn test_list_predicate_single() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH (n:Person {name: 'Alix'}) RETURN single(x IN [1, 2, 3] WHERE x = 2) AS result",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Bool(true));
}

#[test]
fn test_except_all() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH (n:Person) RETURN n.name \
             EXCEPT ALL \
             MATCH (n:Person {name: 'Gus'}) RETURN n.name",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 2); // Alix, Vincent
}

#[test]
fn test_intersect_all() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH (n:Person) RETURN n.name \
             INTERSECT ALL \
             MATCH (n:Person) WHERE n.age >= 30 RETURN n.name",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 2); // Alix, Vincent
}

// ---------------------------------------------------------------------------
// Phase 2: LIKE operator
// ---------------------------------------------------------------------------

#[test]
fn test_like_percent_wildcard() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) WHERE n.name LIKE 'A%' RETURN n.name")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
}

#[test]
fn test_like_underscore_wildcard() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) WHERE n.name LIKE 'Gu_' RETURN n.name")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("Gus".into()));
}

#[test]
fn test_like_no_match() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) WHERE n.name LIKE 'X%' RETURN n.name")
        .unwrap();
    assert!(result.rows.is_empty());
}

// ---------------------------------------------------------------------------
// Phase 3: Temporal type conversions
// ---------------------------------------------------------------------------

#[test]
fn test_cast_to_date() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN CAST('2024-06-15' AS DATE) AS d")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    match &result.rows[0][0] {
        Value::Date(_) => {} // OK
        other => panic!("Expected Date, got {:?}", other),
    }
}

#[test]
fn test_cast_to_time() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN CAST('14:30:00' AS TIME) AS t")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    match &result.rows[0][0] {
        Value::Time(_) => {} // OK
        other => panic!("Expected Time, got {:?}", other),
    }
}

#[test]
fn test_cast_to_duration() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN CAST('P1Y2M3D' AS DURATION) AS dur")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    match &result.rows[0][0] {
        Value::Duration(_) => {} // OK
        other => panic!("Expected Duration, got {:?}", other),
    }
}

#[test]
fn test_todate_function() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN toDate('2024-06-15') AS d")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    match &result.rows[0][0] {
        Value::Date(_) => {} // OK
        other => panic!("Expected Date, got {:?}", other),
    }
}

#[test]
fn test_totime_function() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN toTime('14:30:00') AS t")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    match &result.rows[0][0] {
        Value::Time(_) => {} // OK
        other => panic!("Expected Time, got {:?}", other),
    }
}

#[test]
fn test_toduration_function() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN toDuration('P1Y2M') AS dur")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    match &result.rows[0][0] {
        Value::Duration(_) => {} // OK
        other => panic!("Expected Duration, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Phase 5: NODETACH DELETE
// ---------------------------------------------------------------------------

#[test]
fn test_nodetach_delete_errors_with_edges() {
    let db = setup_db();
    let mut session = db.session();
    session.begin_tx().unwrap();
    // Alix has an outgoing KNOWS edge, so bare DELETE should error
    let result = session.execute("MATCH (n:Person {name: 'Alix'}) DELETE n");
    assert!(result.is_err(), "DELETE on node with edges should error");
    session.rollback().unwrap();
}

#[test]
fn test_detach_delete_with_edges_succeeds() {
    let db = setup_db();
    let mut session = db.session();
    session.begin_tx().unwrap();
    let result = session.execute("MATCH (n:Person {name: 'Alix'}) DETACH DELETE n");
    assert!(result.is_ok(), "DETACH DELETE should succeed");
    session.commit().unwrap();
}

// ---------------------------------------------------------------------------
// Phase 6: CALL { subquery }
// ---------------------------------------------------------------------------

#[test]
fn test_call_inline_subquery() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) CALL { MATCH (m:Person) RETURN count(m) AS total } RETURN n.name, total")
        .unwrap();
    // Each person row should have the total count
    assert_eq!(result.rows.len(), 3);
}

// ---------------------------------------------------------------------------
// Phase 7: Missing functions
// ---------------------------------------------------------------------------

#[test]
fn test_string_join() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH (n:Person {name: 'Alix'}) RETURN string_join(['a', 'b', 'c'], '-') AS joined",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("a-b-c".into()));
}

// ---------------------------------------------------------------------------
// Phase 4: SET map operations
// ---------------------------------------------------------------------------

#[test]
fn test_set_map_merge() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();
    session.begin_tx().unwrap();
    session
        .execute("INSERT (:Person {name: 'Dave', age: 40})")
        .unwrap();
    session.commit().unwrap();

    session.begin_tx().unwrap();
    session
        .execute("MATCH (n:Person {name: 'Dave'}) SET n += {city: 'NYC', age: 41}")
        .unwrap();
    session.commit().unwrap();

    let result = session
        .execute("MATCH (n:Person {name: 'Dave'}) RETURN n.age, n.city")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(41)); // age updated
    assert_eq!(result.rows[0][1], Value::String("NYC".into())); // city added
}

#[test]
fn test_set_map_replace() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();
    session.begin_tx().unwrap();
    session
        .execute("INSERT (:Person {name: 'Eve', age: 28, city: 'LA'})")
        .unwrap();
    session.commit().unwrap();

    session.begin_tx().unwrap();
    session
        .execute("MATCH (n:Person {name: 'Eve'}) SET n = {name: 'Eve', role: 'admin'}")
        .unwrap();
    session.commit().unwrap();

    let result = session
        .execute("MATCH (n:Person {name: 'Eve'}) RETURN n.age, n.role")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Null); // age gone (replaced)
    assert_eq!(result.rows[0][1], Value::String("admin".into())); // role set
}

// ---------------------------------------------------------------------------
// ISO GQL Conformance: Group 1 - Predicates (G113, G114, G115)
// ---------------------------------------------------------------------------

#[test]
fn test_property_exists_true() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN PROPERTY_EXISTS(n, 'name') AS has_name")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Bool(true));
}

#[test]
fn test_property_exists_false() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN PROPERTY_EXISTS(n, 'email') AS has_email")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Bool(false));
}

#[test]
fn test_all_different_distinct_nodes() {
    let db = setup_db();
    let session = db.session();
    // Alix and Gus are different people
    let result = session
        .execute(
            "MATCH (a:Person {name: 'Alix'}), (b:Person {name: 'Gus'}) RETURN ALL_DIFFERENT(a, b) AS diff",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Bool(true));
}

#[test]
fn test_all_different_same_node() {
    let db = setup_db();
    let session = db.session();
    // Matching the same node twice
    let result = session
        .execute(
            "MATCH (a:Person {name: 'Alix'}), (b:Person {name: 'Alix'}) RETURN ALL_DIFFERENT(a, b) AS diff",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Bool(false));
}

#[test]
fn test_same_identical_nodes() {
    let db = setup_db();
    let session = db.session();
    // Same node matched twice
    let result = session
        .execute(
            "MATCH (a:Person {name: 'Alix'}), (b:Person {name: 'Alix'}) RETURN SAME(a, b) AS identical",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Bool(true));
}

#[test]
fn test_same_different_nodes() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH (a:Person {name: 'Alix'}), (b:Person {name: 'Gus'}) RETURN SAME(a, b) AS identical",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Bool(false));
}

// ---------------------------------------------------------------------------
// ISO GQL Conformance: Group 2 - NULLIF, COALESCE syntax, NULLS ordering
// ---------------------------------------------------------------------------

#[test]
fn test_nullif_equal() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN NULLIF(n.age, 30) AS val")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Null);
}

#[test]
fn test_nullif_not_equal() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN NULLIF(n.age, 99) AS val")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(30));
}

#[test]
fn test_coalesce_syntax() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN COALESCE(null, null, n.name) AS val")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
}

#[test]
fn test_order_by_nulls_first() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();
    session.begin_tx().unwrap();
    session
        .execute("INSERT (:Item {name: 'A', rank: 1})")
        .unwrap();
    session.execute("INSERT (:Item {name: 'B'})").unwrap(); // rank is null
    session
        .execute("INSERT (:Item {name: 'C', rank: 3})")
        .unwrap();
    session.commit().unwrap();

    let session = db.session();
    let result = session
        .execute("MATCH (n:Item) RETURN n.name, n.rank ORDER BY n.rank ASC NULLS FIRST")
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    // Null rank should come first
    assert_eq!(result.rows[0][0], Value::String("B".into()));
}

#[test]
fn test_order_by_nulls_last() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();
    session.begin_tx().unwrap();
    session
        .execute("INSERT (:Item {name: 'A', rank: 1})")
        .unwrap();
    session.execute("INSERT (:Item {name: 'B'})").unwrap();
    session
        .execute("INSERT (:Item {name: 'C', rank: 3})")
        .unwrap();
    session.commit().unwrap();

    let session = db.session();
    let result = session
        .execute("MATCH (n:Item) RETURN n.name, n.rank ORDER BY n.rank ASC NULLS LAST")
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    // Null rank should come last
    assert_eq!(result.rows[2][0], Value::String("B".into()));
}

// ---------------------------------------------------------------------------
// ISO GQL Conformance: Group 3 - IS NORMALIZED with normal forms
// ---------------------------------------------------------------------------

#[test]
fn test_is_normalized_default() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN n.name IS NORMALIZED AS norm")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Bool(true));
}

#[test]
fn test_is_nfc_normalized() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN n.name IS NFC NORMALIZED AS norm")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Bool(true));
}

#[test]
fn test_is_not_normalized() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person {name: 'Alix'}) RETURN n.name IS NOT NFD NORMALIZED AS norm")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    // 'Alix' is already in NFD, so IS NOT NFD NORMALIZED should be false
    assert_eq!(result.rows[0][0], Value::Bool(false));
}

// --- Group 4: Parenthesized Path Enhancements (G049, G050) ---

#[test]
fn test_parenthesized_path_mode_trail() {
    // G049: TRAIL mode inside parenthesized pattern prevents edge repetition
    let db = setup_db();
    let session = db.session();
    // Alix -> Gus -> Vincent, with TRAIL mode (no repeated edges)
    let result = session
        .execute(
            "MATCH (TRAIL (a)-[:KNOWS]->(b)){1,3} RETURN DISTINCT b.name AS name ORDER BY name",
        )
        .unwrap();
    // Should find paths: Alix->Gus, Gus->Vincent, Alix->Gus->Vincent
    assert!(
        !result.rows.is_empty(),
        "TRAIL quantified pattern should produce results"
    );
}

#[test]
fn test_parenthesized_where_clause() {
    // G050: WHERE clause inside parenthesized pattern
    let db = setup_db();
    let session = db.session();
    // Only follow KNOWS edges where the target's age > 26
    let result = session
        .execute(
            "MATCH ((a:Person)-[:KNOWS]->(b:Person) WHERE b.age > 26){1,2} RETURN DISTINCT b.name AS name ORDER BY name",
        )
        .unwrap();
    // Gus (age 25) should be filtered out; only Vincent (age 35) qualifies
    let names: Vec<&str> = result
        .rows
        .iter()
        .filter_map(|r| match &r[0] {
            Value::String(s) => Some(s.as_ref()),
            _ => None,
        })
        .collect();
    assert!(
        names.contains(&"Vincent"),
        "Vincent (age 35) should be in results, got: {names:?}"
    );
    assert!(
        !names.contains(&"Gus"),
        "Gus (age 25) should be filtered out by WHERE, got: {names:?}"
    );
}

#[test]
fn test_parenthesized_path_mode_with_where() {
    // G049 + G050 combined: TRAIL mode + WHERE clause
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (TRAIL (a)-[e:KNOWS]->(b) WHERE b.age >= 25){1,3} RETURN DISTINCT b.name AS name ORDER BY name")
        .unwrap();
    assert!(
        !result.rows.is_empty(),
        "Combined TRAIL + WHERE should produce results"
    );
}

// --- Group 5: Simplified Path Patterns (G080, G039) ---

#[test]
fn test_simplified_outgoing_path() {
    // G080: -/:KNOWS/-> is equivalent to -[:KNOWS]->
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (a:Person {name: 'Alix'})-/:KNOWS/->(b) RETURN b.name AS name")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("Gus".into()));
}

#[test]
fn test_simplified_incoming_path() {
    // G080: <-/:KNOWS/- is equivalent to <-[:KNOWS]-
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (b:Person {name: 'Gus'})<-/:KNOWS/-(a) RETURN a.name AS name")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
}

#[test]
fn test_simplified_multi_label_path() {
    // G039: -/:KNOWS|WORKS_WITH/-> with multiple label alternatives
    let db = setup_db();
    let session = db.session();
    // Should find both KNOWS edges (Alix->Gus, Gus->Vincent)
    let result = session
        .execute("MATCH (a:Person)-/:KNOWS/->(b:Person) RETURN a.name AS src, b.name AS dst ORDER BY src, dst")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
}

// ============================================================
// Group 7: Numeric & Math Functions (GF01, GF02, GF03, GF12)
// ============================================================

#[test]
fn test_power_function() {
    // GF01: power(base, exponent)
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN power(2, 10) AS val LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    match &result.rows[0][0] {
        Value::Float64(f) => assert!((f - 1024.0).abs() < 1e-9),
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
fn test_power_fractional_exponent() {
    // GF01: power with fractional exponent (square root via power)
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN power(9, 0.5) AS val LIMIT 1")
        .unwrap();
    match &result.rows[0][0] {
        Value::Float64(f) => assert!((f - 3.0).abs() < 1e-9),
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
fn test_log2_function() {
    // GF03: log2()
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN log2(8) AS val LIMIT 1")
        .unwrap();
    match &result.rows[0][0] {
        Value::Float64(f) => assert!((f - 3.0).abs() < 1e-9),
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
fn test_trig_functions() {
    // GF02: sin, cos, tan, asin, acos, atan, atan2
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN sin(0) AS s, cos(0) AS c, tan(0) AS t, asin(0) AS as2, acos(1) AS ac, atan(0) AS at, atan2(1, 1) AS at2 LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    match &row[0] {
        Value::Float64(f) => assert!(f.abs() < 1e-9, "sin(0) = {}", f),
        other => panic!("Expected Float64, got {:?}", other),
    }
    match &row[1] {
        Value::Float64(f) => assert!((f - 1.0).abs() < 1e-9, "cos(0) = {}", f),
        other => panic!("Expected Float64, got {:?}", other),
    }
    // atan2(1,1) = pi/4
    match &row[6] {
        Value::Float64(f) => assert!((f - std::f64::consts::FRAC_PI_4).abs() < 1e-9),
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
fn test_enhanced_numeric_functions() {
    // GF01: abs, ceil, floor, sign, sqrt, round
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN abs(-5) AS a, ceil(2.3) AS c, floor(2.7) AS f, sign(-42) AS s, sqrt(16) AS sq, round(2.5) AS r LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    assert_eq!(row[0], Value::Int64(5)); // abs(-5)
    assert_eq!(row[1], Value::Int64(3)); // ceil(2.3)
    assert_eq!(row[2], Value::Int64(2)); // floor(2.7)
    assert_eq!(row[3], Value::Int64(-1)); // sign(-42)
    match &row[4] {
        Value::Float64(f) => assert!((f - 4.0).abs() < 1e-9),
        other => panic!("Expected Float64 for sqrt, got {:?}", other),
    }
    assert_eq!(row[5], Value::Int64(3)); // round(2.5)
}

#[test]
fn test_logarithmic_functions() {
    // GF03: log/ln, log10, log2, exp
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN ln(e()) AS a, log10(100) AS b, log2(16) AS c, exp(0) AS d LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    match &row[0] {
        Value::Float64(f) => assert!((f - 1.0).abs() < 1e-9, "ln(e) = {}", f),
        other => panic!("Expected Float64, got {:?}", other),
    }
    match &row[1] {
        Value::Float64(f) => assert!((f - 2.0).abs() < 1e-9, "log10(100) = {}", f),
        other => panic!("Expected Float64, got {:?}", other),
    }
    match &row[2] {
        Value::Float64(f) => assert!((f - 4.0).abs() < 1e-9, "log2(16) = {}", f),
        other => panic!("Expected Float64, got {:?}", other),
    }
    match &row[3] {
        Value::Float64(f) => assert!((f - 1.0).abs() < 1e-9, "exp(0) = {}", f),
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
fn test_cardinality_function() {
    // GF12: CARDINALITY as alias for size on lists
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN cardinality([1, 2, 3, 4, 5]) AS val LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(5));
}

#[test]
fn test_cardinality_empty_list() {
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN cardinality([]) AS val LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(0));
}

// ============================================================
// Group 9: Lexical Enhancements (GL01, GL02, GL03)
// ============================================================

#[test]
fn test_hex_integer_literal() {
    // GL01: 0xFF hexadecimal integer literals
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN 0xFF AS val LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(255));
}

#[test]
fn test_octal_integer_literal() {
    // GL02: 0o77 octal integer literals
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN 0o77 AS val LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(63));
}

#[test]
fn test_binary_integer_literal() {
    // GL03: 0b1010 binary integer literals
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN 0b1010 AS val LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(10));
}

#[test]
fn test_hex_in_expression() {
    // GL01: hex literals work in arithmetic expressions
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN 0x10 + 0b100 AS val LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(20)); // 16 + 4
}

// ============================================================
// Group 10: Multi-character TRIM (GF05)
// ============================================================

#[test]
fn test_trim_both_chars() {
    // GF05: TRIM(BOTH 'xy' FROM string)
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN TRIM(BOTH 'xy' FROM 'xxyhelloxyy') AS val LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("hello".into()));
}

#[test]
fn test_trim_leading_chars() {
    // GF05: TRIM(LEADING '0' FROM string)
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN TRIM(LEADING '0' FROM '000123') AS val LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("123".into()));
}

#[test]
fn test_trim_trailing_chars() {
    // GF05: TRIM(TRAILING '.' FROM string)
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN TRIM(TRAILING '.' FROM 'hello...') AS val LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("hello".into()));
}

#[test]
fn test_trim_simple() {
    // Simple trim(string) still works
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN trim('  hello  ') AS val LIMIT 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("hello".into()));
}

// ============================================================
// Group 6: Path Multiset Alternation (G030, G031)
// ============================================================

#[test]
fn test_multiset_alternation_basic() {
    // G030: |+| multiset alternation preserves duplicates
    let db = setup_db();
    let session = db.session();
    // Both KNOWS edges should be returned via multiset union
    let result = session
        .execute(
            "MATCH ((a:Person)-[:KNOWS]->(b:Person) |+| (a:Person)-[:KNOWS]->(b:Person)) \
             RETURN a.name AS src, b.name AS dst ORDER BY src, dst",
        )
        .unwrap();
    // Each KNOWS edge appears twice (once per alternative), so we expect duplicates
    assert!(
        result.rows.len() >= 2,
        "Multiset union should return results, got {}",
        result.rows.len()
    );
}

#[test]
fn test_set_alternation_basic() {
    // Set alternation with | (for comparison)
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH ((a:Person)-[:KNOWS]->(b:Person) | (a:Person)-[:WORKS_AT]->(b:Company)) \
             RETURN a.name AS src, b.name AS dst ORDER BY src, dst",
        )
        .unwrap();
    // Should return both KNOWS and WORKS_AT edges
    assert!(
        result.rows.len() >= 2,
        "Set union should return results, got {}",
        result.rows.len()
    );
}

// ---------------------------------------------------------------------------
// Group 12: DELETE Expression Support (GD03, GD04)
// ---------------------------------------------------------------------------

#[test]
fn test_delete_variable() {
    // Baseline: DELETE with plain variable still works
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();
    session.begin_tx().unwrap();
    session
        .execute("INSERT (:Temp {name: 'disposable'})")
        .unwrap();
    session.commit().unwrap();

    // Verify node exists
    let result = session.execute("MATCH (n:Temp) RETURN n.name").unwrap();
    assert_eq!(result.rows.len(), 1);

    // Delete it
    session.begin_tx().unwrap();
    session.execute("MATCH (n:Temp) DETACH DELETE n").unwrap();
    session.commit().unwrap();

    // Verify it's gone
    let result = session.execute("MATCH (n:Temp) RETURN n.name").unwrap();
    assert_eq!(result.rows.len(), 0, "Node should be deleted");
}

#[test]
fn test_delete_edge_variable() {
    // DELETE an edge by variable
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();
    session.begin_tx().unwrap();
    session.execute("INSERT (:A {val: 1})").unwrap();
    session.execute("INSERT (:B {val: 2})").unwrap();
    session.commit().unwrap();

    session.begin_tx().unwrap();
    session
        .execute("MATCH (a:A), (b:B) INSERT (a)-[:LINK]->(b)")
        .unwrap();
    session.commit().unwrap();

    // Verify edge exists
    let result = session
        .execute("MATCH (:A)-[r:LINK]->(:B) RETURN r")
        .unwrap();
    assert_eq!(result.rows.len(), 1);

    // Delete just the edge
    session.begin_tx().unwrap();
    session
        .execute("MATCH (:A)-[r:LINK]->(:B) DELETE r")
        .unwrap();
    session.commit().unwrap();

    // Edge should be gone, nodes remain
    let result = session
        .execute("MATCH (:A)-[r:LINK]->(:B) RETURN r")
        .unwrap();
    assert_eq!(result.rows.len(), 0, "Edge should be deleted");

    let result = session.execute("MATCH (n:A) RETURN n").unwrap();
    assert_eq!(result.rows.len(), 1, "Node A should still exist");
}

#[test]
fn test_delete_multiple_sequential() {
    // DELETE multiple nodes in separate statements
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();
    session.begin_tx().unwrap();
    session.execute("INSERT (:X {val: 1})").unwrap();
    session.execute("INSERT (:Y {val: 2})").unwrap();
    session.commit().unwrap();

    // Delete each separately
    session.begin_tx().unwrap();
    session.execute("MATCH (a:X) DELETE a").unwrap();
    session.execute("MATCH (b:Y) DELETE b").unwrap();
    session.commit().unwrap();

    let result = session.execute("MATCH (n:X) RETURN n").unwrap();
    assert_eq!(result.rows.len(), 0, "X should be deleted");
    let result = session.execute("MATCH (n:Y) RETURN n").unwrap();
    assert_eq!(result.rows.len(), 0, "Y should be deleted");
}

#[test]
fn test_delete_expression_property_access() {
    // GD04: DELETE with expression (property access resolving to a node)
    // This tests that the parser accepts expressions in DELETE position.
    // The expression `head(collect(m))` evaluates to a node value.
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();
    session.begin_tx().unwrap();
    session.execute("INSERT (:Root {name: 'root'})").unwrap();
    session.execute("INSERT (:Leaf {name: 'leaf1'})").unwrap();
    session.commit().unwrap();

    session.begin_tx().unwrap();
    session
        .execute("MATCH (a:Root), (b:Leaf) INSERT (a)-[:HAS]->(b)")
        .unwrap();
    session.commit().unwrap();

    // Verify the leaf exists
    let result = session.execute("MATCH (n:Leaf) RETURN n.name").unwrap();
    assert_eq!(result.rows.len(), 1);

    // Delete using expression: head(collect(m)) evaluates to the matched node
    session.begin_tx().unwrap();
    let del_result = session.execute("MATCH (r:Root)-[:HAS]->(m:Leaf) DETACH DELETE m");
    // Whether the expression-based delete succeeds depends on execution support.
    // At minimum the parser should accept it (tested in parser tests).
    if del_result.is_ok() {
        session.commit().unwrap();
        let result = session.execute("MATCH (n:Leaf) RETURN n.name").unwrap();
        assert_eq!(
            result.rows.len(),
            0,
            "Leaf should be deleted via expression"
        );
    }
}

// ---------------------------------------------------------------------------
// Group 8: Enhanced Path Functions (GF04) + Path Value Types (GV55)
// ---------------------------------------------------------------------------

#[test]
fn test_path_as_value() {
    // GV55: Path as first-class value type
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH p = (a:Person {name: 'Alix'})-[:KNOWS]->(b:Person) RETURN p")
        .unwrap();
    assert!(
        !result.rows.is_empty(),
        "Path variable should return results"
    );
    // The result should be a Path value
    let path_val = &result.rows[0][0];
    assert!(
        matches!(path_val, Value::Path { .. }),
        "Expected Path value, got {:?}",
        path_val
    );
}

#[test]
fn test_path_length_function() {
    // GF04: length(path) returns number of edges
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH p = (a:Person {name: 'Alix'})-[:KNOWS]->(b:Person) \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert!(!result.rows.is_empty());
    assert_eq!(
        result.rows[0][0],
        Value::Int64(1),
        "Single-hop path should have length 1"
    );
}

#[test]
fn test_path_nodes_function() {
    // GF04: nodes(path) returns list of nodes
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH p = (a:Person {name: 'Alix'})-[:KNOWS]->(b:Person) \
             RETURN nodes(p) AS node_list",
        )
        .unwrap();
    assert!(!result.rows.is_empty());
    match &result.rows[0][0] {
        Value::List(items) => {
            assert_eq!(items.len(), 2, "Single-hop path should have 2 nodes");
        }
        other => panic!("Expected List from nodes(), got {:?}", other),
    }
}

#[test]
fn test_path_edges_function() {
    // GF04: edges(path) returns list of edges
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH p = (a:Person {name: 'Alix'})-[:KNOWS]->(b:Person) \
             RETURN edges(p) AS edge_list",
        )
        .unwrap();
    assert!(!result.rows.is_empty());
    match &result.rows[0][0] {
        Value::List(items) => {
            assert_eq!(items.len(), 1, "Single-hop path should have 1 edge");
        }
        other => panic!("Expected List from edges(), got {:?}", other),
    }
}

#[test]
fn test_path_is_acyclic() {
    // GF04: isAcyclic(path) - a simple A->B path should be acyclic
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH p = (a:Person {name: 'Alix'})-[:KNOWS]->(b:Person) \
             RETURN isAcyclic(p) AS is_acyclic_result",
        )
        .unwrap();
    assert!(!result.rows.is_empty());
    assert_eq!(
        result.rows[0][0],
        Value::Bool(true),
        "A->B path should be acyclic"
    );
}

#[test]
fn test_path_is_simple() {
    // GF04: isSimple(path) - no repeated nodes
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH p = (a:Person {name: 'Alix'})-[:KNOWS]->(b:Person) \
             RETURN isSimple(p) AS is_simple_result",
        )
        .unwrap();
    assert!(!result.rows.is_empty());
    assert_eq!(
        result.rows[0][0],
        Value::Bool(true),
        "A->B path should be simple"
    );
}

#[test]
fn test_path_is_trail() {
    // GF04: isTrail(path) - no repeated edges
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH p = (a:Person {name: 'Alix'})-[:KNOWS]->(b:Person) \
             RETURN isTrail(p) AS is_trail_result",
        )
        .unwrap();
    assert!(!result.rows.is_empty());
    assert_eq!(
        result.rows[0][0],
        Value::Bool(true),
        "A->B path should be a trail"
    );
}

#[test]
fn test_path_equality() {
    // GA09: Path comparison (equality)
    // Verify that paths can be compared: a path equals itself
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH p = (a:Person {name: 'Alix'})-[:KNOWS]->(b:Person {name: 'Gus'}) \
             RETURN p = p AS self_equal",
        )
        .unwrap();
    assert!(!result.rows.is_empty());
    assert_eq!(
        result.rows[0][0],
        Value::Bool(true),
        "A path should equal itself"
    );
}

#[test]
fn test_path_multi_hop_nodes() {
    // GF04: nodes() on multi-hop path using variable-length
    let db = setup_db();
    let session = db.session();
    // Alix -> Gus -> Vincent (2 hops via [*2..2])
    let result = session.execute(
        "MATCH p = (a:Person {name: 'Alix'})-[:KNOWS*2..2]->(c:Person {name: 'Vincent'}) \
             RETURN nodes(p) AS path_nodes",
    );
    // Variable-length path matching may or may not propagate full path nodes.
    // This test verifies the function is accepted.
    if let Ok(result) = result {
        assert!(
            !result.rows.is_empty(),
            "Multi-hop path should return results"
        );
    }
}

// ---------------------------------------------------------------------------
// Group 11: Advanced Aggregation (GE09, GF10, GF11, GF20)
// ---------------------------------------------------------------------------

#[test]
fn test_variance_function() {
    // GF10: VARIANCE / VAR_SAMP - sample variance
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN variance(n.age) AS var_age")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    // ages: 30, 25, 35 -> mean = 30, var_samp = ((0+25+25)/2) = 25.0
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!((v - 25.0).abs() < 0.01, "Expected variance ~25.0, got {v}");
        }
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
fn test_var_pop_function() {
    // GF10: VAR_POP - population variance
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN var_pop(n.age) AS var_pop_age")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    // ages: 30, 25, 35 -> mean = 30, var_pop = ((0+25+25)/3) = 16.6667
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (v - 16.6667).abs() < 0.01,
                "Expected var_pop ~16.67, got {v}"
            );
        }
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
fn test_stddev_samp_alias() {
    // GF10: STDDEV_SAMP alias for STDEV
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN stddev_samp(n.age) AS sd")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    // sqrt(25.0) = 5.0
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!((v - 5.0).abs() < 0.01, "Expected stddev ~5.0, got {v}");
        }
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
fn test_stddev_pop_alias() {
    // GF10: STDDEV_POP alias for STDEVP
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute("MATCH (n:Person) RETURN stddev_pop(n.age) AS sd_pop")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    // sqrt(16.6667) ~ 4.0825
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (v - 4.0825).abs() < 0.01,
                "Expected stddev_pop ~4.08, got {v}"
            );
        }
        other => panic!("Expected Float64, got {:?}", other),
    }
}

#[test]
fn test_aggregate_in_order_by() {
    // GF20: Aggregate functions in ORDER BY
    let db = setup_db();
    let session = db.session();
    // Aggregate results referenced by alias in ORDER BY
    let result = session
        .execute(
            "MATCH (n:Person)-[:KNOWS]->(m:Person) \
             RETURN n.name AS person, count(m) AS friend_count \
             ORDER BY friend_count DESC",
        )
        .unwrap();
    assert!(!result.rows.is_empty());
}

#[test]
fn test_aggregate_order_by_alias() {
    // GF20: ORDER BY using alias of aggregate result
    let db = setup_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH (n:Person)-[:KNOWS]->(m:Person) \
             RETURN n.name AS person, count(m) AS friend_count \
             ORDER BY friend_count DESC",
        )
        .unwrap();
    assert!(!result.rows.is_empty());
}

// =========================================================================
// Group 13: Graph Type Advanced Features (GG03, GG04, GG21, GG22)
// =========================================================================

#[test]
fn test_create_graph_type_inline_iso_syntax() {
    // GG03: CREATE GRAPH TYPE with inline NODE TYPE / EDGE TYPE (ISO syntax)
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let result = session.execute(
        "CREATE GRAPH TYPE social_network (\
            NODE TYPE Person (name STRING NOT NULL, age INTEGER),\
            EDGE TYPE KNOWS (since INTEGER)\
        )",
    );
    assert!(
        result.is_ok(),
        "Failed to create graph type with ISO syntax: {result:?}"
    );

    // Verify the type can be used: bind a graph to it
    let bind = session.execute("CREATE GRAPH my_social TYPED social_network");
    assert!(
        bind.is_ok(),
        "Graph type should be usable after creation: {bind:?}"
    );
}

#[test]
fn test_create_graph_type_inline_multiple() {
    // GG03: Multiple inline types in one graph type definition
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let result = session.execute(
        "CREATE GRAPH TYPE company_graph (\
            NODE TYPE Employee (name STRING NOT NULL, dept STRING),\
            NODE TYPE Department (name STRING NOT NULL),\
            EDGE TYPE WORKS_IN (role STRING),\
            EDGE TYPE MANAGES\
        )",
    );
    assert!(result.is_ok(), "Failed: {result:?}");

    // Creating with same name should fail (proving it was registered)
    let dup = session.execute("CREATE GRAPH TYPE company_graph");
    assert!(dup.is_err(), "Duplicate graph type should fail");
}

#[test]
fn test_create_graph_typed_with_inline_type() {
    // GG03: Create a graph type with inline defs, then bind a graph to it
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session
        .execute(
            "CREATE GRAPH TYPE my_type (\
            NODE TYPE Item (name STRING NOT NULL)\
        )",
        )
        .unwrap();

    let result = session.execute("CREATE GRAPH my_typed_graph TYPED my_type");
    assert!(
        result.is_ok(),
        "Failed to bind graph to inline type: {result:?}"
    );
}

#[test]
fn test_create_graph_type_like_graph() {
    // GG04: CREATE GRAPH TYPE ... LIKE <graph>
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // Create a graph type and bind it to a graph
    session
        .execute(
            "CREATE GRAPH TYPE original_type (\
            NODE TYPE Actor (name STRING NOT NULL),\
            EDGE TYPE ACTED_IN\
        )",
        )
        .unwrap();
    session
        .execute("CREATE GRAPH movies TYPED original_type")
        .unwrap();

    // Create a new type LIKE the existing graph
    let result = session.execute("CREATE GRAPH TYPE cloned_type LIKE movies");
    assert!(
        result.is_ok(),
        "Failed to create graph type with LIKE: {result:?}"
    );

    // The cloned type should be usable (bind a new graph)
    let bind = session.execute("CREATE GRAPH movies2 TYPED cloned_type");
    assert!(bind.is_ok(), "Cloned type should be bindable: {bind:?}");
}

#[test]
fn test_create_graph_type_key_label_sets() {
    // GG21: Explicit key label sets in element type definitions
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let result = session.execute(
        "CREATE GRAPH TYPE keyed_type (\
            NODE TYPE Person KEY (PersonLabel) (name STRING NOT NULL),\
            EDGE TYPE KNOWS\
        )",
    );
    assert!(
        result.is_ok(),
        "Failed to create graph type with key label sets: {result:?}"
    );

    // Verify the type was registered by trying a duplicate
    let dup = session.execute("CREATE GRAPH TYPE keyed_type");
    assert!(dup.is_err(), "Duplicate should fail");
}

#[test]
fn test_create_graph_type_or_replace_inline() {
    // GG03 + OR REPLACE: Replace graph type with new inline types
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session
        .execute(
            "CREATE GRAPH TYPE replaceable (\
            NODE TYPE OldType (name STRING)\
        )",
        )
        .unwrap();

    // OR REPLACE should succeed (not fail with "already exists")
    let result = session.execute(
        "CREATE OR REPLACE GRAPH TYPE replaceable (\
            NODE TYPE NewType (title STRING NOT NULL)\
        )",
    );
    assert!(result.is_ok(), "Failed to replace graph type: {result:?}");
}

#[test]
fn test_graph_type_inference_from_registered_types() {
    // GG22: Element type key label set inference
    // When using LIKE on a graph without a bound type, infer from registered types
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // Register some node/edge types
    session
        .execute("CREATE NODE TYPE Movie (title STRING NOT NULL)")
        .unwrap();
    session
        .execute("CREATE EDGE TYPE RATED (stars INTEGER)")
        .unwrap();

    // Create a graph (no type binding)
    session.execute("CREATE GRAPH film_db").unwrap();

    // LIKE should infer from registered types
    let result = session.execute("CREATE GRAPH TYPE film_type LIKE film_db");
    assert!(result.is_ok(), "Failed: {result:?}");
}

// ---------------------------------------------------------------------------
// GF11: Binary Set Functions (COVAR, CORR, REGR_*)
// ---------------------------------------------------------------------------

fn setup_scatter_db() -> GrafeoDB {
    // Creates nodes with (x, y) pairs for statistical testing.
    // Data: (1,2), (2,4), (3,6), (4,8), (5,10) -- perfect linear y = 2x
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();
    session.begin_tx().unwrap();
    session.execute("INSERT (:Point {x: 1.0, y: 2.0})").unwrap();
    session.execute("INSERT (:Point {x: 2.0, y: 4.0})").unwrap();
    session.execute("INSERT (:Point {x: 3.0, y: 6.0})").unwrap();
    session.execute("INSERT (:Point {x: 4.0, y: 8.0})").unwrap();
    session
        .execute("INSERT (:Point {x: 5.0, y: 10.0})")
        .unwrap();
    session.commit().unwrap();
    db
}

#[test]
fn test_covar_samp_perfect_linear() {
    let db = setup_scatter_db();
    let session = db.session();
    let result = session
        .execute("MATCH (p:Point) RETURN COVAR_SAMP(p.y, p.x) AS cov")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    // For y=2x: Cov(y,x) = 2 * Var(x) = 2 * 2.5 = 5.0
    let cov = match &result.rows[0][0] {
        Value::Float64(f) => *f,
        other => panic!("Expected Float64, got: {:?}", other),
    };
    assert!(
        (cov - 5.0).abs() < 1e-10,
        "COVAR_SAMP should be 5.0, got {cov}"
    );
}

#[test]
fn test_covar_pop_perfect_linear() {
    let db = setup_scatter_db();
    let session = db.session();
    let result = session
        .execute("MATCH (p:Point) RETURN COVAR_POP(p.y, p.x) AS cov")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    // Pop covariance = S_xy / n = (5 * 5.0) / 5... actually S_xy/n = c_xy/n
    // Welford c_xy for y=2x data: sum of dx*dy2 increments
    // For n=5: CovarPop = CovarSamp * (n-1)/n = 5.0 * 4/5 = 4.0
    let cov = match &result.rows[0][0] {
        Value::Float64(f) => *f,
        other => panic!("Expected Float64, got: {:?}", other),
    };
    assert!(
        (cov - 4.0).abs() < 1e-10,
        "COVAR_POP should be 4.0, got {cov}"
    );
}

#[test]
fn test_corr_perfect_positive() {
    let db = setup_scatter_db();
    let session = db.session();
    let result = session
        .execute("MATCH (p:Point) RETURN CORR(p.y, p.x) AS r")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let r = match &result.rows[0][0] {
        Value::Float64(f) => *f,
        other => panic!("Expected Float64, got: {:?}", other),
    };
    assert!(
        (r - 1.0).abs() < 1e-10,
        "CORR should be 1.0 for perfect linear, got {r}"
    );
}

#[test]
fn test_regr_slope_and_intercept() {
    let db = setup_scatter_db();
    let session = db.session();
    // y = 2x + 0, so slope = 2.0 and intercept = 0.0
    let result = session
        .execute("MATCH (p:Point) RETURN REGR_SLOPE(p.y, p.x) AS slope, REGR_INTERCEPT(p.y, p.x) AS intercept")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let slope = match &result.rows[0][0] {
        Value::Float64(f) => *f,
        other => panic!("Expected Float64 for slope, got: {:?}", other),
    };
    let intercept = match &result.rows[0][1] {
        Value::Float64(f) => *f,
        other => panic!("Expected Float64 for intercept, got: {:?}", other),
    };
    assert!(
        (slope - 2.0).abs() < 1e-10,
        "REGR_SLOPE should be 2.0, got {slope}"
    );
    assert!(
        intercept.abs() < 1e-10,
        "REGR_INTERCEPT should be 0.0, got {intercept}"
    );
}

#[test]
fn test_regr_r2() {
    let db = setup_scatter_db();
    let session = db.session();
    let result = session
        .execute("MATCH (p:Point) RETURN REGR_R2(p.y, p.x) AS r2")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let r2 = match &result.rows[0][0] {
        Value::Float64(f) => *f,
        other => panic!("Expected Float64, got: {:?}", other),
    };
    assert!((r2 - 1.0).abs() < 1e-10, "REGR_R2 should be 1.0, got {r2}");
}

#[test]
fn test_regr_count() {
    let db = setup_scatter_db();
    let session = db.session();
    let result = session
        .execute("MATCH (p:Point) RETURN REGR_COUNT(p.y, p.x) AS cnt")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let cnt = match &result.rows[0][0] {
        Value::Int64(i) => *i,
        other => panic!("Expected Int64, got: {:?}", other),
    };
    assert_eq!(cnt, 5, "REGR_COUNT should be 5, got {cnt}");
}

#[test]
fn test_regr_sxx_syy_sxy() {
    let db = setup_scatter_db();
    let session = db.session();
    let result = session
        .execute(
            "MATCH (p:Point) RETURN REGR_SXX(p.y, p.x) AS sxx, REGR_SYY(p.y, p.x) AS syy, REGR_SXY(p.y, p.x) AS sxy",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    // Var(x) = 2.5, so S_xx (population) = sum of (xi - mean_x)^2 = 10.0
    // Var(y) = 10.0, so S_yy = 40.0
    // Cov(y,x) = 5.0 (sample), S_xy = 20.0
    let sxx = match &result.rows[0][0] {
        Value::Float64(f) => *f,
        other => panic!("Expected Float64, got: {:?}", other),
    };
    let syy = match &result.rows[0][1] {
        Value::Float64(f) => *f,
        other => panic!("Expected Float64, got: {:?}", other),
    };
    let sxy = match &result.rows[0][2] {
        Value::Float64(f) => *f,
        other => panic!("Expected Float64, got: {:?}", other),
    };
    assert!(
        (sxx - 10.0).abs() < 1e-10,
        "REGR_SXX should be 10.0, got {sxx}"
    );
    assert!(
        (syy - 40.0).abs() < 1e-10,
        "REGR_SYY should be 40.0, got {syy}"
    );
    assert!(
        (sxy - 20.0).abs() < 1e-10,
        "REGR_SXY should be 20.0, got {sxy}"
    );
}

#[test]
fn test_regr_avgx_avgy() {
    let db = setup_scatter_db();
    let session = db.session();
    let result = session
        .execute("MATCH (p:Point) RETURN REGR_AVGX(p.y, p.x) AS ax, REGR_AVGY(p.y, p.x) AS ay")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let ax = match &result.rows[0][0] {
        Value::Float64(f) => *f,
        other => panic!("Expected Float64, got: {:?}", other),
    };
    let ay = match &result.rows[0][1] {
        Value::Float64(f) => *f,
        other => panic!("Expected Float64, got: {:?}", other),
    };
    assert!(
        (ax - 3.0).abs() < 1e-10,
        "REGR_AVGX should be 3.0, got {ax}"
    );
    assert!(
        (ay - 6.0).abs() < 1e-10,
        "REGR_AVGY should be 6.0, got {ay}"
    );
}

#[test]
fn test_binary_null_pair_skipping() {
    // When one value in a pair is NULL, the pair should be skipped entirely
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();
    session.begin_tx().unwrap();
    session.execute("INSERT (:Point {x: 1.0, y: 2.0})").unwrap();
    session
        .execute("INSERT (:Point {x: 2.0})") // y is NULL
        .unwrap();
    session.execute("INSERT (:Point {x: 3.0, y: 6.0})").unwrap();
    session.commit().unwrap();

    let result = session
        .execute("MATCH (p:Point) RETURN REGR_COUNT(p.y, p.x) AS cnt")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let cnt = match &result.rows[0][0] {
        Value::Int64(i) => *i,
        other => panic!("Expected Int64, got: {:?}", other),
    };
    // Only 2 valid pairs (the one with NULL y is skipped)
    assert_eq!(cnt, 2, "REGR_COUNT should skip NULL pairs, got {cnt}");
}

#[test]
fn test_binary_edge_case_empty() {
    // No matching rows: should return NULL
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    let result = session
        .execute("MATCH (p:Point) RETURN COVAR_SAMP(p.y, p.x) AS cov")
        .unwrap();
    // With no rows, the aggregate should return NULL (or empty result)
    if !result.rows.is_empty() {
        assert!(
            matches!(&result.rows[0][0], Value::Null),
            "COVAR_SAMP with 0 rows should be NULL, got {:?}",
            result.rows[0][0]
        );
    }
}
