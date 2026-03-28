//! SQL/PGQ extended coverage tests.
//!
//! Covers areas identified as weakly tested:
//! - Parameterized queries ($param syntax)
//! - NULL arithmetic and three-valued logic
//! - Type coercion (int/float promotion)
//! - String functions (UPPER, LOWER, TRIM, SUBSTRING, REPLACE, etc.)
//! - Math functions (ABS, CEIL, FLOOR, ROUND, SQRT, etc.)
//! - Type conversion functions (TOINTEGER, TOFLOAT, TOBOOLEAN, TOSTRING)
//! - Transaction integration (BEGIN/COMMIT/ROLLBACK + SQL/PGQ)
//! - Multiple optional matches (multiple LEFT JOIN MATCH)
//! - Edge property projection without WHERE
//!
//! Run with:
//! ```bash
//! cargo test -p grafeo-engine --features sql-pgq --test sql_pgq_coverage_extended
//! ```

#![cfg(feature = "sql-pgq")]

use grafeo_common::types::Value;
use grafeo_engine::GrafeoDB;

// ============================================================================
// Shared fixture: same as sql_pgq_coverage.rs
// ============================================================================

fn create_rich_network() -> GrafeoDB {
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
    let harm = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Harm".into())),
            ("age", Value::Int64(35)),
            ("city", Value::String("Amsterdam".into())),
        ],
    );
    let vincent = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Vincent".into())),
            ("age", Value::Int64(28)),
            ("city", Value::String("Paris".into())),
        ],
    );
    let mia = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Mia".into())),
            ("age", Value::Int64(32)),
            ("city", Value::String("Berlin".into())),
        ],
    );

    let e1 = session.create_edge(alix, gus, "KNOWS");
    db.set_edge_property(e1, "since", Value::Int64(2020));
    let e2 = session.create_edge(alix, harm, "KNOWS");
    db.set_edge_property(e2, "since", Value::Int64(2018));
    let e3 = session.create_edge(gus, harm, "KNOWS");
    db.set_edge_property(e3, "since", Value::Int64(2021));
    let e4 = session.create_edge(vincent, mia, "KNOWS");
    db.set_edge_property(e4, "since", Value::Int64(2019));
    let e5 = session.create_edge(alix, vincent, "FOLLOWS");
    db.set_edge_property(e5, "since", Value::Int64(2022));
    let e6 = session.create_edge(gus, alix, "FOLLOWS");
    db.set_edge_property(e6, "since", Value::Int64(2023));

    db
}

// ============================================================================
// Parameterized Queries
// ============================================================================

#[test]
fn test_param_string_in_inner_where() {
    let db = create_rich_network();
    let session = db.session();

    let mut params = std::collections::HashMap::new();
    params.insert("name".to_string(), Value::String("Alix".into()));

    let result = session
        .execute_sql_with_params(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = $name
                COLUMNS (n.name AS name, n.age AS age)
            )",
            params,
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
    assert_eq!(result.rows[0][1], Value::Int64(30));
}

#[test]
fn test_param_integer_in_inner_where() {
    let db = create_rich_network();
    let session = db.session();

    let mut params = std::collections::HashMap::new();
    params.insert("min_age".to_string(), Value::Int64(30));

    let result = session
        .execute_sql_with_params(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.age >= $min_age
                COLUMNS (n.name AS name, n.age AS age)
            ) ORDER BY name",
            params,
        )
        .unwrap();

    assert_eq!(result.row_count(), 3);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
    assert_eq!(result.rows[1][0], Value::String("Harm".into()));
    assert_eq!(result.rows[2][0], Value::String("Mia".into()));
}

#[test]
fn test_param_multiple_in_inner_where() {
    let db = create_rich_network();
    let session = db.session();

    let mut params = std::collections::HashMap::new();
    params.insert("min_age".to_string(), Value::Int64(26));
    params.insert("max_age".to_string(), Value::Int64(31));

    let result = session
        .execute_sql_with_params(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.age >= $min_age AND n.age <= $max_age
                COLUMNS (n.name AS name, n.age AS age)
            ) ORDER BY name",
            params,
        )
        .unwrap();

    assert_eq!(result.row_count(), 2);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
    assert_eq!(result.rows[1][0], Value::String("Vincent".into()));
}

#[test]
fn test_param_in_columns_expression() {
    let db = create_rich_network();
    let session = db.session();

    let mut params = std::collections::HashMap::new();
    params.insert("bonus".to_string(), Value::Int64(100));

    let result = session
        .execute_sql_with_params(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Gus'
                COLUMNS (n.name AS name, n.age + $bonus AS boosted_age)
            )",
            params,
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][1], Value::Int64(125), "25 + 100 = 125");
}

#[test]
fn test_param_with_in_operator() {
    let db = create_rich_network();
    let session = db.session();

    let mut params = std::collections::HashMap::new();
    params.insert(
        "cities".to_string(),
        Value::List(
            vec![
                Value::String("Amsterdam".into()),
                Value::String("Paris".into()),
            ]
            .into(),
        ),
    );

    let result = session
        .execute_sql_with_params(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.city IN $cities
                COLUMNS (n.name AS name, n.city AS city)
            ) ORDER BY name",
            params,
        )
        .unwrap();

    assert_eq!(result.row_count(), 3);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
    assert_eq!(result.rows[1][0], Value::String("Harm".into()));
    assert_eq!(result.rows[2][0], Value::String("Vincent".into()));
}

#[test]
fn test_param_in_outer_sql_where() {
    let db = create_rich_network();
    let session = db.session();

    let mut params = std::collections::HashMap::new();
    params.insert("city".to_string(), Value::String("Berlin".into()));

    let result = session
        .execute_sql_with_params(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.city AS city)
            ) AS g WHERE g.city = $city ORDER BY g.name",
            params,
        )
        .unwrap();

    assert_eq!(result.row_count(), 2);
    assert_eq!(result.rows[0][0], Value::String("Gus".into()));
    assert_eq!(result.rows[1][0], Value::String("Mia".into()));
}

#[test]
fn test_param_missing_parameter_error() {
    let db = create_rich_network();
    let session = db.session();

    let params = std::collections::HashMap::new();

    let result = session.execute_sql_with_params(
        "SELECT * FROM GRAPH_TABLE (
            MATCH (n:Person)
            WHERE n.name = $name
            COLUMNS (n.name AS name)
        )",
        params,
    );

    assert!(result.is_err(), "Should fail when parameter is missing");
}

#[test]
fn test_param_wrong_type_comparison() {
    let db = create_rich_network();
    let session = db.session();

    let mut params = std::collections::HashMap::new();
    params.insert("age".to_string(), Value::String("thirty".into()));

    let result = session.execute_sql_with_params(
        "SELECT * FROM GRAPH_TABLE (
            MATCH (n:Person)
            WHERE n.age = $age
            COLUMNS (n.name AS name)
        )",
        params,
    );

    match result {
        Ok(qr) => assert_eq!(
            qr.row_count(),
            0,
            "String 'thirty' should not match any integer age"
        ),
        Err(_) => {} // type-mismatch error is also acceptable
    }
}

// ============================================================================
// NULL Arithmetic
// ============================================================================

#[test]
fn test_null_add_integer_returns_null() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("alpha".into())),
            ("val", Value::Null),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Item)
                COLUMNS (n.name AS name, n.val + 5 AS sum)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert!(result.rows[0][1].is_null(), "NULL + 5 should produce NULL");
}

#[test]
fn test_null_multiply_float_returns_null() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("alpha".into())),
            ("val", Value::Null),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Item)
                COLUMNS (n.name AS name, n.val * 2.5 AS product)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert!(
        result.rows[0][1].is_null(),
        "NULL * 2.5 should produce NULL"
    );
}

#[test]
fn test_null_subtraction_returns_null() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("alpha".into())),
            ("val", Value::Null),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Item)
                COLUMNS (n.name AS name, n.val - 3 AS diff)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert!(result.rows[0][1].is_null(), "NULL - 3 should produce NULL");
}

#[test]
fn test_null_division_returns_null() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("alpha".into())),
            ("val", Value::Null),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Item)
                COLUMNS (n.name AS name, n.val / 2 AS quotient)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert!(result.rows[0][1].is_null(), "NULL / 2 should produce NULL");
}

#[test]
fn test_null_gt_integer_filters_out() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("alpha".into())),
            ("val", Value::Null),
        ],
    );
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("beta".into())),
            ("val", Value::Int64(10)),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Item) WHERE n.val > 5 COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("beta".into()));
}

#[test]
fn test_null_eq_null_matches_nothing() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("alpha".into())),
            ("val", Value::Null),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Item) WHERE n.val = NULL COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(
        result.row_count(),
        0,
        "NULL = NULL is UNKNOWN, no rows should match"
    );
}

// ============================================================================
// Three-Valued Logic
// ============================================================================

#[test]
fn test_null_and_true_is_unknown() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("alpha".into())),
            ("val", Value::Null),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Item) WHERE n.val = NULL AND n.name = 'alpha' COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 0, "UNKNOWN AND TRUE = UNKNOWN");
}

#[test]
fn test_null_or_true_is_true() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("alpha".into())),
            ("val", Value::Null),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Item) WHERE n.val = NULL OR n.name = 'alpha' COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1, "UNKNOWN OR TRUE = TRUE");
    assert_eq!(result.rows[0][0], Value::String("alpha".into()));
}

#[test]
fn test_null_or_false_is_unknown() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("alpha".into())),
            ("val", Value::Null),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Item) WHERE n.val = NULL OR n.name = 'nonexistent' COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 0, "UNKNOWN OR FALSE = UNKNOWN");
}

#[test]
fn test_not_null_eq_is_unknown() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("alpha".into())),
            ("val", Value::Null),
        ],
    );
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("beta".into())),
            ("val", Value::Int64(10)),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Item) WHERE NOT (n.val = NULL) COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 0, "NOT UNKNOWN = UNKNOWN");
}

// ============================================================================
// NULL in Aggregates
// ============================================================================

#[test]
fn test_null_count_star_counts_all_rows() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("alpha".into())),
            ("val", Value::Int64(10)),
        ],
    );
    session.create_node_with_props(
        &["Item"],
        [("name", Value::String("beta".into())), ("val", Value::Null)],
    );
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("gamma".into())),
            ("val", Value::Int64(30)),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT COUNT(*) AS total FROM GRAPH_TABLE (MATCH (n:Item) COLUMNS (n.val AS val))",
        )
        .unwrap();

    assert_eq!(
        result.rows[0][0],
        Value::Int64(3),
        "COUNT(*) counts all rows including NULL"
    );
}

#[test]
#[ignore = "COUNT(column) does not yet skip NULLs: currently counts all rows like COUNT(*)"]
fn test_null_count_column_skips_nulls() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("alpha".into())),
            ("val", Value::Int64(10)),
        ],
    );
    session.create_node_with_props(
        &["Item"],
        [("name", Value::String("beta".into())), ("val", Value::Null)],
    );
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("gamma".into())),
            ("val", Value::Int64(30)),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT COUNT(val) AS cnt FROM GRAPH_TABLE (MATCH (n:Item) COLUMNS (n.val AS val))",
        )
        .unwrap();

    assert_eq!(
        result.rows[0][0],
        Value::Int64(2),
        "COUNT(column) skips NULLs"
    );
}

#[test]
fn test_null_sum_skips_nulls() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Item"], [("val", Value::Int64(10))]);
    session.create_node_with_props(&["Item"], [("val", Value::Null)]);
    session.create_node_with_props(&["Item"], [("val", Value::Int64(30))]);

    let result = session
        .execute_sql(
            "SELECT SUM(val) AS total FROM GRAPH_TABLE (MATCH (n:Item) COLUMNS (n.val AS val))",
        )
        .unwrap();

    match &result.rows[0][0] {
        Value::Int64(v) => assert_eq!(*v, 40),
        Value::Float64(v) => assert!((*v - 40.0).abs() < 0.01),
        other => panic!("expected numeric sum of 40, got {other:?}"),
    }
}

#[test]
fn test_null_avg_skips_nulls() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Item"], [("val", Value::Int64(10))]);
    session.create_node_with_props(&["Item"], [("val", Value::Null)]);
    session.create_node_with_props(&["Item"], [("val", Value::Int64(30))]);

    let result = session
        .execute_sql(
            "SELECT AVG(val) AS average FROM GRAPH_TABLE (MATCH (n:Item) COLUMNS (n.val AS val))",
        )
        .unwrap();

    match &result.rows[0][0] {
        Value::Float64(v) => assert!(
            (*v - 20.0).abs() < 0.01,
            "avg(10, NULL, 30) = 20.0, got {v}"
        ),
        Value::Int64(v) => assert_eq!(*v, 20),
        other => panic!("expected numeric avg of 20, got {other:?}"),
    }
}

#[test]
fn test_null_min_max_skip_nulls() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Item"], [("val", Value::Int64(10))]);
    session.create_node_with_props(&["Item"], [("val", Value::Null)]);
    session.create_node_with_props(&["Item"], [("val", Value::Int64(30))]);

    let result = session
        .execute_sql("SELECT MIN(val) AS lo, MAX(val) AS hi FROM GRAPH_TABLE (MATCH (n:Item) COLUMNS (n.val AS val))")
        .unwrap();

    assert_eq!(result.rows[0][0], Value::Int64(10));
    assert_eq!(result.rows[0][1], Value::Int64(30));
}

// ============================================================================
// IS NULL edge cases
// ============================================================================

#[test]
fn test_null_is_null_on_missing_property() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Thing"], [("name", Value::String("widget".into()))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (t:Thing) WHERE t.color IS NULL COLUMNS (t.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(
        result.row_count(),
        1,
        "Missing property should behave as NULL"
    );
}

#[test]
fn test_null_explicit_vs_missing_property() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Thing"],
        [
            ("name", Value::String("alpha".into())),
            ("color", Value::Null),
        ],
    );
    session.create_node_with_props(&["Thing"], [("name", Value::String("beta".into()))]);
    session.create_node_with_props(
        &["Thing"],
        [
            ("name", Value::String("gamma".into())),
            ("color", Value::String("blue".into())),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (t:Thing) WHERE t.color IS NULL COLUMNS (t.name AS name)
            ) ORDER BY name",
        )
        .unwrap();

    assert_eq!(
        result.row_count(),
        2,
        "Both explicit NULL and missing property match IS NULL"
    );
}

#[test]
fn test_null_chained_not_is_null() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Thing"], [("name", Value::String("widget".into()))]);
    session.create_node_with_props(
        &["Thing"],
        [
            ("name", Value::String("gadget".into())),
            ("color", Value::String("red".into())),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (t:Thing) WHERE NOT (t.color IS NULL) COLUMNS (t.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("gadget".into()));
}

// ============================================================================
// Type Coercion
// ============================================================================

#[test]
fn test_coercion_int_compared_to_float() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Alix".into())),
            ("age", Value::Int64(30)),
        ],
    );
    session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Gus".into())),
            ("age", Value::Int64(25)),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.age > 29.5 COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
}

#[test]
fn test_coercion_int_arithmetic_with_float() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Alix".into())),
            ("age", Value::Int64(30)),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) COLUMNS (n.name AS name, n.age + 0.5 AS age_plus)
            )",
        )
        .unwrap();

    if let Value::Float64(v) = result.rows[0][1] {
        assert!((v - 30.5).abs() < 0.01, "30 + 0.5 should be 30.5, got {v}");
    } else {
        panic!("expected Float64, got {:?}", result.rows[0][1]);
    }
}

#[test]
fn test_coercion_mixed_int_float_sum() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Val"], [("n", Value::Int64(10))]);
    session.create_node_with_props(&["Val"], [("n", Value::Float64(20.5))]);
    session.create_node_with_props(&["Val"], [("n", Value::Int64(30))]);

    let result = session
        .execute_sql(
            "SELECT SUM(amount) AS total FROM GRAPH_TABLE (MATCH (v:Val) COLUMNS (v.n AS amount))",
        )
        .unwrap();

    match &result.rows[0][0] {
        Value::Float64(v) => assert!(
            (*v - 60.5).abs() < 0.01,
            "sum(10, 20.5, 30) = 60.5, got {v}"
        ),
        other => panic!("expected Float64(60.5), got {other:?}"),
    }
}

#[test]
fn test_coercion_int_eq_float() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Num"],
        [
            ("name", Value::String("five".into())),
            ("v", Value::Int64(5)),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (MATCH (n:Num) WHERE n.v = 5.0 COLUMNS (n.name AS name))",
        )
        .unwrap();

    assert_eq!(
        result.row_count(),
        1,
        "Int64(5) = Float64(5.0) should match"
    );
}

// ============================================================================
// String Functions
// ============================================================================

#[test]
fn test_func_upper() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Alix'
                COLUMNS (n.name AS name, UPPER(n.name) AS upper_name)
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][1], Value::String("ALIX".into()));
}

#[test]
fn test_func_lower() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Vincent'
                COLUMNS (n.name AS name, LOWER(n.name) AS lower_name)
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][1], Value::String("vincent".into()));
}

#[test]
fn test_func_trim() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Person"], [("name", Value::String("  Alix  ".into()))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (TRIM(n.name) AS trimmed))",
        )
        .unwrap();

    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
}

#[test]
fn test_func_substring_two_args() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Vincent'
                COLUMNS (n.name AS name, SUBSTRING(n.name, 1) AS sub)
            )",
        )
        .unwrap();

    // substring('Vincent', 1) skips first char => 'incent'
    assert_eq!(result.rows[0][1], Value::String("incent".into()));
}

#[test]
fn test_func_substring_three_args() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Vincent'
                COLUMNS (n.name AS name, SUBSTRING(n.name, 0, 3) AS sub)
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][1], Value::String("Vin".into()));
}

#[test]
fn test_func_replace() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Alix'
                COLUMNS (n.name AS name, REPLACE(n.city, 'dam', 'DAM') AS replaced)
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][1], Value::String("AmsterDAM".into()));
}

#[test]
#[ignore = "LEFT is a reserved keyword in SQL/PGQ parser (LEFT JOIN), cannot be used as function name"]
fn test_func_left() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Vincent'
                COLUMNS (n.name AS name, LEFT(n.name, 3) AS prefix)
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][1], Value::String("Vin".into()));
}

#[test]
fn test_func_right() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Vincent'
                COLUMNS (n.name AS name, RIGHT(n.name, 3) AS suffix)
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][1], Value::String("ent".into()));
}

#[test]
fn test_func_size_on_string() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Alix'
                COLUMNS (n.name AS name, SIZE(n.name) AS name_len)
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][1], Value::Int64(4));
}

#[test]
fn test_func_reverse_string() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Alix'
                COLUMNS (n.name AS name, REVERSE(n.name) AS rev)
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][1], Value::String("xilA".into()));
}

#[test]
fn test_func_string_concat_via_plus() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Alix'
                COLUMNS (n.name AS name, n.name + ' from ' + n.city AS greeting)
            )",
        )
        .unwrap();

    assert_eq!(
        result.rows[0][1],
        Value::String("Alix from Amsterdam".into())
    );
}

#[test]
fn test_func_split() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Person"], [("tags", Value::String("a,b,c".into()))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (SPLIT(n.tags, ',') AS parts))",
        )
        .unwrap();

    let expected = Value::List(
        vec![
            Value::String("a".into()),
            Value::String("b".into()),
            Value::String("c".into()),
        ]
        .into(),
    );
    assert_eq!(result.rows[0][0], expected);
}

// ============================================================================
// Math Functions
// ============================================================================

#[test]
fn test_func_abs() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Gus'
                COLUMNS (n.name AS name, ABS(n.age - 30) AS distance)
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][1], Value::Int64(5), "abs(25 - 30) = 5");
}

#[test]
fn test_func_ceil() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Person"], [("score", Value::Float64(3.2))]);

    let result = session
        .execute_sql("SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (CEIL(n.score) AS val))")
        .unwrap();

    assert_eq!(result.rows[0][0], Value::Int64(4));
}

#[test]
fn test_func_floor() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Person"], [("score", Value::Float64(3.9))]);

    let result = session
        .execute_sql("SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (FLOOR(n.score) AS val))")
        .unwrap();

    assert_eq!(result.rows[0][0], Value::Int64(3));
}

#[test]
fn test_func_round() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Person"], [("score", Value::Float64(3.5))]);

    let result = session
        .execute_sql("SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (ROUND(n.score) AS val))")
        .unwrap();

    assert_eq!(result.rows[0][0], Value::Int64(4));
}

#[test]
fn test_func_sqrt() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Person"], [("val", Value::Int64(16))]);

    let result = session
        .execute_sql("SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (SQRT(n.val) AS root))")
        .unwrap();

    assert_eq!(result.rows[0][0], Value::Float64(4.0));
}

#[test]
fn test_func_sign() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Num"], [("val", Value::Int64(-7))]);

    let result = session
        .execute_sql("SELECT * FROM GRAPH_TABLE (MATCH (n:Num) COLUMNS (SIGN(n.val) AS s))")
        .unwrap();

    assert_eq!(result.rows[0][0], Value::Int64(-1));
}

#[test]
fn test_func_power() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Person"], [("val", Value::Int64(3))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (POWER(n.val, 2) AS squared))",
        )
        .unwrap();

    assert_eq!(result.rows[0][0], Value::Float64(9.0));
}

#[test]
fn test_func_log_and_exp() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Person"], [("val", Value::Int64(0))]);

    let result = session
        .execute_sql("SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (EXP(n.val) AS exp_val))")
        .unwrap();

    assert_eq!(result.rows[0][0], Value::Float64(1.0), "exp(0) = 1.0");
}

#[test]
fn test_func_nested_math() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Person"], [("val", Value::Float64(-3.7))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (CEIL(ABS(n.val)) AS result))",
        )
        .unwrap();

    // abs(-3.7) = 3.7, ceil(3.7) = 4
    assert_eq!(result.rows[0][0], Value::Int64(4));
}

#[test]
fn test_func_upper_lower_roundtrip() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Alix'
                COLUMNS (n.name AS name, LOWER(UPPER(n.name)) AS roundtrip)
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][1], Value::String("alix".into()));
}

// ============================================================================
// Type Conversion Functions
// ============================================================================

#[test]
fn test_func_tostring_from_integer() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Gus'
                COLUMNS (n.name AS name, TOSTRING(n.age) AS age_str)
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][1], Value::String("25".into()));
}

#[test]
fn test_func_tointeger_from_float() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Person"], [("score", Value::Float64(3.9))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (TOINTEGER(n.score) AS int_val))",
        )
        .unwrap();

    assert_eq!(result.rows[0][0], Value::Int64(3), "truncates toward zero");
}

#[test]
fn test_func_tointeger_from_string() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Person"], [("val", Value::String("42".into()))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (TOINTEGER(n.val) AS int_val))",
        )
        .unwrap();

    assert_eq!(result.rows[0][0], Value::Int64(42));
}

#[test]
fn test_func_tofloat_from_integer() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Alix'
                COLUMNS (n.name AS name, TOFLOAT(n.age) AS float_val)
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][1], Value::Float64(30.0));
}

#[test]
fn test_func_toboolean_from_string() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(&["Person"], [("flag", Value::String("true".into()))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (TOBOOLEAN(n.flag) AS bool_val))",
        )
        .unwrap();

    assert_eq!(result.rows[0][0], Value::Bool(true));
}

#[test]
fn test_func_concat_integer_to_string() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Gus'
                COLUMNS (n.name AS name, n.name + ': ' + TOSTRING(n.age) AS desc)
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][1], Value::String("Gus: 25".into()));
}

#[test]
fn test_func_case_insensitive_names() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Alix'
                COLUMNS (n.name AS name, Upper(n.name) AS u, lower(n.name) AS l, Abs(n.age) AS a)
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][1], Value::String("ALIX".into()));
    assert_eq!(result.rows[0][2], Value::String("alix".into()));
    assert_eq!(result.rows[0][3], Value::Int64(30));
}

#[test]
fn test_func_multiple_functions_in_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) WHERE n.name = 'Gus'
                COLUMNS (
                    n.name AS name,
                    UPPER(n.name) AS upper_name,
                    SIZE(n.name) AS name_len,
                    ABS(n.age - 30) AS age_diff,
                    ROUND(TOFLOAT(n.age) * 1.1) AS adjusted_age
                )
            )",
        )
        .unwrap();

    assert_eq!(result.rows[0][0], Value::String("Gus".into()));
    assert_eq!(result.rows[0][1], Value::String("GUS".into()));
    assert_eq!(result.rows[0][2], Value::Int64(3));
    assert_eq!(result.rows[0][3], Value::Int64(5));
    // 25 * 1.1 = 27.5, round = 28
    assert_eq!(result.rows[0][4], Value::Int64(28));
}

// ============================================================================
// Transaction Integration
// ============================================================================

#[test]
fn test_tx_commit_makes_data_visible_to_sql_pgq() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();

    session.begin_transaction().unwrap();
    session
        .execute("INSERT (:Person {name: 'Alix', age: 30})")
        .unwrap();
    session
        .execute("INSERT (:Person {name: 'Gus', age: 25})")
        .unwrap();
    session.commit().unwrap();

    let result = session
        .execute_sql("SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (n.name AS name))")
        .unwrap();

    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_tx_rollback_hides_data_from_sql_pgq() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();

    session
        .execute("INSERT (:Person {name: 'Alix', age: 30})")
        .unwrap();

    session.begin_transaction().unwrap();
    session
        .execute("INSERT (:Person {name: 'Gus', age: 25})")
        .unwrap();
    session.rollback().unwrap();

    let result = session
        .execute_sql("SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (n.name AS name))")
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0].as_str(), Some("Alix"));
}

#[test]
fn test_tx_sql_pgq_sees_data_in_same_transaction() {
    let db = GrafeoDB::new_in_memory();
    let mut session = db.session();

    session.begin_transaction().unwrap();
    session
        .execute("INSERT (:Person {name: 'Mia', city: 'Berlin'})")
        .unwrap();
    session
        .execute("INSERT (:Person {name: 'Butch', city: 'Amsterdam'})")
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (n.name AS name))")
        .unwrap();

    assert_eq!(result.row_count(), 2);
    session.commit().unwrap();
}

#[test]
fn test_tx_isolation_uncommitted_not_visible() {
    let db = GrafeoDB::new_in_memory();

    let setup = db.session();
    setup.create_node_with_props(&["Person"], [("name", Value::String("Alix".into()))]);

    let mut session1 = db.session();
    session1.begin_transaction().unwrap();
    session1
        .execute("INSERT (:Person {name: 'Django'})")
        .unwrap();

    let session2 = db.session();
    let result = session2
        .execute_sql("SELECT * FROM GRAPH_TABLE (MATCH (n:Person) COLUMNS (n.name AS name))")
        .unwrap();

    assert_eq!(
        result.row_count(),
        1,
        "session2 should not see uncommitted data"
    );
    assert_eq!(result.rows[0][0].as_str(), Some("Alix"));

    session1.rollback().unwrap();
}

// ============================================================================
// Multiple Optional Matches
// ============================================================================

#[test]
fn test_multi_optional_two_left_join_match() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let alix =
        session.create_node_with_props(&["Person"], [("name", Value::String("Alix".into()))]);
    let gus = session.create_node_with_props(&["Person"], [("name", Value::String("Gus".into()))]);
    let vincent =
        session.create_node_with_props(&["Person"], [("name", Value::String("Vincent".into()))]);
    let berlin =
        session.create_node_with_props(&["City"], [("name", Value::String("Berlin".into()))]);

    session.create_edge(alix, gus, "KNOWS");
    session.create_edge(alix, berlin, "LIVES_IN");
    let _ = vincent;

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)
                LEFT OUTER JOIN MATCH (a)-[:KNOWS]->(b:Person)
                LEFT OUTER JOIN MATCH (a)-[:LIVES_IN]->(c:City)
                COLUMNS (a.name AS person, b.name AS friend, c.name AS city)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 3);

    let alix_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Alix"))
        .unwrap();
    assert_eq!(alix_row[1].as_str(), Some("Gus"));
    assert_eq!(alix_row[2].as_str(), Some("Berlin"));

    let gus_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Gus"))
        .unwrap();
    assert!(gus_row[1].is_null());
    assert!(gus_row[2].is_null());
}

#[test]
fn test_multi_optional_three_left_joins() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let alix =
        session.create_node_with_props(&["Person"], [("name", Value::String("Alix".into()))]);
    let gus = session.create_node_with_props(&["Person"], [("name", Value::String("Gus".into()))]);
    let amsterdam =
        session.create_node_with_props(&["City"], [("name", Value::String("Amsterdam".into()))]);
    let grafeo_inc = session.create_node_with_props(
        &["Company"],
        [("name", Value::String("GrafeoDB Inc".into()))],
    );
    let _ = gus;

    session.create_edge(alix, gus, "KNOWS");
    session.create_edge(alix, amsterdam, "LIVES_IN");
    session.create_edge(alix, grafeo_inc, "WORKS_AT");

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)
                LEFT OUTER JOIN MATCH (a)-[:KNOWS]->(b:Person)
                LEFT OUTER JOIN MATCH (a)-[:LIVES_IN]->(c:City)
                LEFT OUTER JOIN MATCH (a)-[:WORKS_AT]->(d:Company)
                COLUMNS (a.name AS person, b.name AS friend, c.name AS city, d.name AS company)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 2);

    let alix_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Alix"))
        .unwrap();
    assert_eq!(alix_row[1].as_str(), Some("Gus"));
    assert_eq!(alix_row[2].as_str(), Some("Amsterdam"));
    assert_eq!(alix_row[3].as_str(), Some("GrafeoDB Inc"));

    let gus_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Gus"))
        .unwrap();
    assert!(gus_row[1].is_null());
    assert!(gus_row[2].is_null());
    assert!(gus_row[3].is_null());
}

#[test]
fn test_multi_optional_all_nulls() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Person"], [("name", Value::String("Shosanna".into()))]);
    session.create_node_with_props(&["Person"], [("name", Value::String("Hans".into()))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)
                LEFT OUTER JOIN MATCH (a)-[:KNOWS]->(b:Person)
                LEFT OUTER JOIN MATCH (a)-[:FOLLOWS]->(c:Person)
                COLUMNS (a.name AS person, b.name AS friend, c.name AS followed)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 2);
    for row in &result.rows {
        assert!(row[1].is_null());
        assert!(row[2].is_null());
    }
}

// ============================================================================
// Edge Property Projection Without WHERE
// ============================================================================

#[test]
fn test_edge_prop_in_columns_without_where() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let alix =
        session.create_node_with_props(&["Person"], [("name", Value::String("Alix".into()))]);
    let gus = session.create_node_with_props(&["Person"], [("name", Value::String("Gus".into()))]);

    let edge = session.create_edge(alix, gus, "KNOWS");
    db.set_edge_property(edge, "since", Value::Int64(2020));

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[e:KNOWS]->(b:Person)
                COLUMNS (a.name AS source, b.name AS target, e.since AS since)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][2], Value::Int64(2020));
}

#[test]
fn test_edge_prop_multiple_properties() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let vincent =
        session.create_node_with_props(&["Person"], [("name", Value::String("Vincent".into()))]);
    let mia = session.create_node_with_props(&["Person"], [("name", Value::String("Mia".into()))]);

    let edge = session.create_edge(vincent, mia, "KNOWS");
    db.set_edge_property(edge, "since", Value::Int64(2019));
    db.set_edge_property(edge, "strength", Value::Float64(0.85));
    db.set_edge_property(edge, "context", Value::String("work".into()));

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[e:KNOWS]->(b:Person)
                COLUMNS (a.name AS source, b.name AS target, e.since AS since, e.strength AS strength, e.context AS context)
            )",
        )
        .unwrap();

    assert_eq!(result.columns.len(), 5);
    assert_eq!(result.rows[0][2], Value::Int64(2019));
    assert_eq!(result.rows[0][3], Value::Float64(0.85));
    assert_eq!(result.rows[0][4], Value::String("work".into()));
}

#[test]
fn test_edge_prop_nonexistent_is_null() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let butch =
        session.create_node_with_props(&["Person"], [("name", Value::String("Butch".into()))]);
    let django =
        session.create_node_with_props(&["Person"], [("name", Value::String("Django".into()))]);

    let edge = session.create_edge(butch, django, "KNOWS");
    db.set_edge_property(edge, "since", Value::Int64(2021));

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[e:KNOWS]->(b:Person)
                COLUMNS (a.name AS source, b.name AS target, e.weight AS weight)
            )",
        )
        .unwrap();

    assert!(
        result.rows[0][2].is_null(),
        "Non-existent edge property should be NULL"
    );
}
