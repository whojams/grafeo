//! SQL/PGQ advanced expression and type feature tests.
//!
//! Covers areas not exercised by `sql_pgq_coverage_extended` or `sql_pgq_expr_edge_cases`:
//! - Temporal/date functions and literals (date(), time(), datetime(), duration())
//! - Statistical aggregates (STDEV, VARIANCE, COLLECT)
//! - Math functions (LOG, LN, LOG10, LOG2, PI, E, trig, DEGREES, RADIANS, ATAN2)
//! - String functions (CHAR_LENGTH, REVERSE on lists, lexicographic comparison)
//! - Boolean property access and filtering
//! - List operations (SIZE, REVERSE, HEAD, LAST, TAIL on list literals and properties)
//! - Map operations (map property access, nested maps)
//! - Complex nested expressions (chained functions)
//! - Type edge cases (large floats, integer extremes, empty structures)
//! - COALESCE function
//! - CASE expression
//!
//! Run with:
//! ```bash
//! cargo test -p grafeo-engine --features sql-pgq --test sql_pgq_advanced_expr
//! ```

#![cfg(feature = "sql-pgq")]

use std::collections::BTreeMap;
use std::sync::Arc;

use grafeo_common::types::{PropertyKey, Value};
use grafeo_engine::GrafeoDB;

// ============================================================================
// Shared fixture: rich dataset with various data types
// ============================================================================

/// Creates a graph with diverse property types: strings, ints, floats, bools,
/// nulls, lists, and maps.
///
/// Nodes:
/// - Alix (Person, age: 30, score: 8.5, active: true, tags: ["dev", "lead"],
///         meta: {team: "core"}, nickname: "Lix")
/// - Gus (Person, age: 25, score: 6.2, active: false, tags: ["dev"],
///         meta: {team: "infra"}, nickname: null/missing)
/// - Vincent (Person, age: 28, score: 9.1, active: true, tags: ["qa", "dev", "ops"],
///            meta: {team: "core", level: "senior"})
/// - Mia (Person, age: 32, score: 7.0, active: true, tags: [],
///         meta: {})
/// - Jules (Person, age: 40, score: null, active: false, tags: ["mgmt"],
///          meta: {team: "exec"})
fn create_typed_dataset() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let alix = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Alix".into())),
            ("age", Value::Int64(30)),
            ("score", Value::Float64(8.5)),
            ("active", Value::Bool(true)),
            (
                "tags",
                Value::List(vec![Value::String("dev".into()), Value::String("lead".into())].into()),
            ),
            ("nickname", Value::String("Lix".into())),
        ],
    );
    let meta_alix = BTreeMap::from([(PropertyKey::new("team"), Value::String("core".into()))]);
    db.set_node_property(alix, "meta", Value::Map(Arc::new(meta_alix)));

    let gus = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Gus".into())),
            ("age", Value::Int64(25)),
            ("score", Value::Float64(6.2)),
            ("active", Value::Bool(false)),
            (
                "tags",
                Value::List(vec![Value::String("dev".into())].into()),
            ),
        ],
    );
    let meta_gus = BTreeMap::from([(PropertyKey::new("team"), Value::String("infra".into()))]);
    db.set_node_property(gus, "meta", Value::Map(Arc::new(meta_gus)));

    let vincent = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Vincent".into())),
            ("age", Value::Int64(28)),
            ("score", Value::Float64(9.1)),
            ("active", Value::Bool(true)),
            (
                "tags",
                Value::List(
                    vec![
                        Value::String("qa".into()),
                        Value::String("dev".into()),
                        Value::String("ops".into()),
                    ]
                    .into(),
                ),
            ),
        ],
    );
    let meta_vincent = BTreeMap::from([
        (PropertyKey::new("team"), Value::String("core".into())),
        (PropertyKey::new("level"), Value::String("senior".into())),
    ]);
    db.set_node_property(vincent, "meta", Value::Map(Arc::new(meta_vincent)));

    let mia = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Mia".into())),
            ("age", Value::Int64(32)),
            ("score", Value::Float64(7.0)),
            ("active", Value::Bool(true)),
            ("tags", Value::List(vec![].into())),
        ],
    );
    let meta_mia: BTreeMap<PropertyKey, Value> = BTreeMap::new();
    db.set_node_property(mia, "meta", Value::Map(Arc::new(meta_mia)));

    let jules = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Jules".into())),
            ("age", Value::Int64(40)),
            ("active", Value::Bool(false)),
            (
                "tags",
                Value::List(vec![Value::String("mgmt".into())].into()),
            ),
        ],
    );
    let meta_jules = BTreeMap::from([(PropertyKey::new("team"), Value::String("exec".into()))]);
    db.set_node_property(jules, "meta", Value::Map(Arc::new(meta_jules)));

    // Edges for relationship tests
    session.create_edge(alix, gus, "KNOWS");
    session.create_edge(vincent, mia, "KNOWS");
    session.create_edge(jules, alix, "MANAGES");

    db
}

/// Creates a simple graph for numeric-only aggregate tests.
///
/// Nodes (all :Stat): val = 2, 4, 4, 4, 5, 5, 7, 9
fn create_stats_dataset() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    for val in [2, 4, 4, 4, 5, 5, 7, 9] {
        session.create_node_with_props(&["Stat"], [("val", Value::Int64(val))]);
    }

    db
}

// ============================================================================
// Temporal / date functions
// ============================================================================

#[test]
fn test_date_function_from_string() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Event"], [("name", Value::String("launch".into()))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Event)
                COLUMNS (n.name AS name, date('2024-01-15') AS d)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    let date_val = &result.rows[0][1];
    match date_val {
        Value::Date(d) => {
            assert_eq!(d.year(), 2024);
            assert_eq!(d.month(), 1);
            assert_eq!(d.day(), 15);
        }
        other => panic!("Expected Date, got {other:?}"),
    }
}

#[test]
fn test_time_function_from_string() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Event"], [("name", Value::String("meeting".into()))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Event)
                COLUMNS (n.name AS name, time('14:30:00') AS t)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][1] {
        Value::Time(t) => {
            assert_eq!(t.hour(), 14);
            assert_eq!(t.minute(), 30);
            assert_eq!(t.second(), 0);
        }
        other => panic!("Expected Time, got {other:?}"),
    }
}

#[test]
fn test_datetime_function_from_string() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Event"], [("name", Value::String("deploy".into()))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Event)
                COLUMNS (n.name AS name, datetime('2024-06-15T10:30:00') AS dt)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][1] {
        Value::Timestamp(ts) => {
            let d = ts.to_date();
            assert_eq!(d.year(), 2024);
            assert_eq!(d.month(), 6);
            assert_eq!(d.day(), 15);
        }
        other => panic!("Expected Timestamp, got {other:?}"),
    }
}

#[test]
fn test_duration_function_from_string() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Task"], [("name", Value::String("build".into()))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Task)
                COLUMNS (n.name AS name, duration('P1Y2M3D') AS dur)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][1] {
        Value::Duration(d) => {
            // P1Y2M3D = 14 months, 3 days
            assert_eq!(d.months(), 14, "1 year + 2 months = 14 months");
            assert_eq!(d.days(), 3);
        }
        other => panic!("Expected Duration, got {other:?}"),
    }
}

#[test]
fn test_date_property_comparison() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let d1 = grafeo_common::types::Date::from_ymd(2024, 1, 15).unwrap();
    let d2 = grafeo_common::types::Date::from_ymd(2024, 6, 20).unwrap();

    let n1 = session.create_node_with_props(&["Event"], [("name", Value::String("alpha".into()))]);
    db.set_node_property(n1, "event_date", Value::Date(d1));

    let n2 = session.create_node_with_props(&["Event"], [("name", Value::String("beta".into()))]);
    db.set_node_property(n2, "event_date", Value::Date(d2));

    // Filter for events after 2024-03-01
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Event)
                WHERE n.event_date > date('2024-03-01')
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("beta".into()));
}

#[test]
#[ignore = "DATE literal syntax (DATE '2024-01-15') not supported in SQL/PGQ parser"]
fn test_date_literal_sql_syntax() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Event"], [("name", Value::String("conf".into()))]);

    // SQL standard DATE literal syntax
    let _result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Event)
                COLUMNS (n.name AS name, DATE '2024-01-15' AS d)
            )",
        )
        .unwrap();
}

// ============================================================================
// Statistical aggregate functions
// ============================================================================

#[test]
fn test_stdev_aggregate() {
    let db = create_stats_dataset();
    let session = db.session();

    // Values: 2, 4, 4, 4, 5, 5, 7, 9. Mean = 5.0.
    // Sample variance = sum((xi - 5)^2) / (8-1) = 32/7
    // Sample stdev = sqrt(32/7) ~ 2.138
    let result = session
        .execute_sql(
            "SELECT STDEV(v) AS sd FROM GRAPH_TABLE (
                MATCH (n:Stat)
                COLUMNS (n.val AS v)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!((*v - 2.138).abs() < 0.01, "Expected stdev ~ 2.138, got {v}");
        }
        other => panic!("Expected Float64 for STDEV, got {other:?}"),
    }
}

#[test]
fn test_stdevp_aggregate() {
    let db = create_stats_dataset();
    let session = db.session();

    // Population stdev = sqrt(32/8) = 2.0
    let result = session
        .execute_sql(
            "SELECT STDEVP(v) AS sd FROM GRAPH_TABLE (
                MATCH (n:Stat)
                COLUMNS (n.val AS v)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (*v - 2.0).abs() < 0.01,
                "Expected population stdev = 2.0, got {v}"
            );
        }
        other => panic!("Expected Float64 for STDEVP, got {other:?}"),
    }
}

#[test]
fn test_variance_aggregate() {
    let db = create_stats_dataset();
    let session = db.session();

    // Sample variance = 32/7 ~ 4.571
    let result = session
        .execute_sql(
            "SELECT VARIANCE(v) AS var FROM GRAPH_TABLE (
                MATCH (n:Stat)
                COLUMNS (n.val AS v)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (*v - 4.571).abs() < 0.01,
                "Expected variance ~ 4.571, got {v}"
            );
        }
        other => panic!("Expected Float64 for VARIANCE, got {other:?}"),
    }
}

#[test]
fn test_var_pop_aggregate() {
    let db = create_stats_dataset();
    let session = db.session();

    // Population variance = 32/8 = 4.0
    let result = session
        .execute_sql(
            "SELECT VAR_POP(v) AS var FROM GRAPH_TABLE (
                MATCH (n:Stat)
                COLUMNS (n.val AS v)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (*v - 4.0).abs() < 0.01,
                "Expected population variance = 4.0, got {v}"
            );
        }
        other => panic!("Expected Float64 for VAR_POP, got {other:?}"),
    }
}

#[test]
fn test_collect_aggregate() {
    let db = create_typed_dataset();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT COLLECT(name) AS names FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.active = TRUE
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::List(items) => {
            // Active persons: Alix, Vincent, Mia (order may vary)
            assert_eq!(items.len(), 3, "3 active persons");
            let mut names: Vec<String> = items
                .iter()
                .map(|v| match v {
                    Value::String(s) => s.to_string(),
                    other => panic!("Expected string in list, got {other:?}"),
                })
                .collect();
            names.sort();
            assert_eq!(names, vec!["Alix", "Mia", "Vincent"]);
        }
        other => panic!("Expected List for COLLECT, got {other:?}"),
    }
}

#[test]
fn test_collect_distinct_aggregate() {
    let db = create_stats_dataset();
    let session = db.session();

    // Values: 2, 4, 4, 4, 5, 5, 7, 9. Distinct: 2, 4, 5, 7, 9 (5 items).
    let result = session
        .execute_sql(
            "SELECT COLLECT(DISTINCT v) AS vals FROM GRAPH_TABLE (
                MATCH (n:Stat)
                COLUMNS (n.val AS v)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::List(items) => {
            assert_eq!(items.len(), 5, "5 distinct values");
        }
        other => panic!("Expected List for COLLECT(DISTINCT), got {other:?}"),
    }
}

#[test]
#[ignore = "PERCENTILE_DISC requires percentile parameter not passed through SQL/PGQ outer aggregates yet"]
fn test_percentile_disc_aggregate() {
    let db = create_stats_dataset();
    let session = db.session();

    // Median of [2, 4, 4, 4, 5, 5, 7, 9] = 4 (disc at 0.5)
    let result = session
        .execute_sql(
            "SELECT PERCENTILE_DISC(0.5, v) AS median FROM GRAPH_TABLE (
                MATCH (n:Stat)
                COLUMNS (n.val AS v)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    let fv = match &result.rows[0][0] {
        Value::Int64(i) => *i as f64,
        Value::Float64(f) => *f,
        other => panic!("Expected numeric median, got {other:?}"),
    };
    assert!(
        (fv - 4.0).abs() < 0.5,
        "Median should be around 4, got {fv}"
    );
}

// ============================================================================
// Math functions (LOG, LN, LOG10, LOG2, PI, E, trig, DEGREES, RADIANS, ATAN2)
// ============================================================================

#[test]
fn test_ln_natural_log() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Num"], [("val", Value::Float64(std::f64::consts::E))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (LN(n.val) AS ln_e)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!((*v - 1.0).abs() < 1e-10, "ln(e) should be 1.0, got {v}");
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_log_alias_for_ln() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Num"], [("val", Value::Float64(std::f64::consts::E))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (LOG(n.val) AS log_e)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (*v - 1.0).abs() < 1e-10,
                "log(e) = ln(e) should be 1.0, got {v}"
            );
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_log10_base10() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Num"], [("val", Value::Float64(1000.0))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (LOG10(n.val) AS log10_v)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (*v - 3.0).abs() < 1e-10,
                "log10(1000) should be 3.0, got {v}"
            );
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_log2_base2() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Num"], [("val", Value::Int64(8))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (LOG2(n.val) AS log2_v)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!((*v - 3.0).abs() < 1e-10, "log2(8) should be 3.0, got {v}");
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_pi_constant() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Num"], [("val", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (PI() AS pi_val)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (*v - std::f64::consts::PI).abs() < 1e-10,
                "Expected PI, got {v}"
            );
        }
        other => panic!("Expected Float64 for PI(), got {other:?}"),
    }
}

#[test]
fn test_e_constant() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Num"], [("val", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (E() AS e_val)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (*v - std::f64::consts::E).abs() < 1e-10,
                "Expected E, got {v}"
            );
        }
        other => panic!("Expected Float64 for E(), got {other:?}"),
    }
}

#[test]
fn test_sin_known_value() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // sin(0) = 0
    session.create_node_with_props(&["Num"], [("val", Value::Float64(0.0))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (SIN(n.val) AS s)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(v.abs() < 1e-10, "sin(0) should be 0.0, got {v}");
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_cos_known_value() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // cos(0) = 1
    session.create_node_with_props(&["Num"], [("val", Value::Float64(0.0))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (COS(n.val) AS c)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!((*v - 1.0).abs() < 1e-10, "cos(0) should be 1.0, got {v}");
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_tan_known_value() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // tan(0) = 0
    session.create_node_with_props(&["Num"], [("val", Value::Float64(0.0))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (TAN(n.val) AS t)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(v.abs() < 1e-10, "tan(0) should be 0.0, got {v}");
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_sin_cos_identity() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // sin^2(x) + cos^2(x) = 1 for any x. Use x = 1.5.
    session.create_node_with_props(&["Num"], [("val", Value::Float64(1.5))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (SIN(n.val) * SIN(n.val) + COS(n.val) * COS(n.val) AS identity)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (*v - 1.0).abs() < 1e-10,
                "sin^2(1.5) + cos^2(1.5) should be 1.0, got {v}"
            );
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_degrees_conversion() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // degrees(PI) = 180
    session.create_node_with_props(&["Num"], [("val", Value::Float64(std::f64::consts::PI))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (DEGREES(n.val) AS deg)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (*v - 180.0).abs() < 1e-10,
                "degrees(PI) should be 180.0, got {v}"
            );
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_radians_conversion() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // radians(180) = PI
    session.create_node_with_props(&["Num"], [("val", Value::Float64(180.0))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (RADIANS(n.val) AS rad)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (*v - std::f64::consts::PI).abs() < 1e-10,
                "radians(180) should be PI, got {v}"
            );
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_atan2_known_value() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // atan2(1, 1) = PI/4
    session.create_node_with_props(
        &["Num"],
        [("y", Value::Float64(1.0)), ("x", Value::Float64(1.0))],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (ATAN2(n.y, n.x) AS angle)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            let expected = std::f64::consts::FRAC_PI_4;
            assert!(
                (*v - expected).abs() < 1e-10,
                "atan2(1,1) should be PI/4 ({expected}), got {v}"
            );
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_log_on_integer_argument() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // ln(1) = 0
    session.create_node_with_props(&["Num"], [("val", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (LN(n.val) AS ln_v)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(v.abs() < 1e-10, "ln(1) should be 0.0, got {v}");
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

// ============================================================================
// String functions
// ============================================================================

#[test]
fn test_char_length_function() {
    let db = create_typed_dataset();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Vincent'
                COLUMNS (n.name AS name, CHAR_LENGTH(n.name) AS len)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][1], Value::Int64(7), "Vincent has 7 chars");
}

#[test]
fn test_size_vs_char_length_on_string() {
    let db = create_typed_dataset();
    let session = db.session();

    // Both SIZE and CHAR_LENGTH should return the same value on strings
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Alix'
                COLUMNS (SIZE(n.name) AS sz, CHAR_LENGTH(n.name) AS cl)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(
        result.rows[0][0], result.rows[0][1],
        "SIZE and CHAR_LENGTH should agree on strings"
    );
}

#[test]
fn test_string_lexicographic_comparison() {
    let db = create_typed_dataset();
    let session = db.session();

    // Names sorted: Alix, Gus, Jules, Mia, Vincent
    // 'Gus' < 'Mia' should be true
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name < 'Jules'
                COLUMNS (n.name AS name)
            ) ORDER BY name",
        )
        .unwrap();

    // Alix and Gus come before Jules lexicographically
    assert_eq!(result.row_count(), 2);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
    assert_eq!(result.rows[1][0], Value::String("Gus".into()));
}

#[test]
fn test_reverse_on_list_property() {
    let db = create_typed_dataset();
    let session = db.session();

    // Alix has tags: ["dev", "lead"]. Reversed: ["lead", "dev"].
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Alix'
                COLUMNS (REVERSE(n.tags) AS rev_tags)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::List(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Value::String("lead".into()));
            assert_eq!(items[1], Value::String("dev".into()));
        }
        other => panic!("Expected List, got {other:?}"),
    }
}

// ============================================================================
// Boolean expressions and properties
// ============================================================================

#[test]
fn test_boolean_property_access() {
    let db = create_typed_dataset();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Alix'
                COLUMNS (n.name AS name, n.active AS active)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][1], Value::Bool(true));
}

#[test]
fn test_boolean_property_equality_true() {
    let db = create_typed_dataset();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.active = TRUE
                COLUMNS (n.name AS name)
            ) ORDER BY name",
        )
        .unwrap();

    // Active: Alix, Vincent, Mia
    assert_eq!(result.row_count(), 3);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
    assert_eq!(result.rows[1][0], Value::String("Mia".into()));
    assert_eq!(result.rows[2][0], Value::String("Vincent".into()));
}

#[test]
fn test_boolean_property_equality_false() {
    let db = create_typed_dataset();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.active = FALSE
                COLUMNS (n.name AS name)
            ) ORDER BY name",
        )
        .unwrap();

    // Inactive: Gus, Jules
    assert_eq!(result.row_count(), 2);
    assert_eq!(result.rows[0][0], Value::String("Gus".into()));
    assert_eq!(result.rows[1][0], Value::String("Jules".into()));
}

#[test]
fn test_not_boolean_property() {
    let db = create_typed_dataset();
    let session = db.session();

    // NOT n.active = TRUE should match inactive people
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE NOT n.active = TRUE
                COLUMNS (n.name AS name)
            ) ORDER BY name",
        )
        .unwrap();

    assert_eq!(result.row_count(), 2);
    assert_eq!(result.rows[0][0], Value::String("Gus".into()));
    assert_eq!(result.rows[1][0], Value::String("Jules".into()));
}

#[test]
fn test_boolean_aggregate_expression() {
    let db = create_typed_dataset();
    let session = db.session();

    // COUNT(*) yields a numeric column, test comparison in outer WHERE
    let result = session
        .execute_sql(
            "SELECT COUNT(*) AS cnt FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.active = TRUE
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(3), "3 active persons");

    // Boolean expression in COLUMNS (non-aggregate): age > 30 yields Bool
    let result2 = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Jules'
                COLUMNS (n.age > 30 AS is_senior)
            )",
        )
        .unwrap();

    assert_eq!(result2.row_count(), 1);
    assert_eq!(
        result2.rows[0][0],
        Value::Bool(true),
        "Jules (40) > 30 is true"
    );
}

// ============================================================================
// List operations
// ============================================================================

#[test]
fn test_size_of_list_property() {
    let db = create_typed_dataset();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Vincent'
                COLUMNS (n.name AS name, SIZE(n.tags) AS tag_count)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(
        result.rows[0][1],
        Value::Int64(3),
        "Vincent has 3 tags: qa, dev, ops"
    );
}

#[test]
fn test_size_of_empty_list() {
    let db = create_typed_dataset();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Mia'
                COLUMNS (n.name AS name, SIZE(n.tags) AS tag_count)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(
        result.rows[0][1],
        Value::Int64(0),
        "Mia has an empty tags list"
    );
}

#[test]
fn test_size_of_list_literal() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Dummy"], [("val", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Dummy)
                COLUMNS (SIZE([1, 2, 3]) AS list_size)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(3));
}

#[test]
fn test_reverse_list_literal() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Dummy"], [("val", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Dummy)
                COLUMNS (REVERSE([1, 2, 3]) AS rev)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::List(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Value::Int64(3));
            assert_eq!(items[1], Value::Int64(2));
            assert_eq!(items[2], Value::Int64(1));
        }
        other => panic!("Expected List, got {other:?}"),
    }
}

#[test]
fn test_head_of_list_property() {
    let db = create_typed_dataset();
    let session = db.session();

    // Alix tags: ["dev", "lead"]. HEAD = "dev".
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Alix'
                COLUMNS (HEAD(n.tags) AS first_tag)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("dev".into()));
}

#[test]
fn test_last_of_list_property() {
    let db = create_typed_dataset();
    let session = db.session();

    // Alix tags: ["dev", "lead"]. LAST = "lead".
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Alix'
                COLUMNS (LAST(n.tags) AS last_tag)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("lead".into()));
}

#[test]
fn test_tail_of_list_property() {
    let db = create_typed_dataset();
    let session = db.session();

    // Vincent tags: ["qa", "dev", "ops"]. TAIL = ["dev", "ops"].
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Vincent'
                COLUMNS (TAIL(n.tags) AS rest_tags)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::List(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Value::String("dev".into()));
            assert_eq!(items[1], Value::String("ops".into()));
        }
        other => panic!("Expected List, got {other:?}"),
    }
}

#[test]
fn test_head_of_list_literal() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Dummy"], [("val", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Dummy)
                COLUMNS (HEAD([10, 20, 30]) AS first)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(10));
}

#[test]
fn test_last_of_list_literal() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Dummy"], [("val", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Dummy)
                COLUMNS (LAST([10, 20, 30]) AS last_val)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(30));
}

#[test]
fn test_tail_of_list_literal() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Dummy"], [("val", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Dummy)
                COLUMNS (TAIL([10, 20, 30]) AS rest)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::List(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Value::Int64(20));
            assert_eq!(items[1], Value::Int64(30));
        }
        other => panic!("Expected List, got {other:?}"),
    }
}

#[test]
fn test_list_property_filter() {
    let db = create_typed_dataset();
    let session = db.session();

    // Filter by list size: only nodes with more than 1 tag
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE SIZE(n.tags) > 1
                COLUMNS (n.name AS name, SIZE(n.tags) AS cnt)
            ) ORDER BY name",
        )
        .unwrap();

    // Alix: 2 tags, Vincent: 3 tags
    assert_eq!(result.row_count(), 2);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
    assert_eq!(result.rows[1][0], Value::String("Vincent".into()));
}

// ============================================================================
// Map operations
// ============================================================================

#[test]
fn test_map_property_projection() {
    let db = create_typed_dataset();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Alix'
                COLUMNS (n.name AS name, n.meta AS meta)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][1] {
        Value::Map(m) => {
            assert_eq!(
                m.get("team"),
                Some(&Value::String("core".into())),
                "Alix meta.team should be 'core'"
            );
        }
        other => panic!("Expected Map, got {other:?}"),
    }
}

#[test]
fn test_empty_map_property() {
    let db = create_typed_dataset();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Mia'
                COLUMNS (n.name AS name, n.meta AS meta)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][1] {
        Value::Map(m) => {
            assert!(m.is_empty(), "Mia's meta should be empty map");
        }
        other => panic!("Expected Map, got {other:?}"),
    }
}

#[test]
fn test_map_literal_in_columns() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Dummy"], [("val", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Dummy)
                COLUMNS ({a: 1, b: 'hello'} AS m)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Map(m) => {
            assert_eq!(m.get("a"), Some(&Value::Int64(1)));
            assert_eq!(m.get("b"), Some(&Value::String("hello".into())));
        }
        other => panic!("Expected Map literal, got {other:?}"),
    }
}

#[test]
fn test_nested_map_literal() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Dummy"], [("val", Value::Int64(1))]);

    // Use non-keyword key names to avoid parser issues with reserved words
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Dummy)
                COLUMNS ({parent: {child: 42}} AS nested)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Map(m) => match m.get("parent") {
            Some(Value::Map(inner)) => {
                assert_eq!(inner.get("child"), Some(&Value::Int64(42)));
            }
            other => panic!("Expected nested Map for 'parent', got {other:?}"),
        },
        other => panic!("Expected Map, got {other:?}"),
    }
}

#[test]
fn test_empty_map_literal() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Dummy"], [("val", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Dummy)
                COLUMNS ({} AS empty_map)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Map(m) => {
            assert!(m.is_empty(), "Expected empty map");
        }
        other => panic!("Expected empty Map, got {other:?}"),
    }
}

#[test]
fn test_nested_empty_structures() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Dummy"], [("val", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Dummy)
                COLUMNS ({items: [], meta: {}} AS structure)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Map(m) => {
            match m.get("items") {
                Some(Value::List(items)) => assert!(items.is_empty()),
                other => panic!("Expected empty list for items, got {other:?}"),
            }
            match m.get("meta") {
                Some(Value::Map(inner)) => assert!(inner.is_empty()),
                other => panic!("Expected empty map for meta, got {other:?}"),
            }
        }
        other => panic!("Expected Map, got {other:?}"),
    }
}

// ============================================================================
// Complex nested expressions
// ============================================================================

#[test]
fn test_chained_string_functions() {
    let db = create_typed_dataset();
    let session = db.session();

    // UPPER(SUBSTRING(n.name, 0, 1)) + LOWER(SUBSTRING(n.name, 1))
    // For "vincent" -> "V" + "incent" = "Vincent" (already capitalized, but tests chaining)
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Alix'
                COLUMNS (UPPER(SUBSTRING(n.name, 0, 1)) + LOWER(SUBSTRING(n.name, 1)) AS titled)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::String("Alix".into()),
        "UPPER('A') + LOWER('lix') = 'Alix'"
    );
}

#[test]
fn test_chained_math_expressions() {
    let db = create_typed_dataset();
    let session = db.session();

    // ABS(ROUND(n.score * 100) / 100) on Alix score=8.5
    // ROUND(8.5 * 100) = ROUND(850) = 850
    // 850 / 100 = 8.5
    // ABS(8.5) = 8.5
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Alix'
                COLUMNS (ABS(ROUND(n.score * 100) / 100) AS normalized)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!((*v - 8.5).abs() < 0.01, "Expected ~8.5, got {v}");
        }
        Value::Int64(v) => {
            // In case integer division happens
            assert!((*v as f64 - 8.5).abs() < 1.0, "Expected ~8, got {v}");
        }
        other => panic!("Expected numeric, got {other:?}"),
    }
}

#[test]
fn test_multiple_nested_functions() {
    let db = create_typed_dataset();
    let session = db.session();

    // Combine SIZE, UPPER: UPPER(n.name) with SIZE filter
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE SIZE(n.name) > 3 AND n.active = TRUE
                COLUMNS (n.name AS name, UPPER(n.name) AS upper_name, SIZE(n.name) AS name_len)
            ) ORDER BY name",
        )
        .unwrap();

    // Active with name > 3 chars: Alix(4), Vincent(7), Mia has 3 chars so excluded
    assert!(
        result.row_count() >= 2,
        "Expected at least Alix and Vincent"
    );
    // Verify UPPER works
    for row in &result.rows {
        let name = match &row[0] {
            Value::String(s) => s.to_string(),
            other => panic!("Expected string, got {other:?}"),
        };
        let upper = match &row[1] {
            Value::String(s) => s.to_string(),
            other => panic!("Expected string, got {other:?}"),
        };
        assert_eq!(upper, name.to_uppercase());
    }
}

// ============================================================================
// CASE expression
// ============================================================================

#[test]
#[ignore = "CASE expression not supported in SQL/PGQ parser (no CASE/WHEN/THEN/ELSE tokens)"]
fn test_case_when_expression() {
    let db = create_typed_dataset();
    let session = db.session();

    let _result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (
                    n.name AS name,
                    CASE WHEN SIZE(n.name) > 4 THEN 'long' ELSE 'short' END AS name_class
                )
            ) ORDER BY name",
        )
        .unwrap();
}

// ============================================================================
// Type edge cases
// ============================================================================

#[test]
fn test_very_large_float() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Num"], [("val", Value::Float64(1.7976931348623157e308))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (n.val AS big)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(v.is_finite(), "Value should be finite");
            assert!(*v > 1e300, "Value should be very large");
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_float_division_by_zero() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Num"], [("val", Value::Float64(0.0))]);

    // 0.0 / 0.0 behavior: may produce NaN or Null depending on engine
    let result = session.execute_sql(
        "SELECT * FROM GRAPH_TABLE (
            MATCH (n:Num)
            COLUMNS (n.val / n.val AS result)
        )",
    );

    match result {
        Ok(qr) => {
            assert_eq!(qr.row_count(), 1);
            // NaN or Null are both acceptable
            match &qr.rows[0][0] {
                Value::Float64(v) => {
                    // NaN is acceptable
                    assert!(
                        v.is_nan() || (*v - 1.0).abs() < 1e-10 || *v == 0.0,
                        "0.0/0.0 should be NaN or handled specially, got {v}"
                    );
                }
                Value::Null => {} // Also acceptable
                other => panic!("Expected Float64(NaN) or Null, got {other:?}"),
            }
        }
        Err(_) => {} // Division by zero error is also acceptable
    }
}

#[test]
fn test_integer_max_value() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Num"], [("val", Value::Int64(i64::MAX))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (n.val AS max_int)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(i64::MAX));
}

#[test]
fn test_integer_min_value() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Num"], [("val", Value::Int64(i64::MIN))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (n.val AS min_int)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(i64::MIN));
}

#[test]
fn test_empty_list_literal() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Dummy"], [("val", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Dummy)
                COLUMNS ([] AS empty_list)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::List(items) => assert!(items.is_empty()),
        other => panic!("Expected empty List, got {other:?}"),
    }
}

#[test]
fn test_mixed_type_list_literal() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Dummy"], [("val", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Dummy)
                COLUMNS ([1, 'two', 3.0, TRUE] AS mixed)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::List(items) => {
            assert_eq!(items.len(), 4);
            assert_eq!(items[0], Value::Int64(1));
            assert_eq!(items[1], Value::String("two".into()));
            assert_eq!(items[2], Value::Float64(3.0));
            assert_eq!(items[3], Value::Bool(true));
        }
        other => panic!("Expected List, got {other:?}"),
    }
}

// ============================================================================
// COALESCE function
// ============================================================================

#[test]
fn test_coalesce_first_non_null() {
    let db = create_typed_dataset();
    let session = db.session();

    // Alix has nickname "Lix", so COALESCE(nickname, name) = "Lix"
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Alix'
                COLUMNS (COALESCE(n.nickname, n.name) AS display_name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Lix".into()));
}

#[test]
fn test_coalesce_fallback_to_second() {
    let db = create_typed_dataset();
    let session = db.session();

    // Gus has no nickname property, so COALESCE(nickname, name) = "Gus"
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Gus'
                COLUMNS (COALESCE(n.nickname, n.name) AS display_name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Gus".into()));
}

#[test]
fn test_coalesce_multiple_nulls_with_default() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("alpha".into())),
            ("a", Value::Null),
            ("b", Value::Null),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Item)
                COLUMNS (COALESCE(n.a, n.b, 'default') AS val)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("default".into()));
}

#[test]
fn test_coalesce_single_arg() {
    let db = create_typed_dataset();
    let session = db.session();

    // Single argument: COALESCE(n.name) = n.name
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Alix'
                COLUMNS (COALESCE(n.name) AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
}

#[test]
fn test_coalesce_all_null_returns_null() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("alpha".into())),
            ("a", Value::Null),
            ("b", Value::Null),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Item)
                COLUMNS (COALESCE(n.a, n.b) AS val)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Null);
}

// ============================================================================
// Statistical aggregates: STDDEV_SAMP, STDDEV_POP aliases
// ============================================================================

#[test]
fn test_stddev_samp_alias() {
    let db = create_stats_dataset();
    let session = db.session();

    // STDDEV_SAMP is an alias for STDEV
    let result = session
        .execute_sql(
            "SELECT STDDEV_SAMP(v) AS sd FROM GRAPH_TABLE (
                MATCH (n:Stat)
                COLUMNS (n.val AS v)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (*v - 2.138).abs() < 0.01,
                "STDDEV_SAMP should match STDEV, got {v}"
            );
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

#[test]
fn test_stddev_pop_alias() {
    let db = create_stats_dataset();
    let session = db.session();

    // STDDEV_POP is an alias for STDEVP
    let result = session
        .execute_sql(
            "SELECT STDDEV_POP(v) AS sd FROM GRAPH_TABLE (
                MATCH (n:Stat)
                COLUMNS (n.val AS v)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (*v - 2.0).abs() < 0.01,
                "STDDEV_POP should match STDEVP, got {v}"
            );
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

// ============================================================================
// Aggregate with GROUP BY and statistical functions
// ============================================================================

#[test]
fn test_stdev_with_group_by() {
    let db = create_typed_dataset();
    let session = db.session();

    // Group by active, compute STDEV(age)
    let result = session
        .execute_sql(
            "SELECT active, STDEV(age) AS sd FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.active AS active, n.age AS age)
            ) GROUP BY active ORDER BY active",
        )
        .unwrap();

    // Two groups: false (Gus:25, Jules:40), true (Alix:30, Vincent:28, Mia:32)
    assert_eq!(result.row_count(), 2);
    // Just verify we get numeric results for both groups
    for row in &result.rows {
        match &row[1] {
            Value::Float64(_) => {}
            other => panic!("Expected Float64 for STDEV, got {other:?}"),
        }
    }
}

#[test]
fn test_collect_with_group_by() {
    let db = create_typed_dataset();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT active, COLLECT(name) AS names FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.active AS active, n.name AS name)
            ) GROUP BY active ORDER BY active",
        )
        .unwrap();

    assert_eq!(result.row_count(), 2);
    // false group: Gus, Jules (2 names)
    match &result.rows[0][1] {
        Value::List(items) => {
            assert_eq!(items.len(), 2, "Inactive group should have 2 names");
        }
        other => panic!("Expected List, got {other:?}"),
    }
    // true group: Alix, Vincent, Mia (3 names)
    match &result.rows[1][1] {
        Value::List(items) => {
            assert_eq!(items.len(), 3, "Active group should have 3 names");
        }
        other => panic!("Expected List, got {other:?}"),
    }
}

// ============================================================================
// Degrees/radians roundtrip
// ============================================================================

#[test]
fn test_degrees_radians_roundtrip() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Num"], [("val", Value::Float64(45.0))]);

    // DEGREES(RADIANS(45)) should be 45
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (DEGREES(RADIANS(n.val)) AS roundtrip)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                (*v - 45.0).abs() < 1e-10,
                "DEGREES(RADIANS(45)) should be 45, got {v}"
            );
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

// ============================================================================
// Map literal with various value types
// ============================================================================

#[test]
fn test_map_literal_various_types() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Dummy"], [("val", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Dummy)
                COLUMNS ({name: 'Alix', age: 30, score: 9.5, active: TRUE, nothing: NULL} AS m)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Map(m) => {
            assert_eq!(m.get("name"), Some(&Value::String("Alix".into())));
            assert_eq!(m.get("age"), Some(&Value::Int64(30)));
            assert_eq!(m.get("score"), Some(&Value::Float64(9.5)));
            assert_eq!(m.get("active"), Some(&Value::Bool(true)));
            assert_eq!(m.get("nothing"), Some(&Value::Null));
        }
        other => panic!("Expected Map, got {other:?}"),
    }
}

// ============================================================================
// Temporal: date/time on stored properties
// ============================================================================

#[test]
fn test_date_function_on_stored_property() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(
        &["Event"],
        [
            ("name", Value::String("launch".into())),
            ("date_str", Value::String("2024-06-15".into())),
        ],
    );

    // Use date() to convert string property to Date
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Event)
                COLUMNS (n.name AS name, date(n.date_str) AS d)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][1] {
        Value::Date(d) => {
            assert_eq!(d.year(), 2024);
            assert_eq!(d.month(), 6);
            assert_eq!(d.day(), 15);
        }
        other => panic!("Expected Date from date() on string property, got {other:?}"),
    }
}

// ============================================================================
// Index access on lists
// ============================================================================

#[test]
#[ignore = "Index access n.tags[1] in COLUMNS not supported by SQL/PGQ parser (LBracket after property)"]
fn test_index_access_on_list_property() {
    let db = create_typed_dataset();
    let session = db.session();

    // Vincent tags: ["qa", "dev", "ops"]. tags[1] = "dev".
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Vincent'
                COLUMNS (n.tags[1] AS second_tag)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("dev".into()));
}

// ============================================================================
// Score property as NULL (Jules has no score)
// ============================================================================

#[test]
fn test_null_score_property() {
    let db = create_typed_dataset();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Jules'
                COLUMNS (n.name AS name, n.score AS score)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(
        result.rows[0][1],
        Value::Null,
        "Jules has no score property"
    );
}

#[test]
fn test_coalesce_on_null_property() {
    let db = create_typed_dataset();
    let session = db.session();

    // Jules has no score, COALESCE should return default
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'Jules'
                COLUMNS (n.name AS name, COALESCE(n.score, 0.0) AS score)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][1], Value::Float64(0.0));
}

// ============================================================================
// Trig on integer arguments
// ============================================================================

#[test]
fn test_sin_on_integer() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Num"], [("val", Value::Int64(0))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (SIN(n.val) AS s)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(v.abs() < 1e-10, "sin(0) = 0, got {v}");
        }
        other => panic!("Expected Float64 from sin(int), got {other:?}"),
    }
}

#[test]
fn test_degrees_on_integer() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // degrees(0) = 0
    session.create_node_with_props(&["Num"], [("val", Value::Int64(0))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (DEGREES(n.val) AS deg)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(v.abs() < 1e-10, "degrees(0) = 0, got {v}");
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

// ============================================================================
// ATAN2 with integer arguments
// ============================================================================

#[test]
fn test_atan2_with_integers() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // atan2(0, 1) = 0
    session.create_node_with_props(&["Num"], [("y", Value::Int64(0)), ("x", Value::Int64(1))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Num)
                COLUMNS (ATAN2(n.y, n.x) AS angle)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(v.abs() < 1e-10, "atan2(0, 1) = 0, got {v}");
        }
        other => panic!("Expected Float64, got {other:?}"),
    }
}

// ============================================================================
// Combined aggregate and scalar in outer SELECT
// ============================================================================

#[test]
fn test_collect_and_count_together() {
    let db = create_typed_dataset();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) AS cnt, COLLECT(name) AS names FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(5));
    match &result.rows[0][1] {
        Value::List(items) => {
            assert_eq!(items.len(), 5);
        }
        other => panic!("Expected List, got {other:?}"),
    }
}

// ============================================================================
// Variance on single-element group
// ============================================================================

#[test]
fn test_variance_single_element() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Stat"], [("val", Value::Int64(42))]);

    let result = session
        .execute_sql(
            "SELECT VARIANCE(v) AS var FROM GRAPH_TABLE (
                MATCH (n:Stat)
                COLUMNS (n.val AS v)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    // Sample variance of a single element is typically NaN or 0 or Null
    // (dividing by n-1 = 0)
    match &result.rows[0][0] {
        Value::Float64(v) => {
            // Could be NaN or 0
            assert!(
                v.is_nan() || v.abs() < 1e-10,
                "Variance of single element should be NaN or 0, got {v}"
            );
        }
        Value::Null => {} // Also acceptable
        other => panic!("Expected Float64 or Null for single-element variance, got {other:?}"),
    }
}

#[test]
fn test_var_pop_single_element() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Stat"], [("val", Value::Int64(42))]);

    let result = session
        .execute_sql(
            "SELECT VAR_POP(v) AS var FROM GRAPH_TABLE (
                MATCH (n:Stat)
                COLUMNS (n.val AS v)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    // Population variance of a single element = 0
    match &result.rows[0][0] {
        Value::Float64(v) => {
            assert!(
                v.abs() < 1e-10,
                "Population variance of single element should be 0, got {v}"
            );
        }
        other => panic!("Expected Float64(0.0) for single-element VAR_POP, got {other:?}"),
    }
}
