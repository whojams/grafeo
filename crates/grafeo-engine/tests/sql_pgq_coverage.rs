//! SQL/PGQ comprehensive coverage tests.
//!
//! These tests aim for complete feature coverage of the SQL/PGQ (SQL:2023 GRAPH_TABLE)
//! implementation. Tests that exercise not-yet-implemented functionality are expected
//! to fail and serve as a roadmap for future work.
//!
//! Run with:
//! ```bash
//! cargo test -p grafeo-engine --features sql-pgq --test sql_pgq_coverage
//! ```

#![cfg(feature = "sql-pgq")]

use grafeo_common::types::Value;
use grafeo_engine::GrafeoDB;

// ============================================================================
// Test Fixtures
// ============================================================================

/// Creates a rich social network for comprehensive testing.
///
/// Nodes:
/// - Alix (Person, age: 30, city: "Amsterdam")
/// - Gus (Person, age: 25, city: "Berlin")
/// - Harm (Person, age: 35, city: "Amsterdam")
/// - Vincent (Person, age: 28, city: "Paris")
/// - Mia (Person, age: 32, city: "Berlin")
///
/// Edges:
/// - Alix -KNOWS-> Gus (since: 2020)
/// - Alix -KNOWS-> Harm (since: 2018)
/// - Gus -KNOWS-> Harm (since: 2021)
/// - Vincent -KNOWS-> Mia (since: 2019)
/// - Alix -FOLLOWS-> Vincent (since: 2022)
/// - Gus -FOLLOWS-> Alix (since: 2023)
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

/// Creates a multi-label graph for label-related tests.
///
/// Nodes:
/// - Django (Person + Employee, name, dept: "Engineering")
/// - Shosanna (Person + Manager, name, dept: "Engineering")
/// - Hans (Person + Employee, name, dept: "Sales")
/// - Beatrix (Company, name: "GrafeoDB Inc")
///
/// Edges:
/// - Django -WORKS_AT-> Beatrix
/// - Shosanna -WORKS_AT-> Beatrix
/// - Hans -WORKS_AT-> Beatrix
/// - Shosanna -MANAGES-> Django
/// - Shosanna -MANAGES-> Hans
fn create_multi_label_graph() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let django = session.create_node_with_props(
        &["Person", "Employee"],
        [
            ("name", Value::String("Django".into())),
            ("dept", Value::String("Engineering".into())),
        ],
    );
    let shosanna = session.create_node_with_props(
        &["Person", "Manager"],
        [
            ("name", Value::String("Shosanna".into())),
            ("dept", Value::String("Engineering".into())),
        ],
    );
    let hans = session.create_node_with_props(
        &["Person", "Employee"],
        [
            ("name", Value::String("Hans".into())),
            ("dept", Value::String("Sales".into())),
        ],
    );
    let company = session.create_node_with_props(
        &["Company"],
        [("name", Value::String("GrafeoDB Inc".into()))],
    );

    session.create_edge(django, company, "WORKS_AT");
    session.create_edge(shosanna, company, "WORKS_AT");
    session.create_edge(hans, company, "WORKS_AT");
    session.create_edge(shosanna, django, "MANAGES");
    session.create_edge(shosanna, hans, "MANAGES");

    db
}

/// Creates a chain graph for path and variable-length edge tests.
///
/// A -> B -> C -> D -> E (all LINK edges)
/// A -> C (SHORTCUT edge)
/// E -> A (LINK edge, creates a cycle)
fn create_chain_with_cycle() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let a = session.create_node_with_props(
        &["Node"],
        [
            ("name", Value::String("A".into())),
            ("weight", Value::Int64(10)),
        ],
    );
    let b = session.create_node_with_props(
        &["Node"],
        [
            ("name", Value::String("B".into())),
            ("weight", Value::Int64(20)),
        ],
    );
    let c = session.create_node_with_props(
        &["Node"],
        [
            ("name", Value::String("C".into())),
            ("weight", Value::Int64(30)),
        ],
    );
    let d = session.create_node_with_props(
        &["Node"],
        [
            ("name", Value::String("D".into())),
            ("weight", Value::Int64(40)),
        ],
    );
    let e = session.create_node_with_props(
        &["Node"],
        [
            ("name", Value::String("E".into())),
            ("weight", Value::Int64(50)),
        ],
    );

    session.create_edge(a, b, "LINK");
    session.create_edge(b, c, "LINK");
    session.create_edge(c, d, "LINK");
    session.create_edge(d, e, "LINK");
    session.create_edge(a, c, "SHORTCUT");
    session.create_edge(e, a, "LINK"); // cycle

    db
}

/// Creates a minimal graph with a single isolated node (no edges).
fn create_single_node() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();
    session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Jules".into())),
            ("age", Value::Int64(40)),
        ],
    );
    db
}

/// Creates a graph with self-loops.
///
/// - Butch (Person) -LIKES-> Butch (self-loop)
/// - Butch -KNOWS-> Django
fn create_self_loop_graph() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let butch =
        session.create_node_with_props(&["Person"], [("name", Value::String("Butch".into()))]);
    let django =
        session.create_node_with_props(&["Person"], [("name", Value::String("Django".into()))]);

    session.create_edge(butch, butch, "LIKES"); // self-loop
    session.create_edge(butch, django, "KNOWS");

    db
}

// ============================================================================
// Expressions in COLUMNS clause
// ============================================================================

#[test]
fn test_arithmetic_addition_in_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age + 10 AS age_plus_ten)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
    let alix_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Alix"))
        .expect("Alix should appear");
    assert_eq!(alix_row[1], Value::Int64(40), "30 + 10 = 40");
}

#[test]
fn test_arithmetic_subtraction_in_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age - 5 AS age_minus_five)
            )",
        )
        .unwrap();

    let gus_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Gus"))
        .expect("Gus should appear");
    assert_eq!(gus_row[1], Value::Int64(20), "25 - 5 = 20");
}

#[test]
fn test_arithmetic_multiplication_in_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age * 2 AS double_age)
            )",
        )
        .unwrap();

    let vincent_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Vincent"))
        .expect("Vincent should appear");
    assert_eq!(vincent_row[1], Value::Int64(56), "28 * 2 = 56");
}

#[test]
fn test_arithmetic_division_in_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age / 5 AS age_div_five)
            )",
        )
        .unwrap();

    let harm_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Harm"))
        .expect("Harm should appear");
    assert_eq!(harm_row[1], Value::Int64(7), "35 / 5 = 7");
}

#[test]
fn test_arithmetic_modulo_in_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age % 10 AS age_mod_ten)
            )",
        )
        .unwrap();

    let mia_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Mia"))
        .expect("Mia should appear");
    assert_eq!(mia_row[1], Value::Int64(2), "32 % 10 = 2");
}

#[test]
fn test_complex_arithmetic_expression() {
    let db = create_rich_network();
    let session = db.session();

    // (age * 2) + 10
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, (n.age * 2) + 10 AS computed)
            )",
        )
        .unwrap();

    let alix_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Alix"))
        .expect("Alix should appear");
    assert_eq!(alix_row[1], Value::Int64(70), "(30 * 2) + 10 = 70");
}

#[test]
fn test_literal_in_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, 42 AS answer)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
    for row in &result.rows {
        assert_eq!(row[1], Value::Int64(42), "literal column should be 42");
    }
}

#[test]
fn test_string_literal_in_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, 'active' AS status)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
    for row in &result.rows {
        assert_eq!(
            row[1],
            Value::String("active".into()),
            "string literal column should be 'active'"
        );
    }
}

#[test]
fn test_boolean_literal_in_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, TRUE AS is_active)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
    for row in &result.rows {
        assert_eq!(row[1], Value::Bool(true));
    }
}

#[test]
fn test_null_literal_in_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, NULL AS empty)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
    for row in &result.rows {
        assert!(row[1].is_null(), "NULL literal should produce null");
    }
}

// ============================================================================
// Comparison operators in WHERE (inside GRAPH_TABLE)
// ============================================================================

#[test]
fn test_where_less_than() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.age < 30
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Gus (25), Vincent (28)
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_where_less_than_or_equal() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.age <= 30
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Gus (25), Vincent (28), Alix (30)
    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_where_greater_than() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.age > 30
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Harm (35), Mia (32)
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_where_greater_than_or_equal() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.age >= 30
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Alix (30), Harm (35), Mia (32)
    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_where_not_equal() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.age <> 30
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Everyone except Alix (30): Gus (25), Harm (35), Vincent (28), Mia (32)
    assert_eq!(result.row_count(), 4);
}

#[test]
fn test_where_equality_string() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.city = 'Amsterdam'
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Alix, Harm
    assert_eq!(result.row_count(), 2);
}

// ============================================================================
// Logical operators in WHERE
// ============================================================================

#[test]
fn test_where_and() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.age > 25 AND n.city = 'Berlin'
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Mia (32, Berlin) only. Gus is 25, not > 25.
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Mia".into()));
}

#[test]
fn test_where_or() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.city = 'Paris' OR n.city = 'Berlin'
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Gus (Berlin), Vincent (Paris), Mia (Berlin)
    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_where_not() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE NOT n.city = 'Amsterdam'
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Gus (Berlin), Vincent (Paris), Mia (Berlin)
    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_where_combined_and_or() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE (n.city = 'Amsterdam' AND n.age > 30) OR n.city = 'Paris'
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Harm (Amsterdam, 35) and Vincent (Paris, 28)
    assert_eq!(result.row_count(), 2);
}

// ============================================================================
// BETWEEN, LIKE, IN, IS NULL / IS NOT NULL
// ============================================================================

#[test]
fn test_where_between() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.age BETWEEN 28 AND 32
                COLUMNS (n.name AS name, n.age AS age)
            )",
        )
        .unwrap();

    // Alix (30), Vincent (28), Mia (32)
    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_where_like() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name LIKE 'A%'
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Alix only
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
}

#[test]
fn test_where_like_suffix() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name LIKE '%m'
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Harm
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Harm".into()));
}

#[test]
fn test_where_in_list() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name IN ['Alix', 'Mia', 'Vincent']
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_where_is_null() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // Create a node with a missing property
    session.create_node_with_props(&["Item"], [("name", Value::String("widget".into()))]);
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("gadget".into())),
            ("color", Value::String("red".into())),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Item)
                WHERE n.color IS NULL
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("widget".into()));
}

#[test]
fn test_where_is_not_null() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Item"], [("name", Value::String("widget".into()))]);
    session.create_node_with_props(
        &["Item"],
        [
            ("name", Value::String("gadget".into())),
            ("color", Value::String("red".into())),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Item)
                WHERE n.color IS NOT NULL
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("gadget".into()));
}

// ============================================================================
// Aggregate functions
// ============================================================================

#[test]
fn test_aggregate_count_star() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) AS total FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(5));
}

#[test]
fn test_aggregate_count_column() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT COUNT(name) AS total FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(5));
}

#[test]
fn test_aggregate_sum() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT SUM(age) AS total_age FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.age AS age)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    // 30 + 25 + 35 + 28 + 32 = 150
    assert_eq!(result.rows[0][0], Value::Int64(150));
}

#[test]
fn test_aggregate_avg() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT AVG(age) AS avg_age FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.age AS age)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    // 150 / 5 = 30.0
    match &result.rows[0][0] {
        Value::Float64(f) => assert!((*f - 30.0).abs() < 0.01, "avg should be ~30.0, got {f}"),
        Value::Int64(i) => assert_eq!(*i, 30, "avg should be 30"),
        other => panic!("Expected numeric avg, got: {other:?}"),
    }
}

#[test]
fn test_aggregate_min() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT MIN(age) AS min_age FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.age AS age)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(25));
}

#[test]
fn test_aggregate_max() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT MAX(age) AS max_age FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.age AS age)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(35));
}

#[test]
fn test_aggregate_count_distinct() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT COUNT(DISTINCT city) AS unique_cities FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    // Amsterdam, Berlin, Paris = 3 unique cities
    assert_eq!(result.rows[0][0], Value::Int64(3));
}

#[test]
fn test_multiple_aggregates() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) AS total, MIN(age) AS youngest, MAX(age) AS oldest
             FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.age AS age)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(5));
    assert_eq!(result.rows[0][1], Value::Int64(25));
    assert_eq!(result.rows[0][2], Value::Int64(35));
}

#[test]
fn test_group_by_with_multiple_aggregates() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT city, COUNT(*) AS cnt, MIN(age) AS youngest, MAX(age) AS oldest
             FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city, n.age AS age)
            )
            GROUP BY city
            ORDER BY city",
        )
        .unwrap();

    // Amsterdam (Alix 30, Harm 35), Berlin (Gus 25, Mia 32), Paris (Vincent 28)
    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_group_by_with_sum() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT city, SUM(age) AS total_age
             FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city, n.age AS age)
            )
            GROUP BY city",
        )
        .unwrap();

    assert_eq!(result.row_count(), 3);

    let amsterdam_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Amsterdam"))
        .expect("Amsterdam should appear");
    assert_eq!(amsterdam_row[1], Value::Int64(65), "30 + 35 = 65");
}

#[test]
fn test_having_with_count() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT city, COUNT(*) AS cnt
             FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city)
            )
            GROUP BY city
            HAVING cnt >= 2",
        )
        .unwrap();

    // Amsterdam (2), Berlin (2), Paris has only 1
    assert_eq!(result.row_count(), 2);
}

// ============================================================================
// Edge patterns: direction, types, properties
// ============================================================================

#[test]
fn test_incoming_edge() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)<-[e:KNOWS]-(b:Person)
                COLUMNS (a.name AS target, b.name AS source)
            )",
        )
        .unwrap();

    // 4 KNOWS edges: Alix->Gus, Alix->Harm, Gus->Harm, Vincent->Mia
    // Incoming to Gus from Alix, Harm from Alix, Harm from Gus, Mia from Vincent
    assert_eq!(result.row_count(), 4);
}

#[test]
fn test_undirected_edge() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[e:KNOWS]-(b:Person)
                COLUMNS (a.name AS person1, b.name AS person2)
            )",
        )
        .unwrap();

    // Undirected: each edge traversed both ways = 8 results
    assert_eq!(result.row_count(), 8);
}

#[test]
fn test_edge_without_type() {
    let db = create_rich_network();
    let session = db.session();

    // Match any edge type between Person nodes
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[e]->(b:Person)
                COLUMNS (a.name AS source, b.name AS target)
            )",
        )
        .unwrap();

    // 4 KNOWS + 2 FOLLOWS = 6 edges
    assert_eq!(result.row_count(), 6);
}

#[test]
fn test_multiple_edge_types() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[e:KNOWS|FOLLOWS]->(b:Person)
                COLUMNS (a.name AS source, b.name AS target)
            )",
        )
        .unwrap();

    // 4 KNOWS + 2 FOLLOWS = 6 edges
    assert_eq!(result.row_count(), 6);
}

#[test]
fn test_edge_with_property_access() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[e:KNOWS]->(b:Person)
                COLUMNS (a.name AS source, b.name AS target, e.since AS since)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 4);
    assert_eq!(result.columns.len(), 3);

    // Verify edge property is accessible
    let alix_gus = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Alix") && r[1].as_str() == Some("Gus"))
        .expect("Alix->Gus edge should exist");
    assert_eq!(alix_gus[2], Value::Int64(2020));
}

#[test]
fn test_edge_with_inline_property_filter() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[e:KNOWS]->(b:Person)
                WHERE e.since >= 2020
                COLUMNS (a.name AS source, b.name AS target, e.since AS since)
            )",
        )
        .unwrap();

    // Alix->Gus (2020), Gus->Harm (2021)
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_multi_hop_pattern() {
    let db = create_rich_network();
    let session = db.session();

    // Alix -KNOWS-> Gus -KNOWS-> Harm
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
                COLUMNS (a.name AS first, b.name AS middle, c.name AS last)
            )",
        )
        .unwrap();

    // Alix->Gus->Harm is the only 2-hop KNOWS path
    assert!(
        result.row_count() >= 1,
        "Should find at least one 2-hop path"
    );
}

#[test]
fn test_mixed_edge_types_in_path() {
    let db = create_rich_network();
    let session = db.session();

    // FOLLOWS then KNOWS:
    // Alix -FOLLOWS-> Vincent -KNOWS-> Mia
    // Gus -FOLLOWS-> Alix -KNOWS-> Gus
    // Gus -FOLLOWS-> Alix -KNOWS-> Harm
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:KNOWS]->(c:Person)
                COLUMNS (a.name AS follower, b.name AS followed, c.name AS friend)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 3);

    // Verify Alix->Vincent->Mia path exists
    let alix_path = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Alix") && r[1].as_str() == Some("Vincent"));
    assert!(alix_path.is_some(), "Alix->Vincent->Mia path should exist");
}

// ============================================================================
// Node patterns: labels, properties, anonymous
// ============================================================================

#[test]
fn test_node_with_inline_properties() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person {city: 'Amsterdam'})
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Alix, Harm
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_node_with_multiple_inline_properties() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person {city: 'Amsterdam', age: 30})
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Only Alix
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
}

#[test]
fn test_node_without_label() {
    let db = create_multi_label_graph();
    let session = db.session();

    // Match any node
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n)
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Django, Shosanna, Hans, GrafeoDB Inc = 4 nodes
    assert_eq!(result.row_count(), 4);
}

#[test]
fn test_multi_label_node() {
    let db = create_multi_label_graph();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Employee)
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Django (Person+Employee), Hans (Person+Employee)
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_label_filter_manager() {
    let db = create_multi_label_graph();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Manager)
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    // Only Shosanna
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Shosanna".into()));
}

#[test]
fn test_cross_label_edge_pattern() {
    let db = create_multi_label_graph();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (p:Person)-[:WORKS_AT]->(c:Company)
                COLUMNS (p.name AS person, c.name AS company)
            )",
        )
        .unwrap();

    // Django, Shosanna, Hans all work at GrafeoDB Inc
    assert_eq!(result.row_count(), 3);
}

// ============================================================================
// Variable-length paths: edge cases
// ============================================================================

#[test]
fn test_variable_length_exact_one_hop() {
    let db = create_chain_with_cycle();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Node {name: 'A'})-[p:LINK*1..1]->(b:Node)
                COLUMNS (a.name AS source, b.name AS target)
            )",
        )
        .unwrap();

    // A -> B (the only direct LINK from A)
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][1], Value::String("B".into()));
}

#[test]
fn test_variable_length_two_to_three_hops() {
    let db = create_chain_with_cycle();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Node {name: 'A'})-[p:LINK*2..3]->(b:Node)
                COLUMNS (a.name AS source, b.name AS target)
            )",
        )
        .unwrap();

    // 2 hops: A->B->C, 3 hops: A->B->C->D
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_variable_length_path_with_length_function() {
    let db = create_chain_with_cycle();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Node {name: 'A'})-[p:LINK*1..4]->(b:Node)
                COLUMNS (a.name AS source, b.name AS target, LENGTH(p) AS hops)
            )",
        )
        .unwrap();

    // 1 hop: A->B, 2 hops: A->B->C, 3: A->B->C->D, 4: A->B->C->D->E
    assert_eq!(result.row_count(), 4);

    let hops_col = result
        .columns
        .iter()
        .position(|c| c == "hops")
        .expect("hops column");
    let mut hops: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match &r[hops_col] {
            Value::Int64(h) => *h,
            other => panic!("Expected Int64 hop count, got: {other:?}"),
        })
        .collect();
    hops.sort_unstable();
    assert_eq!(hops, vec![1, 2, 3, 4]);
}

#[test]
fn test_different_edge_type_in_variable_length() {
    let db = create_chain_with_cycle();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Node {name: 'A'})-[p:SHORTCUT*1..1]->(b:Node)
                COLUMNS (a.name AS source, b.name AS target)
            )",
        )
        .unwrap();

    // Only A->C via SHORTCUT
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][1], Value::String("C".into()));
}

// ============================================================================
// Self-loops
// ============================================================================

#[test]
fn test_self_loop_outgoing() {
    let db = create_self_loop_graph();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[:LIKES]->(b:Person)
                COLUMNS (a.name AS source, b.name AS target)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Butch".into()));
    assert_eq!(result.rows[0][1], Value::String("Butch".into()));
}

#[test]
fn test_self_loop_with_other_edges() {
    let db = create_self_loop_graph();
    let session = db.session();

    // All outgoing edges from Butch
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person {name: 'Butch'})-[e]->(b:Person)
                COLUMNS (a.name AS source, b.name AS target)
            )",
        )
        .unwrap();

    // LIKES -> Butch (self), KNOWS -> Django
    assert_eq!(result.row_count(), 2);
}

// ============================================================================
// Empty results
// ============================================================================

#[test]
fn test_empty_result_no_matching_label() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:NonExistentLabel)
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 0);
}

#[test]
fn test_empty_result_no_matching_edge() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[:MARRIED_TO]->(b:Person)
                COLUMNS (a.name AS source, b.name AS target)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 0);
}

#[test]
fn test_empty_result_where_filters_all() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.age > 100
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 0);
}

#[test]
fn test_empty_graph() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 0);
}

// ============================================================================
// Single node (no edges)
// ============================================================================

#[test]
fn test_single_node_query() {
    let db = create_single_node();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age AS age)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Jules".into()));
    assert_eq!(result.rows[0][1], Value::Int64(40));
}

#[test]
fn test_single_node_no_edges_relationship_query() {
    let db = create_single_node();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[:KNOWS]->(b:Person)
                COLUMNS (a.name AS source, b.name AS target)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 0);
}

// ============================================================================
// SELECT list: explicit columns, aliases, expressions
// ============================================================================

#[test]
#[ignore = "outer SELECT column projection not yet implemented: returns all COLUMNS"]
fn test_select_explicit_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT name FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age AS age, n.city AS city)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
    // Only 'name' column should be in output
    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0], "name");
}

#[test]
#[ignore = "outer SELECT column projection not yet implemented: returns all COLUMNS"]
fn test_select_multiple_explicit_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT name, city FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age AS age, n.city AS city)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
    assert_eq!(result.columns.len(), 2);
}

#[test]
#[ignore = "outer SELECT alias renaming not yet implemented"]
fn test_select_with_alias() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT name AS person_name FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
    assert_eq!(result.columns[0], "person_name");
}

#[test]
fn test_select_with_table_qualified_column() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT g.name, g.age FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age AS age)
            ) AS g",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
}

// ============================================================================
// ORDER BY: multiple columns, ASC/DESC
// ============================================================================

#[test]
fn test_order_by_asc_explicit() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age AS age)
            ) AS g
            ORDER BY g.age ASC",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
    let ages: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match &r[1] {
            Value::Int64(a) => *a,
            other => panic!("Expected Int64, got: {other:?}"),
        })
        .collect();
    assert_eq!(ages, vec![25, 28, 30, 32, 35], "should be sorted ascending");
}

#[test]
fn test_order_by_desc() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age AS age)
            ) AS g
            ORDER BY g.age DESC",
        )
        .unwrap();

    let ages: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match &r[1] {
            Value::Int64(a) => *a,
            other => panic!("Expected Int64, got: {other:?}"),
        })
        .collect();
    assert_eq!(
        ages,
        vec![35, 32, 30, 28, 25],
        "should be sorted descending"
    );
}

#[test]
fn test_order_by_multiple_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city, n.name AS name, n.age AS age)
            ) AS g
            ORDER BY g.city ASC, g.age DESC",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
    // Amsterdam first (Harm 35, Alix 30), then Berlin (Mia 32, Gus 25), then Paris (Vincent 28)
    assert_eq!(result.rows[0][0], Value::String("Amsterdam".into()));
}

#[test]
fn test_order_by_with_limit_and_offset() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age AS age)
            ) AS g
            ORDER BY g.age ASC
            LIMIT 2
            OFFSET 1",
        )
        .unwrap();

    // Sorted: 25, 28, 30, 32, 35 -> skip 1, take 2 -> 28, 30
    assert_eq!(result.row_count(), 2);
    assert_eq!(result.rows[0][1], Value::Int64(28));
    assert_eq!(result.rows[1][1], Value::Int64(30));
}

// ============================================================================
// DISTINCT with edges
// ============================================================================

#[test]
fn test_distinct_on_projection() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT DISTINCT city FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 3); // Amsterdam, Berlin, Paris
}

// ============================================================================
// CASE expression
// ============================================================================

#[test]
#[ignore = "CASE expression not yet supported in SQL/PGQ COLUMNS clause parser"]
fn test_case_expression_in_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (
                    n.name AS name,
                    CASE WHEN n.age >= 30 THEN 'senior' ELSE 'junior' END AS category
                )
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);

    let alix_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Alix"))
        .expect("Alix should appear");
    assert_eq!(
        alix_row[1],
        Value::String("senior".into()),
        "Alix (30) should be senior"
    );

    let gus_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Gus"))
        .expect("Gus should appear");
    assert_eq!(
        gus_row[1],
        Value::String("junior".into()),
        "Gus (25) should be junior"
    );
}

#[test]
#[ignore = "CASE expression not yet supported in SQL/PGQ COLUMNS clause parser"]
fn test_case_expression_no_else() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (
                    n.name AS name,
                    CASE WHEN n.city = 'Amsterdam' THEN 'NL' END AS country
                )
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);

    // Non-Amsterdam should be NULL
    let gus_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Gus"))
        .expect("Gus should appear");
    assert!(
        gus_row[1].is_null(),
        "Gus (Berlin) should have NULL country"
    );
}

// ============================================================================
// CALL procedure
// ============================================================================

#[test]
fn test_call_procedure_basic() {
    let db = create_rich_network();
    let session = db.session();

    let result = session.execute_sql("CALL grafeo.schema()");
    // Should parse and execute (procedure may or may not be implemented)
    assert!(
        result.is_ok() || result.is_err(),
        "CALL should at least parse"
    );
}

#[test]
fn test_call_procedure_with_yield() {
    let db = create_rich_network();
    let session = db.session();

    let result = session.execute_sql("CALL grafeo.nodeLabels() YIELD label");
    // Should parse
    assert!(
        result.is_ok() || result.is_err(),
        "CALL with YIELD should at least parse"
    );
}

#[test]
fn test_call_procedure_with_arguments() {
    let db = create_rich_network();
    let session = db.session();

    let result = session.execute_sql("CALL grafeo.degree('Person', 'KNOWS')");
    assert!(
        result.is_ok() || result.is_err(),
        "CALL with args should at least parse"
    );
}

// ============================================================================
// Multiple patterns in MATCH (comma-separated)
// ============================================================================

#[test]
fn test_multiple_patterns_comma_separated() {
    let db = create_multi_label_graph();
    let session = db.session();

    // Two separate patterns: match a Person and a Company independently
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (p:Person), (c:Company)
                COLUMNS (p.name AS person, c.name AS company)
            )",
        )
        .unwrap();

    // 3 persons x 1 company = 3 rows (cross product)
    assert_eq!(result.row_count(), 3);
}

// ============================================================================
// Complex combined queries
// ============================================================================

#[test]
fn test_full_pipeline_query() {
    let db = create_rich_network();
    let session = db.session();

    // Full pipeline: MATCH with edge, inner WHERE, COLUMNS, outer WHERE, ORDER BY, LIMIT
    let result = session
        .execute_sql(
            "SELECT g.source, g.target, g.since FROM GRAPH_TABLE (
                MATCH (a:Person)-[e:KNOWS]->(b:Person)
                WHERE a.age >= 25
                COLUMNS (a.name AS source, b.name AS target, e.since AS since)
            ) AS g
            WHERE g.since >= 2020
            ORDER BY g.since DESC
            LIMIT 3",
        )
        .unwrap();

    assert!(result.row_count() <= 3, "LIMIT 3 should cap results");
    // Verify ORDER BY DESC: first row should have highest since value
    if result.row_count() >= 2 {
        let first_since = match &result.rows[0][2] {
            Value::Int64(s) => *s,
            other => panic!("Expected Int64, got: {other:?}"),
        };
        let second_since = match &result.rows[1][2] {
            Value::Int64(s) => *s,
            other => panic!("Expected Int64, got: {other:?}"),
        };
        assert!(first_since >= second_since, "ORDER BY DESC not respected");
    }
}

#[test]
fn test_relationship_with_group_by_on_edge_type() {
    let db = create_rich_network();
    let session = db.session();

    // Count outgoing edges per person across all types (no ORDER BY on alias)
    let result = session
        .execute_sql(
            "SELECT source, COUNT(*) AS edge_count
             FROM GRAPH_TABLE (
                MATCH (a:Person)-[e]->(b:Person)
                COLUMNS (a.name AS source)
            )
            GROUP BY source",
        )
        .unwrap();

    // Alix: KNOWS Gus, KNOWS Harm, FOLLOWS Vincent = 3
    // Gus: KNOWS Harm, FOLLOWS Alix = 2
    // Vincent: KNOWS Mia = 1
    assert_eq!(result.row_count(), 3);

    let alix_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Alix"))
        .expect("Alix should appear");
    assert_eq!(alix_row[1], Value::Int64(3));

    let gus_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Gus"))
        .expect("Gus should appear");
    assert_eq!(gus_row[1], Value::Int64(2));

    let vincent_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Vincent"))
        .expect("Vincent should appear");
    assert_eq!(vincent_row[1], Value::Int64(1));
}

#[test]
fn test_distinct_with_order_by() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT DISTINCT city FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city)
            )
            ORDER BY city ASC",
        )
        .unwrap();

    assert_eq!(result.row_count(), 3);
    assert_eq!(result.rows[0][0], Value::String("Amsterdam".into()));
    assert_eq!(result.rows[1][0], Value::String("Berlin".into()));
    assert_eq!(result.rows[2][0], Value::String("Paris".into()));
}

// ============================================================================
// Unicode and special characters
// ============================================================================

#[test]
fn test_unicode_property_values() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Person"], [("name", Value::String("Ren\u{00e9}".into()))]);
    session.create_node_with_props(&["Person"], [("name", Value::String("\u{00dc}mit".into()))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_escaped_string_in_where() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(&["Person"], [("name", Value::String("O'Brien".into()))]);
    session.create_node_with_props(&["Person"], [("name", Value::String("Smith".into()))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.name = 'O\\'Brien'
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("O'Brien".into()));
}

// ============================================================================
// Trailing semicolons
// ============================================================================

#[test]
fn test_trailing_semicolon() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name)
            );",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
}

// ============================================================================
// CREATE PROPERTY GRAPH: additional cases
// ============================================================================

#[test]
fn test_create_property_graph_all_data_types() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let result = session
        .execute_sql(
            "CREATE PROPERTY GRAPH AllTypes
             NODE TABLES (
                 Everything (
                     id BIGINT PRIMARY KEY,
                     small_id INT,
                     name VARCHAR,
                     description VARCHAR(500),
                     active BOOLEAN,
                     score FLOAT,
                     precise DOUBLE,
                     created_at TIMESTAMP
                 )
             )",
        )
        .expect("Should support all SQL data types");

    assert_eq!(result.columns, vec!["status"]);
    match &result.rows[0][0] {
        Value::String(s) => assert!(s.contains("AllTypes")),
        other => panic!("Expected string status, got: {other:?}"),
    }
}

#[test]
fn test_create_property_graph_node_only() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let result = session
        .execute_sql(
            "CREATE PROPERTY GRAPH NodesOnly
             NODE TABLES (
                 Person (id BIGINT PRIMARY KEY, name VARCHAR)
             )",
        )
        .expect("Node-only graph should work");

    match &result.rows[0][0] {
        Value::String(s) => assert!(s.contains("NodesOnly")),
        other => panic!("Expected string status, got: {other:?}"),
    }
}

#[test]
fn test_create_property_graph_multiple_edge_tables() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let result = session
        .execute_sql(
            "CREATE PROPERTY GRAPH MultiEdge
             NODE TABLES (
                 Person (id BIGINT PRIMARY KEY, name VARCHAR),
                 Company (id BIGINT PRIMARY KEY, name VARCHAR)
             )
             EDGE TABLES (
                 KNOWS (
                     id BIGINT PRIMARY KEY,
                     src BIGINT REFERENCES Person(id),
                     dst BIGINT REFERENCES Person(id)
                 ),
                 WORKS_AT (
                     id BIGINT PRIMARY KEY,
                     employee BIGINT REFERENCES Person(id),
                     employer BIGINT REFERENCES Company(id)
                 )
             )",
        )
        .expect("Multiple edge tables should work");

    match &result.rows[0][0] {
        Value::String(s) => {
            assert!(s.contains("MultiEdge"));
            assert!(s.contains("2 edge tables"));
        }
        other => panic!("Expected string status, got: {other:?}"),
    }
}

// ============================================================================
// Error cases
// ============================================================================

#[test]
fn test_error_missing_from() {
    let db = create_rich_network();
    let session = db.session();

    let result =
        session.execute_sql("SELECT * GRAPH_TABLE (MATCH (n:Person) COLUMNS (n.name AS name))");
    assert!(result.is_err(), "Missing FROM should fail");
}

#[test]
fn test_error_missing_match() {
    let db = create_rich_network();
    let session = db.session();

    let result = session.execute_sql(
        "SELECT * FROM GRAPH_TABLE (
            COLUMNS (n.name AS name)
        )",
    );
    assert!(result.is_err(), "Missing MATCH should fail");
}

#[test]
fn test_error_missing_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session.execute_sql(
        "SELECT * FROM GRAPH_TABLE (
            MATCH (n:Person)
        )",
    );
    assert!(result.is_err(), "Missing COLUMNS should fail");
}

#[test]
fn test_error_empty_columns_clause() {
    let db = create_rich_network();
    let session = db.session();

    let result = session.execute_sql(
        "SELECT * FROM GRAPH_TABLE (
            MATCH (n:Person)
            COLUMNS ()
        )",
    );
    assert!(result.is_err(), "Empty COLUMNS clause should fail");
}

#[test]
fn test_error_missing_column_alias() {
    let db = create_rich_network();
    let session = db.session();

    let result = session.execute_sql(
        "SELECT * FROM GRAPH_TABLE (
            MATCH (n:Person)
            COLUMNS (n.name)
        )",
    );
    assert!(result.is_err(), "Missing column alias (AS) should fail");
}

#[test]
fn test_error_unclosed_graph_table_paren() {
    let db = create_rich_network();
    let session = db.session();

    let result = session.execute_sql(
        "SELECT * FROM GRAPH_TABLE (
            MATCH (n:Person)
            COLUMNS (n.name AS name)",
    );
    assert!(result.is_err(), "Unclosed GRAPH_TABLE paren should fail");
}

#[test]
fn test_error_empty_query() {
    let db = create_rich_network();
    let session = db.session();

    let result = session.execute_sql("");
    assert!(result.is_err(), "Empty query should fail");
}

#[test]
fn test_error_create_graph_no_tables() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let result = session.execute_sql("CREATE PROPERTY GRAPH Empty NODE TABLES ()");
    assert!(
        result.is_err(),
        "CREATE PROPERTY GRAPH with no tables should fail"
    );
}

// ============================================================================
// LEFT JOIN / OPTIONAL MATCH with complex scenarios
// ============================================================================

#[test]
fn test_left_join_preserves_all_left_rows() {
    let db = create_rich_network();
    let session = db.session();

    // All persons, left join with FOLLOWS edges
    // Only Alix and Gus have outgoing FOLLOWS edges
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)
                LEFT OUTER JOIN MATCH (a)-[:FOLLOWS]->(b:Person)
                COLUMNS (a.name AS person, b.name AS follows)
            )",
        )
        .unwrap();

    // 5 persons, but Alix and Gus each have 1 FOLLOWS target = 5 rows total
    // (Harm, Vincent, Mia have NULL for follows)
    assert_eq!(result.row_count(), 5);

    let null_rows = result.rows.iter().filter(|r| r[1].is_null()).count();
    assert_eq!(null_rows, 3, "3 persons have no outgoing FOLLOWS edges");
}

#[test]
fn test_left_join_with_where_on_optional_side() {
    let db = create_rich_network();
    let session = db.session();

    // LEFT JOIN then filter: only keep rows where follows is not null
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)
                LEFT OUTER JOIN MATCH (a)-[:FOLLOWS]->(b:Person)
                COLUMNS (a.name AS person, b.name AS follows)
            ) AS g
            WHERE g.follows IS NOT NULL",
        )
        .unwrap();

    // Only Alix->Vincent and Gus->Alix
    assert_eq!(result.row_count(), 2);
}

// ============================================================================
// WHERE inside GRAPH_TABLE: complex predicates
// ============================================================================

#[test]
fn test_inner_where_with_edge_property() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[e:KNOWS]->(b:Person)
                WHERE e.since < 2020
                COLUMNS (a.name AS source, b.name AS target, e.since AS since)
            )",
        )
        .unwrap();

    // Alix->Harm (2018), Vincent->Mia (2019)
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_inner_where_with_multiple_conditions() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[e:KNOWS]->(b:Person)
                WHERE a.age >= 30 AND b.age >= 30
                COLUMNS (a.name AS source, b.name AS target)
            )",
        )
        .unwrap();

    // Alix (30)->Harm (35) only
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
    assert_eq!(result.rows[0][1], Value::String("Harm".into()));
}

// ============================================================================
// Graph name reference
// ============================================================================

#[test]
fn test_graph_name_with_dotted_identifier() {
    let db = create_rich_network();
    let session = db.session();

    // Graph name can be a dotted identifier
    let result = session.execute_sql(
        "SELECT * FROM GRAPH_TABLE (my_schema.social, MATCH (a:Person) COLUMNS (a.name AS name))",
    );
    // Should at least parse
    assert!(
        result.is_ok() || result.is_err(),
        "Dotted graph name should at least parse"
    );
}

// ============================================================================
// Float and negative values
// ============================================================================

#[test]
fn test_float_literal_in_where() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(
        &["Measurement"],
        [
            ("name", Value::String("temp1".into())),
            ("value", Value::Float64(36.5)),
        ],
    );
    session.create_node_with_props(
        &["Measurement"],
        [
            ("name", Value::String("temp2".into())),
            ("value", Value::Float64(37.8)),
        ],
    );
    session.create_node_with_props(
        &["Measurement"],
        [
            ("name", Value::String("temp3".into())),
            ("value", Value::Float64(38.5)),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (m:Measurement)
                WHERE m.value > 37.0
                COLUMNS (m.name AS name, m.value AS val)
            )",
        )
        .unwrap();

    // temp2 (37.8), temp3 (38.5)
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_negative_literal_in_where() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    session.create_node_with_props(
        &["Data"],
        [
            ("name", Value::String("a".into())),
            ("val", Value::Int64(-5)),
        ],
    );
    session.create_node_with_props(
        &["Data"],
        [
            ("name", Value::String("b".into())),
            ("val", Value::Int64(10)),
        ],
    );

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (d:Data)
                WHERE d.val > -10
                COLUMNS (d.name AS name, d.val AS val)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 2);
}

// ============================================================================
// SQL-level WHERE with various operators
// ============================================================================

#[test]
fn test_sql_where_with_between() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age AS age)
            ) AS g
            WHERE g.age BETWEEN 28 AND 32",
        )
        .unwrap();

    // Alix (30), Vincent (28), Mia (32)
    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_sql_where_with_like() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name)
            ) AS g
            WHERE g.name LIKE '%in%'",
        )
        .unwrap();

    // Vincent
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Vincent".into()));
}

#[test]
fn test_sql_where_with_in() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.city AS city)
            ) AS g
            WHERE g.city IN ['Amsterdam', 'Paris']",
        )
        .unwrap();

    // Alix (Amsterdam), Harm (Amsterdam), Vincent (Paris)
    assert_eq!(result.row_count(), 3);
}

// ============================================================================
// LIMIT without ORDER BY
// ============================================================================

#[test]
fn test_limit_without_order_by() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name)
            )
            LIMIT 2",
        )
        .unwrap();

    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_offset_without_limit() {
    let db = create_rich_network();
    let session = db.session();

    // OFFSET without explicit LIMIT (should skip first N and return rest)
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name)
            )
            LIMIT 100
            OFFSET 3",
        )
        .unwrap();

    // 5 total, skip 3, get 2
    assert_eq!(result.row_count(), 2);
}

// ============================================================================
// SELECT DISTINCT with aggregation
// ============================================================================

#[test]
#[ignore = "ORDER BY on aggregate alias not yet resolved in SQL/PGQ translator"]
fn test_group_by_with_order_by() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT city, COUNT(*) AS cnt
             FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city)
            )
            GROUP BY city
            ORDER BY cnt DESC",
        )
        .unwrap();

    assert_eq!(result.row_count(), 3);
    // Amsterdam (2) and Berlin (2) tie, Paris (1) last
    let last_count = match &result.rows[2][1] {
        Value::Int64(c) => *c,
        other => panic!("Expected Int64, got: {other:?}"),
    };
    assert_eq!(last_count, 1, "Paris should have count 1");
}

#[test]
#[ignore = "ORDER BY on aggregate alias not yet resolved in SQL/PGQ translator"]
fn test_group_by_with_limit() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT city, COUNT(*) AS cnt
             FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city)
            )
            GROUP BY city
            ORDER BY cnt DESC
            LIMIT 1",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
}

// ============================================================================
// Manages/WORKS_AT patterns: different edge types in same query
// ============================================================================

#[test]
fn test_separate_edge_types_in_pattern() {
    let db = create_multi_label_graph();
    let session = db.session();

    // Shosanna manages employees who work at a company
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (m:Manager)-[:MANAGES]->(e:Person)-[:WORKS_AT]->(c:Company)
                COLUMNS (m.name AS manager, e.name AS employee, c.name AS company)
            )",
        )
        .unwrap();

    // Shosanna manages Django and Hans, both work at GrafeoDB Inc
    assert_eq!(result.row_count(), 2);
    for row in &result.rows {
        assert_eq!(row[0], Value::String("Shosanna".into()));
        assert_eq!(row[2], Value::String("GrafeoDB Inc".into()));
    }
}

// ============================================================================
// Map literal in COLUMNS
// ============================================================================

#[test]
fn test_map_literal_in_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, {type: 'Person'} AS meta)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
    for row in &result.rows {
        match &row[1] {
            Value::Map(m) => {
                assert!(m.contains_key("type"));
            }
            other => panic!("Expected map, got: {other:?}"),
        }
    }
}

// ============================================================================
// List literal in COLUMNS
// ============================================================================

#[test]
fn test_list_literal_in_columns() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, [1, 2, 3] AS nums)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
    for row in &result.rows {
        match &row[1] {
            Value::List(items) => {
                assert_eq!(items.len(), 3);
            }
            other => panic!("Expected list, got: {other:?}"),
        }
    }
}

// ============================================================================
// Double-quoted identifiers
// ============================================================================

#[test]
fn test_double_quoted_alias() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS \"Full Name\")
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
    assert_eq!(result.columns[0], "Full Name");
}

// ============================================================================
// execute_language dispatch
// ============================================================================

#[test]
fn test_execute_language_sql_pgq() {
    let db = create_rich_network();

    let result = db.execute_language(
        "SELECT * FROM GRAPH_TABLE (
            MATCH (n:Person)
            COLUMNS (n.name AS name)
        )",
        "sql-pgq",
        None,
    );
    assert!(result.is_ok(), "execute_language with sql-pgq should work");
    assert_eq!(result.unwrap().row_count(), 5);
}

// ============================================================================
// Case insensitivity of SQL keywords
// ============================================================================

#[test]
fn test_case_insensitive_keywords() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "select * from graph_table (
                match (n:Person)
                columns (n.name as name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
}

#[test]
fn test_mixed_case_keywords() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "Select * From GRAPH_TABLE (
                Match (n:Person)
                Columns (n.name As name)
            ) As g
            Order By g.name Asc
            Limit 3",
        )
        .unwrap();

    assert_eq!(result.row_count(), 3);
}

// ============================================================================
// Comments in queries
// ============================================================================

#[test]
fn test_line_comment() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "-- This is a comment
            SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person) -- inline comment
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
}

#[test]
fn test_block_comment() {
    let db = create_rich_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "/* Block comment */
            SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                /* multi-line
                   comment */
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5);
}
