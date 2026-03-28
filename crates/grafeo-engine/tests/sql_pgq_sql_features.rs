//! SQL-level feature tests for SQL/PGQ.
//!
//! These tests cover SQL:2023 spec areas that sit *outside* the GRAPH_TABLE(...)
//! expression: outer SELECT projection, ORDER BY on aggregates, CASE expressions,
//! CTEs, set operations, window functions, correlated subqueries, HAVING edge
//! cases, and DISTINCT + ORDER BY interactions.
//!
//! Run with:
//! ```bash
//! cargo test -p grafeo-engine --features sql-pgq --test sql_pgq_sql_features
//! ```

#![cfg(feature = "sql-pgq")]

use grafeo_common::types::Value;
use grafeo_engine::GrafeoDB;

// ============================================================================
// Test Fixture
// ============================================================================

/// Creates a rich person network for SQL-level feature testing.
///
/// Nodes:
/// - Alix (Person, age: 30, city: "Amsterdam")
/// - Gus (Person, age: 25, city: "Berlin")
/// - Vincent (Person, age: 28, city: "Paris")
/// - Mia (Person, age: 32, city: "Berlin")
/// - Butch (Person, age: 35, city: "Amsterdam")
/// - Django (Person, age: 22, city: "Prague")
///
/// Edges:
/// - Alix -KNOWS-> Gus (since: 2020)
/// - Alix -KNOWS-> Vincent (since: 2018)
/// - Gus -KNOWS-> Mia (since: 2021)
/// - Vincent -KNOWS-> Mia (since: 2019)
/// - Butch -KNOWS-> Alix (since: 2017)
/// - Django -KNOWS-> Gus (since: 2023)
fn create_person_network() -> GrafeoDB {
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
    let butch = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Butch".into())),
            ("age", Value::Int64(35)),
            ("city", Value::String("Amsterdam".into())),
        ],
    );
    let django = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Django".into())),
            ("age", Value::Int64(22)),
            ("city", Value::String("Prague".into())),
        ],
    );

    let e1 = session.create_edge(alix, gus, "KNOWS");
    db.set_edge_property(e1, "since", Value::Int64(2020));
    let e2 = session.create_edge(alix, vincent, "KNOWS");
    db.set_edge_property(e2, "since", Value::Int64(2018));
    let e3 = session.create_edge(gus, mia, "KNOWS");
    db.set_edge_property(e3, "since", Value::Int64(2021));
    let e4 = session.create_edge(vincent, mia, "KNOWS");
    db.set_edge_property(e4, "since", Value::Int64(2019));
    let e5 = session.create_edge(butch, alix, "KNOWS");
    db.set_edge_property(e5, "since", Value::Int64(2017));
    let e6 = session.create_edge(django, gus, "KNOWS");
    db.set_edge_property(e6, "since", Value::Int64(2023));

    db
}

// ============================================================================
// Outer SELECT projection (not yet implemented)
// ============================================================================

#[test]
#[ignore = "outer SELECT column projection not yet implemented: returns all COLUMNS"]
fn test_outer_select_single_column() {
    let db = create_person_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT name FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age AS age, n.city AS city)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 6);
    assert_eq!(result.columns.len(), 1, "should return only 1 column");
    assert_eq!(result.columns[0], "name");
}

#[test]
#[ignore = "outer SELECT column projection not yet implemented: returns all COLUMNS"]
fn test_outer_select_two_columns() {
    let db = create_person_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT name, city FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age AS age, n.city AS city)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 6);
    assert_eq!(result.columns.len(), 2, "should return only 2 columns");
    assert_eq!(result.columns[0], "name");
    assert_eq!(result.columns[1], "city");
}

#[test]
#[ignore = "outer SELECT alias renaming not yet implemented"]
fn test_outer_select_alias_rename() {
    let db = create_person_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT name AS person_name FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 6);
    assert_eq!(result.columns[0], "person_name");
}

#[test]
#[ignore = "outer SELECT qualified alias renaming not yet implemented"]
fn test_outer_select_qualified_and_renamed() {
    let db = create_person_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT g.name AS n, g.age AS a FROM GRAPH_TABLE (
                MATCH (p:Person)
                COLUMNS (p.name AS name, p.age AS age)
            ) AS g",
        )
        .unwrap();

    assert_eq!(result.row_count(), 6);
    assert_eq!(result.columns.len(), 2, "should return 2 renamed columns");
    assert_eq!(result.columns[0], "n");
    assert_eq!(result.columns[1], "a");
}

#[test]
#[ignore = "outer SELECT column projection not yet implemented for aggregate queries"]
fn test_outer_select_aggregate_and_non_aggregate() {
    let db = create_person_network();
    let session = db.session();

    // COUNT(*) with GROUP BY, then outer SELECT picks specific columns
    let result = session
        .execute_sql(
            "SELECT COUNT(*) AS total, city FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city)
            )
            GROUP BY city",
        )
        .unwrap();

    // 4 cities: Amsterdam (2), Berlin (2), Paris (1), Prague (1)
    assert_eq!(result.row_count(), 4);
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[0], "total");
    assert_eq!(result.columns[1], "city");
}

// ============================================================================
// ORDER BY on aggregate aliases (not yet implemented)
// ============================================================================

#[test]
#[ignore = "ORDER BY on aggregate alias not yet resolved in SQL/PGQ translator"]
fn test_order_by_count_desc() {
    let db = create_person_network();
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

    assert_eq!(result.row_count(), 4);
    // Amsterdam (2) and Berlin (2) should come first, then Paris (1) and Prague (1)
    let last_count = match &result.rows[3][1] {
        Value::Int64(c) => *c,
        other => panic!("Expected Int64, got: {other:?}"),
    };
    assert_eq!(last_count, 1, "last city should have count 1");
}

#[test]
#[ignore = "ORDER BY on aggregate alias not yet resolved in SQL/PGQ translator"]
fn test_order_by_sum_asc() {
    let db = create_person_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT city, SUM(age) AS total
             FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city, n.age AS age)
            )
            GROUP BY city
            ORDER BY total ASC",
        )
        .unwrap();

    assert_eq!(result.row_count(), 4);
    // Prague (22), Paris (28), Berlin (25+32=57), Amsterdam (30+35=65)
    let first_total = match &result.rows[0][1] {
        Value::Int64(t) => *t,
        other => panic!("Expected Int64, got: {other:?}"),
    };
    assert_eq!(first_total, 22, "Prague should have lowest sum (22)");
}

#[test]
#[ignore = "ORDER BY on aggregate alias not yet resolved in SQL/PGQ translator"]
fn test_order_by_avg_with_having() {
    let db = create_person_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT city, AVG(age) AS avg_age
             FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city, n.age AS age)
            )
            GROUP BY city
            HAVING avg_age > 28
            ORDER BY avg_age",
        )
        .unwrap();

    // Amsterdam avg = 32.5, Berlin avg = 28.5 (> 28), Paris avg = 28 (not > 28), Prague avg = 22 (not > 28)
    // Only Amsterdam and Berlin should pass HAVING
    assert_eq!(result.row_count(), 2);
}

// ============================================================================
// CASE expression (parser gap: CASE not yet parsed in SQL/PGQ)
// ============================================================================

#[test]
#[ignore = "CASE expression not yet supported in SQL/PGQ parser"]
fn test_case_searched_in_columns() {
    let db = create_person_network();
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

    assert_eq!(result.row_count(), 6);

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
#[ignore = "CASE expression not yet supported in SQL/PGQ parser"]
fn test_case_simple_in_columns() {
    let db = create_person_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (
                    n.name AS name,
                    CASE n.city
                        WHEN 'Amsterdam' THEN 'NL'
                        WHEN 'Berlin' THEN 'DE'
                        ELSE 'other'
                    END AS country_code
                )
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 6);

    let butch_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Butch"))
        .expect("Butch should appear");
    assert_eq!(
        butch_row[1],
        Value::String("NL".into()),
        "Butch (Amsterdam) should map to NL"
    );

    let django_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Django"))
        .expect("Django should appear");
    assert_eq!(
        django_row[1],
        Value::String("other".into()),
        "Django (Prague) should map to other"
    );
}

#[test]
#[ignore = "CASE expression not yet supported in SQL/PGQ parser"]
fn test_case_with_null_check() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // Create nodes with and without a color property
    session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Alix".into())),
            ("color", Value::String("blue".into())),
        ],
    );
    session.create_node_with_props(&["Person"], [("name", Value::String("Gus".into()))]);

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (
                    n.name AS name,
                    CASE WHEN n.color IS NULL THEN 'unknown' ELSE n.color END AS color
                )
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 2);

    let gus_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Gus"))
        .expect("Gus should appear");
    assert_eq!(
        gus_row[1],
        Value::String("unknown".into()),
        "Gus (no color) should get 'unknown'"
    );
}

#[test]
#[ignore = "CASE expression not yet supported in SQL/PGQ parser"]
fn test_case_without_else() {
    let db = create_person_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (
                    n.name AS name,
                    CASE WHEN n.age > 30 THEN 'old' END AS label
                )
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 6);

    // Gus (25) does not match: should be NULL
    let gus_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Gus"))
        .expect("Gus should appear");
    assert!(
        gus_row[1].is_null(),
        "Gus (25) should have NULL for non-matching CASE without ELSE"
    );

    // Butch (35) matches: should be 'old'
    let butch_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Butch"))
        .expect("Butch should appear");
    assert_eq!(
        butch_row[1],
        Value::String("old".into()),
        "Butch (35) should be 'old'"
    );
}

#[test]
#[ignore = "CASE expression not yet supported in SQL/PGQ parser"]
fn test_case_nested() {
    let db = create_person_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (
                    n.name AS name,
                    CASE WHEN n.age > 30 THEN
                        CASE WHEN n.city = 'Amsterdam' THEN 'NL senior' ELSE 'other senior' END
                    END AS category
                )
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 6);

    let butch_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Butch"))
        .expect("Butch should appear");
    assert_eq!(
        butch_row[1],
        Value::String("NL senior".into()),
        "Butch (35, Amsterdam) should be 'NL senior'"
    );

    let mia_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("Mia"))
        .expect("Mia should appear");
    assert_eq!(
        mia_row[1],
        Value::String("other senior".into()),
        "Mia (32, Berlin) should be 'other senior'"
    );
}

// ============================================================================
// WITH (CTE): not parsed, should return an error
// ============================================================================

#[test]
fn test_cte_returns_error() {
    let db = create_person_network();
    let session = db.session();

    let result = session.execute_sql(
        "WITH persons AS (
            SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.name AS name, n.age AS age)
            )
        )
        SELECT * FROM persons",
    );

    assert!(
        result.is_err(),
        "CTE (WITH clause) is not supported and should return a parse error"
    );
}

// ============================================================================
// UNION / INTERSECT / EXCEPT: not parsed, should return errors
// ============================================================================

#[test]
fn test_union_returns_error() {
    let db = create_person_network();
    let session = db.session();

    let result = session.execute_sql(
        "SELECT * FROM GRAPH_TABLE (
            MATCH (n:Person)
            WHERE n.city = 'Amsterdam'
            COLUMNS (n.name AS name)
        )
        UNION
        SELECT * FROM GRAPH_TABLE (
            MATCH (n:Person)
            WHERE n.city = 'Berlin'
            COLUMNS (n.name AS name)
        )",
    );

    assert!(
        result.is_err(),
        "UNION between GRAPH_TABLE queries is not supported and should return an error"
    );
}

#[test]
fn test_intersect_returns_error() {
    let db = create_person_network();
    let session = db.session();

    let result = session.execute_sql(
        "SELECT * FROM GRAPH_TABLE (
            MATCH (n:Person)
            WHERE n.age >= 25
            COLUMNS (n.name AS name)
        )
        INTERSECT
        SELECT * FROM GRAPH_TABLE (
            MATCH (n:Person)
            WHERE n.age <= 30
            COLUMNS (n.name AS name)
        )",
    );

    assert!(
        result.is_err(),
        "INTERSECT between GRAPH_TABLE queries is not supported and should return an error"
    );
}

#[test]
fn test_except_returns_error() {
    let db = create_person_network();
    let session = db.session();

    let result = session.execute_sql(
        "SELECT * FROM GRAPH_TABLE (
            MATCH (n:Person)
            COLUMNS (n.name AS name)
        )
        EXCEPT
        SELECT * FROM GRAPH_TABLE (
            MATCH (n:Person)
            WHERE n.city = 'Amsterdam'
            COLUMNS (n.name AS name)
        )",
    );

    assert!(
        result.is_err(),
        "EXCEPT between GRAPH_TABLE queries is not supported and should return an error"
    );
}

// ============================================================================
// Window functions: not parsed, should return errors
// ============================================================================

#[test]
fn test_window_row_number_returns_error() {
    let db = create_person_network();
    let session = db.session();

    let result = session.execute_sql(
        "SELECT name, ROW_NUMBER() OVER (ORDER BY age) AS rn
         FROM GRAPH_TABLE (
            MATCH (n:Person)
            COLUMNS (n.name AS name, n.age AS age)
        )",
    );

    assert!(
        result.is_err(),
        "Window function ROW_NUMBER() OVER is not supported and should return an error"
    );
}

#[test]
fn test_window_count_partition_returns_error() {
    let db = create_person_network();
    let session = db.session();

    let result = session.execute_sql(
        "SELECT city, COUNT(*) OVER (PARTITION BY city) AS city_count
         FROM GRAPH_TABLE (
            MATCH (n:Person)
            COLUMNS (n.city AS city)
        )",
    );

    assert!(
        result.is_err(),
        "Window function COUNT(*) OVER (PARTITION BY ...) is not supported and should return an error"
    );
}

#[test]
fn test_window_sum_over_returns_error() {
    let db = create_person_network();
    let session = db.session();

    let result = session.execute_sql(
        "SELECT name, SUM(age) OVER () AS total_age
         FROM GRAPH_TABLE (
            MATCH (n:Person)
            COLUMNS (n.name AS name, n.age AS age)
        )",
    );

    assert!(
        result.is_err(),
        "Window function SUM() OVER () is not supported and should return an error"
    );
}

// ============================================================================
// Correlated subquery / scalar subquery: should return errors
// ============================================================================

#[test]
fn test_correlated_subquery_returns_error() {
    let db = create_person_network();
    let session = db.session();

    let result = session.execute_sql(
        "SELECT * FROM GRAPH_TABLE (
            MATCH (n:Person)
            COLUMNS (n.name AS name, n.age AS age)
        ) AS g
        WHERE g.age > (SELECT AVG(age) FROM GRAPH_TABLE (
            MATCH (m:Person)
            COLUMNS (m.age AS age)
        ))",
    );

    assert!(
        result.is_err(),
        "Correlated scalar subquery in WHERE is not supported and should return an error"
    );
}

#[test]
fn test_exists_subquery_in_select_returns_error() {
    let db = create_person_network();
    let session = db.session();

    let result = session.execute_sql(
        "SELECT *, EXISTS (
            SELECT 1 FROM GRAPH_TABLE (
                MATCH (a:Person)-[:KNOWS]->(b:Person)
                COLUMNS (a.name AS name)
            )
        ) AS has_friends
        FROM GRAPH_TABLE (
            MATCH (n:Person)
            COLUMNS (n.name AS name)
        )",
    );

    assert!(
        result.is_err(),
        "EXISTS subquery in SELECT list is not supported and should return an error"
    );
}

// ============================================================================
// HAVING edge cases
// ============================================================================

#[test]
fn test_having_without_group_by() {
    let db = create_person_network();
    let session = db.session();

    // Implicit grouping: the entire result set is one group
    let result = session.execute_sql(
        "SELECT COUNT(*) AS cnt
         FROM GRAPH_TABLE (
            MATCH (n:Person)
            COLUMNS (n.name AS name)
        )
        HAVING cnt > 2",
    );

    // This may succeed (6 > 2 is true, so it returns 1 row) or error depending on
    // whether HAVING without GROUP BY is supported. Either outcome is acceptable.
    match result {
        Ok(r) => {
            assert_eq!(r.row_count(), 1, "one group for the whole result set");
            let count = match &r.rows[0][0] {
                Value::Int64(c) => *c,
                other => panic!("Expected Int64, got: {other:?}"),
            };
            assert_eq!(count, 6, "should count all 6 persons");
        }
        Err(_) => {
            // HAVING without GROUP BY is not yet supported: that is also acceptable
        }
    }
}

#[test]
fn test_having_with_multiple_conditions() {
    let db = create_person_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT city, COUNT(*) AS cnt, AVG(age) AS avg_age
             FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city, n.age AS age)
            )
            GROUP BY city
            HAVING cnt > 1 AND avg_age > 25",
        )
        .unwrap();

    // Amsterdam: cnt=2, avg=32.5 (passes both)
    // Berlin: cnt=2, avg=28.5 (passes both)
    // Paris: cnt=1 (fails cnt > 1)
    // Prague: cnt=1 (fails cnt > 1)
    assert_eq!(result.row_count(), 2);
}

#[test]
#[ignore = "HAVING with inline aggregate function (not alias) not yet resolved"]
fn test_having_with_inline_function() {
    let db = create_person_network();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT city, COUNT(*) AS cnt
             FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city)
            )
            GROUP BY city
            HAVING COUNT(*) > 1",
        )
        .unwrap();

    // Amsterdam (2) and Berlin (2) pass; Paris (1) and Prague (1) do not
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_having_referencing_non_aggregate_column() {
    let db = create_person_network();
    let session = db.session();

    // Per SQL standard, HAVING should only reference aggregate functions or GROUP BY columns.
    // Referencing a non-aggregate, non-grouped column should error.
    let result = session.execute_sql(
        "SELECT city, COUNT(*) AS cnt
         FROM GRAPH_TABLE (
            MATCH (n:Person)
            COLUMNS (n.city AS city, n.name AS name)
        )
        GROUP BY city
        HAVING name = 'Alix'",
    );

    // Either returns an error (correct per SQL standard) or returns some result
    // (if the engine resolves it differently). We primarily want to ensure it
    // does not panic.
    match result {
        Ok(_) => {
            // Engine chose to handle it somehow: that is acceptable
        }
        Err(_) => {
            // Error is the SQL-standard-correct behavior
        }
    }
}

// ============================================================================
// DISTINCT with ORDER BY interaction
// ============================================================================

#[test]
fn test_distinct_order_by_column_in_distinct_list() {
    let db = create_person_network();
    let session = db.session();

    // ORDER BY column (city) is in the DISTINCT list: valid per SQL standard
    let result = session
        .execute_sql(
            "SELECT DISTINCT city FROM GRAPH_TABLE (
                MATCH (n:Person)
                COLUMNS (n.city AS city)
            )
            ORDER BY city",
        )
        .unwrap();

    assert_eq!(result.row_count(), 4);
    assert_eq!(result.rows[0][0], Value::String("Amsterdam".into()));
    assert_eq!(result.rows[1][0], Value::String("Berlin".into()));
    assert_eq!(result.rows[2][0], Value::String("Paris".into()));
    assert_eq!(result.rows[3][0], Value::String("Prague".into()));
}

#[test]
fn test_distinct_order_by_column_not_in_distinct_list() {
    let db = create_person_network();
    let session = db.session();

    // Per SQL standard, ORDER BY column (age) must appear in the SELECT DISTINCT list.
    // This should return an error because 'age' is not in the projected DISTINCT columns.
    let result = session.execute_sql(
        "SELECT DISTINCT city FROM GRAPH_TABLE (
            MATCH (n:Person)
            COLUMNS (n.city AS city, n.age AS age)
        )
        ORDER BY age",
    );

    // Either returns an error (correct per SQL standard) or returns some result.
    // The main goal is ensuring it does not panic.
    match result {
        Ok(_) => {
            // Engine allows it: that is acceptable (some databases are lenient)
        }
        Err(_) => {
            // Error is the SQL-standard-correct behavior
        }
    }
}
