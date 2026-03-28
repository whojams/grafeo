//! Gremlin Integration Tests
//!
//! Verifies end-to-end query execution through the full pipeline:
//! Parse → Translate → Bind → Optimize → Plan → Execute
//!
//! Run with:
//! ```bash
//! cargo test -p grafeo-engine --features gremlin --test gremlin
//! ```

#![cfg(feature = "gremlin")]

use grafeo_common::types::Value;
use grafeo_engine::GrafeoDB;

// ============================================================================
// Test Fixtures
// ============================================================================

/// Creates a social network graph for testing.
///
/// Structure:
/// - Alix (Person, age: 30, city: "Amsterdam") -KNOWS-> Gus (Person, age: 25, city: "Berlin")
/// - Alix -KNOWS-> Vincent (Person, age: 35, city: "Paris")
/// - Gus -KNOWS-> Vincent
/// - Alix -WORKS_AT-> Acme (Company, revenue: 1000000)
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
    let acme = session.create_node_with_props(
        &["Company"],
        [
            ("name", Value::String("Acme".into())),
            ("revenue", Value::Int64(1_000_000)),
        ],
    );

    session.create_edge(alix, gus, "KNOWS");
    session.create_edge(alix, vincent, "KNOWS");
    session.create_edge(gus, vincent, "KNOWS");
    session.create_edge(alix, acme, "WORKS_AT");

    db
}

// ============================================================================
// Basic Traversals: g.V(), g.E()
// ============================================================================

#[test]
fn test_g_v_all_vertices() {
    let db = create_social_network();
    let result = db.execute_gremlin("g.V()").unwrap();
    assert_eq!(result.row_count(), 4, "Should find 4 vertices");
}

#[test]
fn test_g_e_all_edges() {
    let db = create_social_network();
    let result = db.execute_gremlin("g.E()").unwrap();
    assert_eq!(result.row_count(), 4, "Should find 4 edges");
}

// ============================================================================
// Label Filtering: hasLabel()
// ============================================================================

#[test]
fn test_has_label_person() {
    let db = create_social_network();
    let result = db.execute_gremlin("g.V().hasLabel('Person')").unwrap();
    assert_eq!(result.row_count(), 3, "Should find 3 Person vertices");
}

#[test]
fn test_has_label_company() {
    let db = create_social_network();
    let result = db.execute_gremlin("g.V().hasLabel('Company')").unwrap();
    assert_eq!(result.row_count(), 1, "Should find 1 Company vertex");
}

#[test]
fn test_has_label_edge() {
    let db = create_social_network();
    let result = db.execute_gremlin("g.E().hasLabel('KNOWS')").unwrap();
    assert_eq!(result.row_count(), 3, "Should find 3 KNOWS edges");
}

// ============================================================================
// Property Filtering: has()
// ============================================================================

#[test]
fn test_has_property_equals() {
    let db = create_social_network();
    let result = db.execute_gremlin("g.V().has('name', 'Alix')").unwrap();
    assert_eq!(
        result.row_count(),
        1,
        "Should find exactly 1 vertex named Alix"
    );
}

#[test]
fn test_has_label_and_property() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').has('age', 30)")
        .unwrap();
    assert_eq!(result.row_count(), 1, "Should find 1 Person with age 30");
}

#[test]
fn test_has_property_gt() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').has('age', gt(28))")
        .unwrap();
    assert_eq!(
        result.row_count(),
        2,
        "Alix (30) and Vincent (35) have age > 28"
    );
}

#[test]
fn test_has_property_lt() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').has('age', lt(30))")
        .unwrap();
    assert_eq!(result.row_count(), 1, "Only Gus (25) has age < 30");
}

#[test]
fn test_has_property_gte() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').has('age', gte(30))")
        .unwrap();
    assert_eq!(
        result.row_count(),
        2,
        "Alix (30) and Vincent (35) have age >= 30"
    );
}

#[test]
fn test_has_property_lte() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').has('age', lte(25))")
        .unwrap();
    assert_eq!(result.row_count(), 1, "Only Gus (25) has age <= 25");
}

#[test]
fn test_has_property_neq() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').has('age', neq(30))")
        .unwrap();
    assert_eq!(
        result.row_count(),
        2,
        "Gus (25) and Vincent (35) have age != 30"
    );
}

#[test]
fn test_has_property_between() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').has('age', between(25, 35))")
        .unwrap();
    assert_eq!(
        result.row_count(),
        2,
        "Alix (30) and Gus (25) are between 25 and 35"
    );
}

#[test]
fn test_has_property_within() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').has('age', within(25, 35))")
        .unwrap();
    assert_eq!(
        result.row_count(),
        2,
        "Gus (25) and Vincent (35) match within(25, 35)"
    );
}

#[test]
fn test_has_property_without() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').has('age', without(25, 35))")
        .unwrap();
    assert_eq!(result.row_count(), 1, "Only Alix (30) is not in {25, 35}");
}

#[test]
fn test_has_not() {
    let db = create_social_network();
    let result = db.execute_gremlin("g.V().hasNot('revenue')").unwrap();
    assert_eq!(
        result.row_count(),
        3,
        "Only 3 Person vertices lack 'revenue'"
    );
}

// ============================================================================
// String Predicates
// ============================================================================

#[test]
fn test_containing() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').has('city', containing('er'))")
        .unwrap();
    // "Berlin" and "Amsterdam" both contain "er"
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_starting_with() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').has('city', startingWith('Am'))")
        .unwrap();
    assert_eq!(result.row_count(), 1, "Only Amsterdam starts with 'Am'");
}

#[test]
fn test_ending_with() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').has('city', endingWith('is'))")
        .unwrap();
    assert_eq!(result.row_count(), 1, "Only Paris ends with 'is'");
}

// ============================================================================
// Edge Traversals: out(), in(), both()
// ============================================================================

#[test]
fn test_out_all() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Alix').out()")
        .unwrap();
    assert_eq!(
        result.row_count(),
        3,
        "Alix has 3 outgoing edges (2 KNOWS + 1 WORKS_AT)"
    );
}

#[test]
fn test_out_with_label() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Alix').out('KNOWS')")
        .unwrap();
    assert_eq!(result.row_count(), 2, "Alix KNOWS 2 people");
}

#[test]
fn test_in_traversal() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Vincent').in('KNOWS')")
        .unwrap();
    assert_eq!(result.row_count(), 2, "Vincent is known by Alix and Gus");
}

#[test]
fn test_both_traversal() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Gus').both('KNOWS')")
        .unwrap();
    // Gus: in(KNOWS) from Alix, out(KNOWS) to Vincent
    assert_eq!(result.row_count(), 2, "Gus has 2 KNOWS connections");
}

// ============================================================================
// Edge Step Traversals: outE(), inE(), bothE() + inV(), outV()
// ============================================================================

#[test]
fn test_out_e() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Alix').outE('KNOWS')")
        .unwrap();
    assert_eq!(result.row_count(), 2, "Alix has 2 outgoing KNOWS edges");
}

#[test]
fn test_in_e() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Vincent').inE('KNOWS')")
        .unwrap();
    assert_eq!(result.row_count(), 2, "Vincent has 2 incoming KNOWS edges");
}

#[test]
fn test_out_e_in_v() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Alix').outE('KNOWS').inV()")
        .unwrap();
    assert_eq!(
        result.row_count(),
        2,
        "Alix's KNOWS edges point to 2 vertices"
    );
}

#[test]
fn test_in_e_out_v() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Vincent').inE('KNOWS').outV()")
        .unwrap();
    assert_eq!(
        result.row_count(),
        2,
        "Vincent's incoming KNOWS edges come from 2 vertices"
    );
}

#[test]
fn test_both_e() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Gus').bothE('KNOWS')")
        .unwrap();
    assert_eq!(result.row_count(), 2, "Gus has 2 KNOWS edges (1 in, 1 out)");
}

// ============================================================================
// Values and Property Access
// ============================================================================

#[test]
fn test_values_single_property() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').values('name')")
        .unwrap();
    assert_eq!(result.row_count(), 3, "Should return 3 name values");

    let names: Vec<&str> = result.rows.iter().filter_map(|r| r[0].as_str()).collect();
    assert!(names.contains(&"Alix"));
    assert!(names.contains(&"Gus"));
    assert!(names.contains(&"Vincent"));
}

#[test]
fn test_values_multiple_properties() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').values('name', 'age')")
        .unwrap();
    // Each person contributes 2 values (name + age), so 3 * 2 = 6
    assert_eq!(
        result.row_count(),
        6,
        "Should return 6 values (3 names + 3 ages)"
    );
}

#[test]
fn test_value_map() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Alix').valueMap()")
        .unwrap();
    assert_eq!(result.row_count(), 1, "Should return 1 value map for Alix");
}

#[test]
fn test_element_map() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Alix').elementMap()")
        .unwrap();
    assert_eq!(
        result.row_count(),
        1,
        "Should return 1 element map for Alix"
    );
}

// ============================================================================
// Aggregations: count(), sum(), min(), max(), mean()
// ============================================================================

#[test]
fn test_count() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').count()")
        .unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(3));
}

#[test]
fn test_count_edges() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.E().hasLabel('KNOWS').count()")
        .unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(3));
}

#[test]
fn test_sum() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').values('age').sum()")
        .unwrap();
    assert_eq!(result.row_count(), 1);
    // 30 + 25 + 35 = 90
    assert_eq!(result.rows[0][0], Value::Int64(90));
}

#[test]
fn test_min() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').values('age').min()")
        .unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(25));
}

#[test]
fn test_max() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').values('age').max()")
        .unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::Int64(35));
}

#[test]
fn test_mean() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').values('age').mean()")
        .unwrap();
    assert_eq!(result.row_count(), 1);
    // mean of 25, 30, 35 = 30.0
    match &result.rows[0][0] {
        Value::Float64(f) => assert!((f - 30.0).abs() < 0.01, "Mean should be 30.0, got {f}"),
        Value::Int64(i) => assert_eq!(*i, 30, "Mean should be 30"),
        other => panic!("Expected numeric mean, got: {other:?}"),
    }
}

// ============================================================================
// Dedup, Limit, Skip, Range
// ============================================================================

#[test]
fn test_dedup() {
    let db = create_social_network();
    // All Person vertices already have unique names, but dedup shouldn't break anything
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').dedup()")
        .unwrap();
    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_limit() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').limit(2)")
        .unwrap();
    assert_eq!(result.row_count(), 2, "Limit should restrict to 2 results");
}

#[test]
fn test_skip() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').skip(1)")
        .unwrap();
    assert_eq!(result.row_count(), 2, "Skip 1 of 3 should yield 2");
}

#[test]
fn test_range() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').range(1, 3)")
        .unwrap();
    assert_eq!(result.row_count(), 2, "range(1, 3) should yield 2 results");
}

// ============================================================================
// Ordering
// ============================================================================

#[test]
fn test_order_by_property_asc() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').order().by('age', asc).values('name')")
        .unwrap();
    assert_eq!(result.row_count(), 3);
    let names: Vec<&str> = result.rows.iter().filter_map(|r| r[0].as_str()).collect();
    assert_eq!(names, vec!["Gus", "Alix", "Vincent"]);
}

#[test]
fn test_order_by_property_desc() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').order().by('age', desc).values('name')")
        .unwrap();
    assert_eq!(result.row_count(), 3);
    let names: Vec<&str> = result.rows.iter().filter_map(|r| r[0].as_str()).collect();
    assert_eq!(names, vec!["Vincent", "Alix", "Gus"]);
}

// ============================================================================
// Mutations: addV(), addE(), property(), drop()
// ============================================================================

#[test]
fn test_add_vertex() {
    let db = GrafeoDB::new_in_memory();
    db.execute_gremlin("g.addV('Person').property('name', 'Jules').property('age', 28)")
        .unwrap();

    let result = db
        .execute_gremlin("g.V().hasLabel('Person').count()")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int64(1));
}

#[test]
fn test_add_multiple_vertices() {
    let db = GrafeoDB::new_in_memory();
    db.execute_gremlin("g.addV('Person').property('name', 'Alix')")
        .unwrap();
    db.execute_gremlin("g.addV('Person').property('name', 'Gus')")
        .unwrap();

    let result = db
        .execute_gremlin("g.V().hasLabel('Person').count()")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int64(2));
}

#[test]
fn test_add_edge() {
    let db = create_social_network();
    // Add an edge from Gus to Alix
    db.execute_gremlin(
        "g.V().has('name', 'Gus').as('a').V().has('name', 'Alix').addE('FOLLOWS').from('a')",
    )
    .unwrap();

    let result = db
        .execute_gremlin("g.E().hasLabel('FOLLOWS').count()")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int64(1));
}

#[test]
fn test_add_edge_with_property() {
    let db = create_social_network();
    db.execute_gremlin(
        "g.V().has('name', 'Gus').as('a').V().has('name', 'Alix').addE('FOLLOWS').from('a').property('since', 2024)",
    )
    .unwrap();

    let result = db
        .execute_gremlin("g.E().hasLabel('FOLLOWS').count()")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int64(1));
}

#[test]
fn test_source_add_edge() {
    let db = create_social_network();
    db.execute_gremlin(
        "g.addE('FOLLOWS').from(V().has('name', 'Gus')).to(V().has('name', 'Alix'))",
    )
    .unwrap();

    let result = db
        .execute_gremlin("g.E().hasLabel('FOLLOWS').count()")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int64(1));
}

#[test]
fn test_drop_vertex() {
    let db = create_social_network();

    // Drop Acme (Company)
    db.execute_gremlin("g.V().hasLabel('Company').drop()")
        .unwrap();

    let result = db.execute_gremlin("g.V().count()").unwrap();
    assert_eq!(
        result.rows[0][0],
        Value::Int64(3),
        "Should have 3 vertices after drop"
    );
}

#[test]
fn test_drop_edge() {
    let db = create_social_network();

    db.execute_gremlin("g.E().hasLabel('WORKS_AT').drop()")
        .unwrap();

    let result = db.execute_gremlin("g.E().count()").unwrap();
    assert_eq!(
        result.rows[0][0],
        Value::Int64(3),
        "Should have 3 edges after dropping WORKS_AT"
    );
}

// ============================================================================
// As / Select Pattern
// ============================================================================

#[test]
fn test_as_select_single() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Alix').as('a').out('KNOWS').as('b').select('b')")
        .unwrap();
    assert_eq!(result.row_count(), 2, "Alix knows 2 people");
}

#[test]
fn test_as_select_multiple() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Alix').as('a').out('KNOWS').as('b').select('a', 'b')")
        .unwrap();
    assert_eq!(result.row_count(), 2, "Should return 2 pairs");
    assert!(result.columns.len() >= 2, "Should have at least 2 columns");
}

// ============================================================================
// Project Step
// ============================================================================

#[test]
fn test_project() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').project('name', 'age').by('name').by('age')")
        .unwrap();
    assert_eq!(result.row_count(), 3);
    assert!(
        result.columns.len() >= 2,
        "Should have name and age columns"
    );
}

// ============================================================================
// Group and GroupCount
// ============================================================================

#[test]
fn test_group_count() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').groupCount().by('city')")
        .unwrap();
    // Each person has a unique city, so expect 3 groups each with count 1
    assert!(result.row_count() >= 1, "Should have group count results");
}

#[test]
fn test_group_by_label() {
    let db = create_social_network();
    let result = db.execute_gremlin("g.V().group().by(label)").unwrap();
    assert!(result.row_count() >= 1, "Should have grouped results");
}

// ============================================================================
// Fold / Unfold
// ============================================================================

#[test]
fn test_fold() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').values('name').fold()")
        .unwrap();
    assert_eq!(
        result.row_count(),
        1,
        "Fold should collapse into a single list"
    );
}

#[test]
fn test_fold_unfold() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').values('name').fold().unfold()")
        .unwrap();
    assert_eq!(
        result.row_count(),
        3,
        "Unfold should expand the list back to 3 items"
    );
}

// ============================================================================
// Path
// ============================================================================

#[test]
fn test_path() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Alix').out('KNOWS').path()")
        .unwrap();
    assert_eq!(
        result.row_count(),
        2,
        "Should have 2 paths from Alix via KNOWS"
    );
}

// ============================================================================
// Coalesce
// ============================================================================

#[test]
fn test_coalesce() {
    let db = create_social_network();
    // Coalesce: try to get 'nickname' property, fall back to 'name'
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').coalesce(values('nickname'), values('name'))")
        .unwrap();
    assert_eq!(
        result.row_count(),
        3,
        "Should return a value for each Person"
    );
}

// ============================================================================
// Union
// ============================================================================

#[test]
fn test_union() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Alix').union(out('KNOWS'), out('WORKS_AT'))")
        .unwrap();
    assert_eq!(
        result.row_count(),
        3,
        "Union of KNOWS (2) and WORKS_AT (1) = 3"
    );
}

// ============================================================================
// Choose (if/then/else)
// ============================================================================

#[test]
fn test_choose_predicate() {
    let db = create_social_network();
    let result = db
        .execute_gremlin(
            "g.V().hasLabel('Person').choose(has('age', gt(28)), values('name'), constant('young'))",
        )
        .unwrap();
    assert_eq!(
        result.row_count(),
        3,
        "Should return a result for each Person"
    );
}

// ============================================================================
// Optional
// ============================================================================

#[test]
fn test_optional() {
    let db = create_social_network();
    // Vincent has no outgoing WORKS_AT, optional should keep him in the result
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').optional(out('WORKS_AT'))")
        .unwrap();
    assert!(
        result.row_count() >= 3,
        "Optional should preserve all traversers"
    );
}

// ============================================================================
// Constant
// ============================================================================

#[test]
fn test_constant() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').constant('hello')")
        .unwrap();
    assert_eq!(result.row_count(), 3);
    for row in &result.rows {
        assert_eq!(row[0], Value::String("hello".into()));
    }
}

// ============================================================================
// Label and Id Steps
// ============================================================================

#[test]
fn test_label_step() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Alix').label()")
        .unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Person".into()));
}

#[test]
fn test_id_step() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Alix').id()")
        .unwrap();
    assert_eq!(result.row_count(), 1, "Should return Alix's vertex ID");
}

// ============================================================================
// Chained Traversals (Multi-hop)
// ============================================================================

#[test]
fn test_two_hop_traversal() {
    let db = create_social_network();
    // Alix -> Gus -> Vincent, Alix -> Vincent -> (nobody)
    let result = db
        .execute_gremlin("g.V().has('name', 'Alix').out('KNOWS').out('KNOWS')")
        .unwrap();
    // Alix knows Gus and Vincent. Gus knows Vincent. Vincent knows nobody.
    // So: Alix -> Gus -> Vincent (1 path)
    assert_eq!(
        result.row_count(),
        1,
        "Should find 1 two-hop path from Alix"
    );
}

#[test]
fn test_out_then_values() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().has('name', 'Alix').out('KNOWS').values('name')")
        .unwrap();
    assert_eq!(result.row_count(), 2);
    let names: Vec<&str> = result.rows.iter().filter_map(|r| r[0].as_str()).collect();
    assert!(names.contains(&"Gus"));
    assert!(names.contains(&"Vincent"));
}

// ============================================================================
// Filter Combinations
// ============================================================================

#[test]
fn test_has_label_has_property_chain() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').has('city', 'Amsterdam').has('age', gt(20))")
        .unwrap();
    assert_eq!(result.row_count(), 1, "Only Alix matches all filters");
}

#[test]
fn test_filter_then_count() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('Person').has('age', gt(28)).count()")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int64(2));
}

// ============================================================================
// Empty Results
// ============================================================================

#[test]
fn test_no_matching_label() {
    let db = create_social_network();
    let result = db.execute_gremlin("g.V().hasLabel('NonExistent')").unwrap();
    assert_eq!(result.row_count(), 0);
}

#[test]
fn test_no_matching_property() {
    let db = create_social_network();
    let result = db.execute_gremlin("g.V().has('name', 'Django')").unwrap();
    assert_eq!(result.row_count(), 0);
}

#[test]
fn test_count_zero() {
    let db = create_social_network();
    let result = db
        .execute_gremlin("g.V().hasLabel('NonExistent').count()")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int64(0));
}

// ============================================================================
// Syntax Errors
// ============================================================================

#[test]
fn test_syntax_error_bad_query() {
    let db = create_social_network();
    let result = db.execute_gremlin("g.V(");
    assert!(result.is_err(), "Should fail on incomplete query");
}

#[test]
fn test_syntax_error_unknown_step() {
    let db = create_social_network();
    let result = db.execute_gremlin("g.V().foobar()");
    assert!(result.is_err(), "Should fail on unknown step");
}

// ============================================================================
// Database-level execute_gremlin (not session-level)
// ============================================================================

#[test]
fn test_database_level_execute() {
    let db = create_social_network();
    let result = db.execute_gremlin("g.V().count()").unwrap();
    assert_eq!(result.rows[0][0], Value::Int64(4));
}

// ============================================================================
// Parameterized Queries
// ============================================================================

#[test]
fn test_parameterized_query() {
    let db = create_social_network();
    let mut params = std::collections::HashMap::new();
    params.insert("name".to_string(), Value::String("Alix".into()));

    let result = db
        .execute_gremlin_with_params("g.V().has('name', $name)", params)
        .unwrap();
    assert_eq!(result.row_count(), 1);
}
