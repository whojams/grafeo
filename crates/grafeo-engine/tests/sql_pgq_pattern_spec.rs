//! SQL/PGQ graph pattern specification tests.
//!
//! Covers pattern-matching features that are untested or lightly tested:
//! variable-length path bounds, path modes, quantified patterns,
//! graph name routing, complex multi-hop patterns, node/edge edge cases,
//! and compound WHERE predicates on nodes and edges.
//!
//! Run with:
//! ```bash
//! cargo test -p grafeo-engine --features sql-pgq --test sql_pgq_pattern_spec
//! ```

#![cfg(feature = "sql-pgq")]

use grafeo_common::types::Value;
use grafeo_engine::GrafeoDB;

// ============================================================================
// Test Fixtures
// ============================================================================

/// Creates a chain graph for path-length tests.
///
/// Nodes (label: Step):
/// - A (pos: 0) -> B (pos: 1) -> C (pos: 2) -> D (pos: 3) -> E (pos: 4)
///
/// All edges are type NEXT with a `weight` property (1..4).
fn create_chain() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let na = session.create_node_with_props(
        &["Step"],
        [
            ("name", Value::String("A".into())),
            ("pos", Value::Int64(0)),
        ],
    );
    let nb = session.create_node_with_props(
        &["Step"],
        [
            ("name", Value::String("B".into())),
            ("pos", Value::Int64(1)),
        ],
    );
    let nc = session.create_node_with_props(
        &["Step"],
        [
            ("name", Value::String("C".into())),
            ("pos", Value::Int64(2)),
        ],
    );
    let nd = session.create_node_with_props(
        &["Step"],
        [
            ("name", Value::String("D".into())),
            ("pos", Value::Int64(3)),
        ],
    );
    let ne = session.create_node_with_props(
        &["Step"],
        [
            ("name", Value::String("E".into())),
            ("pos", Value::Int64(4)),
        ],
    );

    let e1 = session.create_edge(na, nb, "NEXT");
    db.set_edge_property(e1, "weight", Value::Int64(1));
    let e2 = session.create_edge(nb, nc, "NEXT");
    db.set_edge_property(e2, "weight", Value::Int64(2));
    let e3 = session.create_edge(nc, nd, "NEXT");
    db.set_edge_property(e3, "weight", Value::Int64(3));
    let e4 = session.create_edge(nd, ne, "NEXT");
    db.set_edge_property(e4, "weight", Value::Int64(4));

    db
}

/// Creates a graph with cycles for path-mode tests.
///
/// Triangle: Alix -> Gus -> Vincent -> Alix (all KNOWS)
/// Extra:    Alix -> Jules (KNOWS), Jules has no outgoing edges.
///
/// Cycle:  Alix -> Gus -> Vincent -> Alix (length 3)
///
/// This graph lets us test TRAIL, SIMPLE, ACYCLIC modes.
fn create_cycle_graph() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let alix =
        session.create_node_with_props(&["Person"], [("name", Value::String("Alix".into()))]);
    let gus = session.create_node_with_props(&["Person"], [("name", Value::String("Gus".into()))]);
    let vincent =
        session.create_node_with_props(&["Person"], [("name", Value::String("Vincent".into()))]);
    let jules =
        session.create_node_with_props(&["Person"], [("name", Value::String("Jules".into()))]);

    session.create_edge(alix, gus, "KNOWS");
    session.create_edge(gus, vincent, "KNOWS");
    session.create_edge(vincent, alix, "KNOWS"); // closes the triangle
    session.create_edge(alix, jules, "KNOWS");

    db
}

/// Creates a star graph: one center connected to 5 outer nodes.
///
/// Center: Mia (Person, role: "hub")
/// Outer:  Django, Shosanna, Hans, Beatrix, Butch (Person)
/// Edges:  Mia -LINK-> each outer node (outgoing)
///         Django -LINK-> Mia (incoming to center)
fn create_star() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let mia = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Mia".into())),
            ("role", Value::String("hub".into())),
        ],
    );

    let django =
        session.create_node_with_props(&["Person"], [("name", Value::String("Django".into()))]);
    let shosanna =
        session.create_node_with_props(&["Person"], [("name", Value::String("Shosanna".into()))]);
    let hans =
        session.create_node_with_props(&["Person"], [("name", Value::String("Hans".into()))]);
    let beatrix =
        session.create_node_with_props(&["Person"], [("name", Value::String("Beatrix".into()))]);
    let butch =
        session.create_node_with_props(&["Person"], [("name", Value::String("Butch".into()))]);

    // Center outgoing to all 5
    session.create_edge(mia, django, "LINK");
    session.create_edge(mia, shosanna, "LINK");
    session.create_edge(mia, hans, "LINK");
    session.create_edge(mia, beatrix, "LINK");
    session.create_edge(mia, butch, "LINK");

    // One incoming: Django -> Mia
    session.create_edge(django, mia, "LINK");

    db
}

/// Creates a diamond graph: two paths from A to D.
///
///   A -X-> B -X-> D
///   A -Y-> C -Y-> D
///
/// Edge types X and Y distinguish the two paths.
fn create_diamond() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let na = session.create_node_with_props(&["Node"], [("name", Value::String("A".into()))]);
    let nb = session.create_node_with_props(&["Node"], [("name", Value::String("B".into()))]);
    let nc = session.create_node_with_props(&["Node"], [("name", Value::String("C".into()))]);
    let nd = session.create_node_with_props(&["Node"], [("name", Value::String("D".into()))]);

    session.create_edge(na, nb, "X");
    session.create_edge(nb, nd, "X");
    session.create_edge(na, nc, "Y");
    session.create_edge(nc, nd, "Y");

    db
}

/// Creates a graph with richly-labelled and property-diverse nodes.
///
/// - Alix: Person, Employee, Manager, Admin (age: 30, city: "Amsterdam", active: true)
/// - Gus: Person (age: 0, city: "", active: false)
/// - Vincent: Nonexistent (marker label for negative tests is NOT applied)
///
/// Edge: Alix -KNOWS-> Gus (since: 2020)
fn create_property_edge_cases() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let alix = session.create_node_with_props(
        &["Person", "Employee", "Manager", "Admin"],
        [
            ("name", Value::String("Alix".into())),
            ("age", Value::Int64(30)),
            ("city", Value::String("Amsterdam".into())),
            ("active", Value::Bool(true)),
        ],
    );
    let gus = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Gus".into())),
            ("age", Value::Int64(0)),
            ("city", Value::String(String::new().into())),
            ("active", Value::Bool(false)),
        ],
    );

    let e1 = session.create_edge(alix, gus, "KNOWS");
    db.set_edge_property(e1, "since", Value::Int64(2020));

    db
}

/// Creates a long chain for deep-path tests.
///
/// N0 -> N1 -> N2 -> ... -> N19 (20 nodes, 19 LINK edges)
fn create_long_chain() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let mut prev =
        session.create_node_with_props(&["Node"], [("name", Value::String("N0".into()))]);

    for i in 1..20 {
        let next = session
            .create_node_with_props(&["Node"], [("name", Value::String(format!("N{i}").into()))]);
        session.create_edge(prev, next, "LINK");
        prev = next;
    }

    db
}

/// Creates a graph with multiple edge types between same-label nodes.
///
/// Alix (Person) and Gus (Person):
/// - Alix -A-> Gus
/// - Alix -B-> Gus
/// - Alix -C-> Gus
/// - Alix -D-> Gus
/// - Alix -E-> Gus
fn create_multi_edge_types() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let alix =
        session.create_node_with_props(&["Person"], [("name", Value::String("Alix".into()))]);
    let gus = session.create_node_with_props(&["Person"], [("name", Value::String("Gus".into()))]);

    session.create_edge(alix, gus, "A");
    session.create_edge(alix, gus, "B");
    session.create_edge(alix, gus, "C");
    session.create_edge(alix, gus, "D");
    session.create_edge(alix, gus, "E");

    db
}

/// Creates a graph for bidirectional edge testing.
///
/// Alix -FOLLOWS-> Gus
/// Gus  -FOLLOWS-> Alix
/// Alix -KNOWS->  Vincent
fn create_bidirectional() -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let alix =
        session.create_node_with_props(&["Person"], [("name", Value::String("Alix".into()))]);
    let gus = session.create_node_with_props(&["Person"], [("name", Value::String("Gus".into()))]);
    let vincent =
        session.create_node_with_props(&["Person"], [("name", Value::String("Vincent".into()))]);

    session.create_edge(alix, gus, "FOLLOWS");
    session.create_edge(gus, alix, "FOLLOWS");
    session.create_edge(alix, vincent, "KNOWS");

    db
}

// ============================================================================
// Variable-length path bounds
// ============================================================================

#[test]
#[ignore = "zero-hop paths not implemented: min_hops.unwrap_or(1) forces minimum to 1"]
fn test_vl_zero_to_three_hops() {
    let db = create_chain();
    let session = db.session();

    // *0..3: should yield 0 hops (A itself), 1 hop (B), 2 hops (C), 3 hops (D)
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Step {name: 'A'})-[p:NEXT*0..3]->(b:Step)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 4, "0+1+2+3 hops = 4 results");

    let targets: Vec<&str> = result.rows.iter().filter_map(|r| r[1].as_str()).collect();
    assert!(targets.contains(&"A"), "0-hop returns source node itself");
    assert!(targets.contains(&"B"), "1-hop reaches B");
    assert!(targets.contains(&"C"), "2-hop reaches C");
    assert!(targets.contains(&"D"), "3-hop reaches D");
}

#[test]
#[ignore = "unbounded upper: translator sets max_hops=Some(1) when parser returns None, capping traversal"]
fn test_vl_one_or_more_unbounded() {
    let db = create_chain();
    let session = db.session();

    // *1.. means one or more hops with no upper limit.
    // From A on a 5-node chain: B(1), C(2), D(3), E(4)
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Step {name: 'A'})-[p:NEXT*1..]->(b:Step)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 4, "should reach all 4 downstream nodes");

    let targets: Vec<&str> = result.rows.iter().filter_map(|r| r[1].as_str()).collect();
    assert!(targets.contains(&"E"), "should reach the end of the chain");
}

#[test]
#[ignore = "bare * shorthand: parser returns (None, None), translator converts to (1, Some(1))"]
fn test_vl_bare_star_unbounded() {
    let db = create_chain();
    let session = db.session();

    // Bare * should mean zero-or-more (or one-or-more), not exactly 1.
    // Parser returns (None, None) for * with no digits/dots.
    // Translator currently turns that into (1, Some(1)) via unwrap_or.
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Step {name: 'A'})-[p:NEXT*]->(b:Step)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    // If bare * means 1-or-more: expect 4 rows (B, C, D, E)
    assert!(
        result.row_count() >= 4,
        "bare * should traverse beyond 1 hop, got {} rows",
        result.row_count()
    );
}

#[test]
#[ignore = "zero-hop paths not implemented: *0..0 requires self-match support"]
fn test_vl_zero_hops_only() {
    let db = create_chain();
    let session = db.session();

    // *0..0 should return only the source node itself (zero-length path).
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Step {name: 'C'})-[p:NEXT*0..0]->(b:Step)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1, "zero hops = source node only");
    assert_eq!(result.rows[0][0], Value::String("C".into()));
    assert_eq!(
        result.rows[0][1],
        Value::String("C".into()),
        "target should equal source for zero-hop"
    );
}

#[test]
fn test_vl_inverted_range() {
    let db = create_chain();
    let session = db.session();

    // *5..3: min > max, should produce zero results (impossible range).
    let result = session.execute_sql(
        "SELECT * FROM GRAPH_TABLE (
            MATCH (a:Step {name: 'A'})-[p:NEXT*5..3]->(b:Step)
            COLUMNS (a.name AS src, b.name AS tgt)
        )",
    );

    // Either zero rows or an error: both are acceptable for an inverted range.
    match result {
        Ok(r) => assert_eq!(
            r.row_count(),
            0,
            "inverted range (5..3) should return 0 rows"
        ),
        Err(_) => {} // An error is also acceptable
    }
}

#[test]
fn test_vl_deep_path_on_long_chain() {
    let db = create_long_chain();
    let session = db.session();

    // *1..20 on a 20-node chain from N0: should reach N1 through N19
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Node {name: 'N0'})-[p:LINK*1..19]->(b:Node)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    // 19 reachable nodes (N1..N19)
    assert_eq!(
        result.row_count(),
        19,
        "1..19 hops on a 20-node chain should yield 19 results"
    );

    let targets: Vec<&str> = result.rows.iter().filter_map(|r| r[1].as_str()).collect();
    assert!(targets.contains(&"N1"), "should reach N1 (1 hop)");
    assert!(targets.contains(&"N19"), "should reach N19 (19 hops)");
}

#[test]
fn test_vl_exact_four_hops() {
    let db = create_chain();
    let session = db.session();

    // *4..4 from A: should reach exactly E (A->B->C->D->E)
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Step {name: 'A'})-[p:NEXT*4..4]->(b:Step)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][1], Value::String("E".into()));
}

#[test]
fn test_vl_range_exceeding_chain_length() {
    let db = create_chain();
    let session = db.session();

    // *10..15 on a chain of length 4: no path is long enough
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Step {name: 'A'})-[p:NEXT*10..15]->(b:Step)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    assert_eq!(
        result.row_count(),
        0,
        "range beyond chain length should return 0 rows"
    );
}

// ============================================================================
// Path modes (Walk is default; Trail/Simple/Acyclic are not yet enforced)
// ============================================================================

#[test]
fn test_walk_mode_allows_revisits_on_cycle() {
    let db = create_cycle_graph();
    let session = db.session();

    // Walk mode (default): from Alix with *1..4, the cycle Alix->Gus->Vincent->Alix
    // allows revisiting Alix at hop 3, then continuing to Gus at hop 4.
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person {name: 'Alix'})-[p:KNOWS*1..4]->(b:Person)
                COLUMNS (a.name AS src, b.name AS tgt, LENGTH(p) AS hops)
            )",
        )
        .unwrap();

    // Walk mode on a triangle + spur (Alix->Jules):
    // 1 hop: Gus, Jules
    // 2 hops: Vincent (via Gus), (no path from Jules)
    // 3 hops: Alix (via Vincent, cycle complete)
    // 4 hops: Gus, Jules (continuing from Alix again)
    // Total: 2 + 1 + 1 + 2 = 6
    assert!(
        result.row_count() >= 6,
        "Walk mode should allow cycling: expected >= 6 rows, got {}",
        result.row_count()
    );

    // Verify we see Alix as a target (the cycle-back)
    let targets: Vec<&str> = result.rows.iter().filter_map(|r| r[1].as_str()).collect();
    assert!(
        targets.contains(&"Alix"),
        "Walk mode should revisit start node via cycle"
    );
}

#[test]
#[ignore = "TRAIL mode parsed but not enforced: translator always emits PathMode::Walk"]
fn test_trail_mode_no_repeated_edges() {
    let db = create_cycle_graph();
    let session = db.session();

    // TRAIL mode: no repeated edges. On the triangle with *1..6 from Alix,
    // the traversal should stop once all edges are used (at most 4 edges total).
    // This would need GQL-style syntax within the SQL/PGQ MATCH clause.
    // Testing with standard SQL/PGQ (which always uses Walk):
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person {name: 'Alix'})-[p:KNOWS*1..6]->(b:Person)
                COLUMNS (a.name AS src, b.name AS tgt, LENGTH(p) AS hops)
            )",
        )
        .unwrap();

    // With TRAIL enforced on the triangle (3 edges) + spur (1 edge = 4 edges total):
    // Maximum path length would be 4 (using each edge at most once).
    // With Walk, we can go further because edges repeat.
    // This test documents the expectation that TRAIL would cap results.
    let max_hops: i64 = result
        .rows
        .iter()
        .filter_map(|r| {
            r.last().and_then(|v| match v {
                Value::Int64(h) => Some(*h),
                _ => None,
            })
        })
        .max()
        .unwrap_or(0);

    assert!(
        max_hops <= 4,
        "TRAIL should limit max path to 4 edges (4 unique edges in graph), got {max_hops}"
    );
}

#[test]
#[ignore = "SIMPLE mode parsed but not enforced: translator always emits PathMode::Walk"]
fn test_simple_mode_no_repeated_nodes() {
    let db = create_cycle_graph();
    let session = db.session();

    // SIMPLE mode: no repeated nodes (except possibly start = end).
    // From Alix on the triangle, *1..6 should not revisit any node.
    // Maximum unique-node path: Alix->Gus->Vincent->Alix (3 hops, endpoint repeat allowed)
    // Plus: Alix->Jules (1 hop, dead end)
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person {name: 'Alix'})-[p:KNOWS*1..6]->(b:Person)
                COLUMNS (a.name AS src, b.name AS tgt, LENGTH(p) AS hops)
            )",
        )
        .unwrap();

    // With SIMPLE enforced, we should not see hops > 3 (triangle) or > 1 (spur).
    // Total rows: Gus(1), Jules(1), Vincent(2), Alix(3) = 4
    // Walk mode produces more than 4 because it keeps cycling.
    assert!(
        result.row_count() <= 4,
        "SIMPLE should yield at most 4 results (no repeated interior nodes), got {}",
        result.row_count()
    );
}

#[test]
#[ignore = "ACYCLIC mode parsed but not enforced: translator always emits PathMode::Walk"]
fn test_acyclic_mode_no_cycles() {
    let db = create_cycle_graph();
    let session = db.session();

    // ACYCLIC mode: no repeated nodes at all (not even start = end).
    // From Alix: Gus(1), Jules(1), Vincent(2), and that's it. No Alix(3) cycle-back.
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person {name: 'Alix'})-[p:KNOWS*1..6]->(b:Person)
                COLUMNS (a.name AS src, b.name AS tgt, LENGTH(p) AS hops)
            )",
        )
        .unwrap();

    // ACYCLIC would produce at most 3 rows: Gus, Jules, Vincent
    // It must NOT include Alix as a target (that would be a cycle).
    let targets: Vec<&str> = result.rows.iter().filter_map(|r| r[1].as_str()).collect();
    assert!(
        !targets.contains(&"Alix"),
        "ACYCLIC should not return the start node via cycle-back"
    );
}

// ============================================================================
// Quantified patterns (rejected by translator, verify error)
// ============================================================================

#[test]
fn test_quantified_pattern_error() {
    let db = create_cycle_graph();
    let session = db.session();

    // Quantified sub-path `{1,3}` is parsed as a GQL pattern but rejected by
    // the SQL/PGQ translator with a semantic error.
    // Note: the parser may not support this syntax directly, so a parse error is
    // also acceptable. The key requirement is a clear error, not a panic.
    let result = session.execute_sql(
        "SELECT * FROM GRAPH_TABLE (
            MATCH ((a:Person)-[:KNOWS]->(b:Person)){1,3}
            COLUMNS (a.name AS src, b.name AS tgt)
        )",
    );

    assert!(
        result.is_err(),
        "Quantified patterns should produce an error, not silent success"
    );
}

// ============================================================================
// Graph name routing
// ============================================================================

#[test]
fn test_graph_name_simple_identifier() {
    let db = create_chain();
    let session = db.session();

    // Parser supports `GRAPH_TABLE(graph_name, MATCH ...)`.
    // The translator currently ignores the graph name and uses the default graph.
    // This test verifies parsing succeeds and execution uses the default graph.
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (my_graph, MATCH (a:Step) COLUMNS (a.name AS name))",
        )
        .unwrap();

    assert_eq!(
        result.row_count(),
        5,
        "graph name is currently ignored, should scan all nodes in default graph"
    );
}

#[test]
fn test_graph_name_nonexistent() {
    let db = create_chain();
    let session = db.session();

    // Nonexistent graph name: currently treated same as default (ignored).
    // Documents that no error is raised for unknown graph names.
    let result = session.execute_sql(
        "SELECT * FROM GRAPH_TABLE (nonexistent_graph, MATCH (a:Step) COLUMNS (a.name AS name))",
    );

    // Currently succeeds (graph name ignored). If graph routing is implemented,
    // this should become an error. For now, document the actual behavior.
    match result {
        Ok(r) => assert_eq!(
            r.row_count(),
            5,
            "nonexistent graph name currently falls through to default"
        ),
        Err(_) => {} // An error is also acceptable if routing is implemented
    }
}

// ============================================================================
// Complex multi-hop patterns
// ============================================================================

#[test]
fn test_four_node_chain_with_different_edge_types() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // Build: Alix -KNOWS-> Gus -WORKS_WITH-> Vincent -MANAGES-> Jules
    let alix =
        session.create_node_with_props(&["Person"], [("name", Value::String("Alix".into()))]);
    let gus = session.create_node_with_props(&["Person"], [("name", Value::String("Gus".into()))]);
    let vincent =
        session.create_node_with_props(&["Person"], [("name", Value::String("Vincent".into()))]);
    let jules =
        session.create_node_with_props(&["Person"], [("name", Value::String("Jules".into()))]);

    session.create_edge(alix, gus, "KNOWS");
    session.create_edge(gus, vincent, "WORKS_WITH");
    session.create_edge(vincent, jules, "MANAGES");

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[e1:KNOWS]->(b:Person)-[e2:WORKS_WITH]->(c:Person)-[e3:MANAGES]->(d:Person)
                COLUMNS (a.name AS n1, b.name AS n2, c.name AS n3, d.name AS n4)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1, "exactly one 4-node chain");
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
    assert_eq!(result.rows[0][1], Value::String("Gus".into()));
    assert_eq!(result.rows[0][2], Value::String("Vincent".into()));
    assert_eq!(result.rows[0][3], Value::String("Jules".into()));
}

#[test]
fn test_diamond_pattern_finds_both_paths() {
    let db = create_diamond();
    let session = db.session();

    // Two 2-hop paths from A to D: A->B->D (via X) and A->C->D (via Y)
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Node {name: 'A'})-[]->(mid:Node)-[]->(d:Node {name: 'D'})
                COLUMNS (a.name AS src, mid.name AS middle, d.name AS dst)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 2, "diamond has two paths A->?->D");

    let middles: Vec<&str> = result.rows.iter().filter_map(|r| r[1].as_str()).collect();
    assert!(middles.contains(&"B"), "path via B");
    assert!(middles.contains(&"C"), "path via C");
}

#[test]
fn test_star_pattern_outgoing_from_center() {
    let db = create_star();
    let session = db.session();

    // Mia has 5 outgoing LINK edges
    // Note: `outer` is a SQL keyword in this parser, so we use `spoke` as the variable name.
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (hub:Person {name: 'Mia'})-[:LINK]->(spoke:Person)
                COLUMNS (hub.name AS hub_name, spoke.name AS spoke_name)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 5, "Mia has 5 outgoing edges");

    let spokes: Vec<&str> = result.rows.iter().filter_map(|r| r[1].as_str()).collect();
    assert!(spokes.contains(&"Django"));
    assert!(spokes.contains(&"Shosanna"));
    assert!(spokes.contains(&"Hans"));
    assert!(spokes.contains(&"Beatrix"));
    assert!(spokes.contains(&"Butch"));
}

#[test]
fn test_star_pattern_incoming_to_center() {
    let db = create_star();
    let session = db.session();

    // Only Django -LINK-> Mia as incoming.
    // Note: `outer` is a SQL keyword in this parser, so we use `spoke` as the variable name.
    // Also: inline property filters on the *target* node of a path pattern do not
    // always filter correctly; use WHERE for reliable filtering.
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (spoke:Person)-[:LINK]->(hub:Person)
                WHERE hub.name = 'Mia'
                COLUMNS (spoke.name AS from_node, hub.name AS to_node)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1, "only Django has incoming to Mia");
    assert_eq!(result.rows[0][0], Value::String("Django".into()));
}

#[test]
fn test_triangle_cycle_detection() {
    let db = create_cycle_graph();
    let session = db.session();

    // Detect the triangle: (a)->(b)->(c)->(a)
    // Only cycle: Alix -> Gus -> Vincent -> Alix
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(d:Person)
                WHERE a.name = d.name
                COLUMNS (a.name AS start, b.name AS mid1, c.name AS mid2, d.name AS back)
            )",
        )
        .unwrap();

    // Three rotations of the triangle, depending on start node:
    // Alix->Gus->Vincent->Alix, Gus->Vincent->Alix->Gus, Vincent->Alix->Gus->Vincent
    // But we filter a.name = d.name, so each rotation produces 1 row.
    assert_eq!(
        result.row_count(),
        3,
        "triangle has 3 rotations where start = end"
    );

    // Verify all are valid cycles (start == end for each row)
    for row in &result.rows {
        assert_eq!(row[0], row[3], "start should equal end in a cycle");
    }
}

// ============================================================================
// Node pattern edge cases
// ============================================================================

#[test]
fn test_node_with_many_labels() {
    let db = create_property_edge_cases();
    let session = db.session();

    // Alix has labels: Person, Employee, Manager, Admin
    // Querying by any single label should find Alix
    for label in &["Person", "Employee", "Manager", "Admin"] {
        let query = format!(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:{label})
                WHERE n.name = 'Alix'
                COLUMNS (n.name AS name)
            )"
        );
        let result = session.execute_sql(&query).unwrap();
        assert_eq!(
            result.row_count(),
            1,
            "Alix should be found by label {label}"
        );
    }
}

#[test]
fn test_node_with_nonexistent_label() {
    let db = create_property_edge_cases();
    let session = db.session();

    // No nodes have label NonExistent
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:NonExistent)
                COLUMNS (n.name AS name)
            )",
        )
        .unwrap();

    assert_eq!(
        result.row_count(),
        0,
        "querying a label that no node has should return 0 rows"
    );
}

#[test]
fn test_node_with_zero_age() {
    let db = create_property_edge_cases();
    let session = db.session();

    // Gus has age: 0, which is a valid value (not NULL, not missing)
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person {age: 0})
                COLUMNS (n.name AS name, n.age AS age)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Gus".into()));
    assert_eq!(result.rows[0][1], Value::Int64(0));
}

#[test]
fn test_node_with_empty_string_property() {
    let db = create_property_edge_cases();
    let session = db.session();

    // Gus has city: '' (empty string, not null)
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.city = ''
                COLUMNS (n.name AS name, n.city AS city)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Gus".into()));
}

#[test]
fn test_node_with_false_boolean() {
    let db = create_property_edge_cases();
    let session = db.session();

    // Gus has active: false
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (n:Person)
                WHERE n.active = FALSE
                COLUMNS (n.name AS name, n.active AS active)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Gus".into()));
    assert_eq!(result.rows[0][1], Value::Bool(false));
}

// ============================================================================
// Edge pattern edge cases
// ============================================================================

#[test]
fn test_edge_same_label_different_nodes() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // Two Person nodes both named "Alix" (different entities)
    let a1 = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Alix".into())),
            ("id", Value::Int64(1)),
        ],
    );
    let a2 = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Alix".into())),
            ("id", Value::Int64(2)),
        ],
    );

    session.create_edge(a1, a2, "KNOWS");

    // Query where a.name = b.name: both are "Alix" but they are different nodes
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[:KNOWS]->(b:Person)
                WHERE a.name = b.name
                COLUMNS (a.name AS src_name, a.id AS src_id, b.name AS tgt_name, b.id AS tgt_id)
            )",
        )
        .unwrap();

    assert_eq!(
        result.row_count(),
        1,
        "edge between two nodes with same name should match"
    );
    // Verify they are different nodes (different id values)
    assert_ne!(
        result.rows[0][1], result.rows[0][3],
        "source and target should be different nodes despite same name"
    );
}

#[test]
fn test_edge_many_type_alternatives() {
    let db = create_multi_edge_types();
    let session = db.session();

    // Match edges of type A, B, C, D, or E using pipe syntax
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[:A|B|C|D|E]->(b:Person)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    assert_eq!(
        result.row_count(),
        5,
        "5 edges of different types between Alix and Gus"
    );
}

#[test]
fn test_edge_subset_type_alternatives() {
    let db = create_multi_edge_types();
    let session = db.session();

    // Match only types A and C
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[:A|C]->(b:Person)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 2, "only types A and C should match");
}

#[test]
fn test_bidirectional_edges_outgoing() {
    let db = create_bidirectional();
    let session = db.session();

    // Outgoing FOLLOWS from Alix: only Alix -> Gus
    let outgoing = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person {name: 'Alix'})-[:FOLLOWS]->(b:Person)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();
    assert_eq!(outgoing.row_count(), 1);
    assert_eq!(outgoing.rows[0][1], Value::String("Gus".into()));
}

#[test]
fn test_bidirectional_edges_incoming_via_where() {
    let db = create_bidirectional();
    let session = db.session();

    // Find who FOLLOWS Alix using WHERE on target instead of inline property.
    // Inline properties on target node in a path pattern may not filter correctly
    // in all cases, so use WHERE for reliable filtering.
    let incoming = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (b:Person)-[:FOLLOWS]->(a:Person)
                WHERE a.name = 'Alix'
                COLUMNS (b.name AS src, a.name AS tgt)
            )",
        )
        .unwrap();

    assert_eq!(incoming.row_count(), 1, "only Gus FOLLOWS Alix");
    assert_eq!(incoming.rows[0][0], Value::String("Gus".into()));
}

#[test]
fn test_edge_without_brackets_shorthand() {
    let db = create_chain();
    let session = db.session();

    // `->` (Arrow token, no edge brackets) should match any edge type.
    // Note: `-->` is NOT valid because `--` tokenizes as DoubleDash (undirected).
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Step {name: 'A'})->(b:Step)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    // A has one outgoing NEXT edge to B
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][1], Value::String("B".into()));
}

#[test]
fn test_undirected_edge_without_brackets() {
    let db = create_chain();
    let session = db.session();

    // `--(b)` undirected shorthand, no brackets
    // From B: outgoing to C, "incoming" from A (undirected treats both)
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Step {name: 'B'})--(b:Step)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    // B is connected to A (incoming) and C (outgoing) via undirected matching
    assert_eq!(result.row_count(), 2);

    let targets: Vec<&str> = result.rows.iter().filter_map(|r| r[1].as_str()).collect();
    assert!(targets.contains(&"A"), "undirected should see A");
    assert!(targets.contains(&"C"), "undirected should see C");
}

// ============================================================================
// Pattern with WHERE on both nodes and edges
// ============================================================================

#[test]
fn test_compound_where_on_nodes_and_edges() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // Build a richer graph for compound filtering
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

    let e1 = session.create_edge(alix, gus, "KNOWS");
    db.set_edge_property(e1, "since", Value::Int64(2020));
    let e2 = session.create_edge(alix, vincent, "KNOWS");
    db.set_edge_property(e2, "since", Value::Int64(2023));
    let e3 = session.create_edge(gus, vincent, "KNOWS");
    db.set_edge_property(e3, "since", Value::Int64(2018));

    // Complex WHERE: source age > 25, target city = 'Berlin', edge since > 2019
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[e:KNOWS]->(b:Person)
                WHERE a.age > 25 AND b.city = 'Berlin' AND e.since > 2019
                COLUMNS (a.name AS src, b.name AS tgt, e.since AS yr)
            )",
        )
        .unwrap();

    // Only Alix (age 30 > 25) -> Gus (city Berlin) with since 2020 (> 2019)
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
    assert_eq!(result.rows[0][1], Value::String("Gus".into()));
    assert_eq!(result.rows[0][2], Value::Int64(2020));
}

#[test]
fn test_where_filters_all_rows() {
    let db = create_chain();
    let session = db.session();

    // Impossible condition: pos > 100 for nodes with pos 0..4
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Step)-[:NEXT]->(b:Step)
                WHERE a.pos > 100
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    assert_eq!(
        result.row_count(),
        0,
        "impossible WHERE should return 0 rows"
    );
}

#[test]
fn test_where_with_or_on_edge_and_node() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let alix = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Alix".into())),
            ("age", Value::Int64(30)),
        ],
    );
    let gus = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Gus".into())),
            ("age", Value::Int64(25)),
        ],
    );
    let vincent = session.create_node_with_props(
        &["Person"],
        [
            ("name", Value::String("Vincent".into())),
            ("age", Value::Int64(40)),
        ],
    );

    let e1 = session.create_edge(alix, gus, "KNOWS");
    db.set_edge_property(e1, "since", Value::Int64(2015));
    let e2 = session.create_edge(alix, vincent, "KNOWS");
    db.set_edge_property(e2, "since", Value::Int64(2023));

    // OR: either target age < 26 OR edge since > 2022
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person {name: 'Alix'})-[e:KNOWS]->(b:Person)
                WHERE b.age < 26 OR e.since > 2022
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    // Gus (age 25 < 26) matches first condition
    // Vincent (since 2023 > 2022) matches second condition
    assert_eq!(result.row_count(), 2, "OR should match both edges");
}

#[test]
fn test_where_with_function_and_property() {
    let db = create_chain();
    let session = db.session();

    // Combine function call (LENGTH on a variable-length path) with node property filter
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Step {name: 'A'})-[p:NEXT*1..4]->(b:Step)
                WHERE b.pos >= 3
                COLUMNS (a.name AS src, b.name AS tgt, b.pos AS pos, LENGTH(p) AS hops)
            )",
        )
        .unwrap();

    // pos >= 3: D (pos 3, 3 hops) and E (pos 4, 4 hops)
    assert_eq!(result.row_count(), 2);

    let targets: Vec<&str> = result.rows.iter().filter_map(|r| r[1].as_str()).collect();
    assert!(targets.contains(&"D"));
    assert!(targets.contains(&"E"));
}

// ============================================================================
// Multi-pattern combinations
// ============================================================================

#[test]
fn test_multi_pattern_with_edge_and_node_patterns() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // Create persons and cities
    let alix =
        session.create_node_with_props(&["Person"], [("name", Value::String("Alix".into()))]);
    let gus = session.create_node_with_props(&["Person"], [("name", Value::String("Gus".into()))]);
    session.create_node_with_props(&["City"], [("name", Value::String("Amsterdam".into()))]);
    session.create_node_with_props(&["City"], [("name", Value::String("Berlin".into()))]);

    session.create_edge(alix, gus, "KNOWS");

    // Cross product of an edge pattern with a node pattern:
    // (a)-[:KNOWS]->(b) x (c:City) = 1 edge x 2 cities = 2 rows
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[:KNOWS]->(b:Person), (c:City)
                COLUMNS (a.name AS src, b.name AS tgt, c.name AS city)
            )",
        )
        .unwrap();

    assert_eq!(
        result.row_count(),
        2,
        "1 KNOWS edge x 2 cities = 2 cross-product rows"
    );
}

// ============================================================================
// Variable-length with edge types and undirected
// ============================================================================

#[test]
fn test_vl_undirected_on_cycle() {
    let db = create_cycle_graph();
    let session = db.session();

    // Undirected *1..1 from Alix: all nodes connected by any KNOWS edge
    // Outgoing: Gus, Jules. Incoming: Vincent (via Vincent->Alix).
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person {name: 'Alix'})-[p:KNOWS*1..1]-(b:Person)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    assert_eq!(
        result.row_count(),
        3,
        "undirected 1-hop from Alix: Gus (out), Jules (out), Vincent (in)"
    );

    let targets: Vec<&str> = result.rows.iter().filter_map(|r| r[1].as_str()).collect();
    assert!(targets.contains(&"Gus"));
    assert!(targets.contains(&"Jules"));
    assert!(targets.contains(&"Vincent"));
}

// ============================================================================
// Edge with empty brackets (no variable, no type)
// ============================================================================

#[test]
fn test_edge_empty_brackets() {
    let db = create_chain();
    let session = db.session();

    // `-[]->`  empty brackets: match any edge type, no variable binding
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Step {name: 'A'})-[]->(b:Step)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1, "A has one outgoing edge to B");
    assert_eq!(result.rows[0][1], Value::String("B".into()));
}

// ============================================================================
// Variable-length path: exact hop with shorthand
// ============================================================================

#[test]
fn test_vl_shorthand_exact_two() {
    let db = create_chain();
    let session = db.session();

    // *2 (no dots) should mean exactly 2 hops
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Step {name: 'A'})-[p:NEXT*2]->(b:Step)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1, "exactly 2 hops from A reaches C");
    assert_eq!(result.rows[0][1], Value::String("C".into()));
}

// ============================================================================
// Mixed directions in multi-hop pattern
// ============================================================================

#[test]
fn test_outgoing_then_incoming_in_pattern() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    // Alix -> Gus and Vincent -> Gus (Gus receives from both)
    let alix =
        session.create_node_with_props(&["Person"], [("name", Value::String("Alix".into()))]);
    let gus = session.create_node_with_props(&["Person"], [("name", Value::String("Gus".into()))]);
    let vincent =
        session.create_node_with_props(&["Person"], [("name", Value::String("Vincent".into()))]);

    session.create_edge(alix, gus, "KNOWS");
    session.create_edge(vincent, gus, "KNOWS");

    // Pattern: (a)-[:KNOWS]->(b)<-[:KNOWS]-(c) where a != c
    // Should find: (Alix)->Gus<-(Vincent) and (Vincent)->Gus<-(Alix)
    // Note: `left` is a SQL keyword in this parser, so we use different aliases.
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[:KNOWS]->(b:Person)<-[:KNOWS]-(c:Person)
                WHERE a.name <> c.name
                COLUMNS (a.name AS src, b.name AS mid, c.name AS dst)
            )",
        )
        .unwrap();

    assert_eq!(
        result.row_count(),
        2,
        "two orientations of the converging pattern"
    );
}

// ============================================================================
// Edge property combined with path traversal
// ============================================================================

#[test]
fn test_vl_with_where_on_endpoint_properties() {
    let db = create_chain();
    let session = db.session();

    // Variable-length 1..3, but filter on target property
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Step {name: 'A'})-[p:NEXT*1..3]->(b:Step)
                WHERE b.pos <= 2
                COLUMNS (a.name AS src, b.name AS tgt, b.pos AS pos)
            )",
        )
        .unwrap();

    // 1 hop: B (pos 1), 2 hops: C (pos 2), 3 hops: D (pos 3, filtered out)
    assert_eq!(result.row_count(), 2, "pos <= 2 filters D out");

    let targets: Vec<&str> = result.rows.iter().filter_map(|r| r[1].as_str()).collect();
    assert!(targets.contains(&"B"));
    assert!(targets.contains(&"C"));
}

// ============================================================================
// Empty graph
// ============================================================================

#[test]
fn test_empty_graph_pattern_query() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[:KNOWS]->(b:Person)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 0, "empty graph has no matches");
}

#[test]
fn test_empty_graph_node_query() {
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

    assert_eq!(result.row_count(), 0, "empty graph has no nodes");
}

// ============================================================================
// Isolated node in edge query
// ============================================================================

#[test]
fn test_isolated_node_not_in_edge_results() {
    let db = GrafeoDB::new_in_memory();
    let session = db.session();

    let alix =
        session.create_node_with_props(&["Person"], [("name", Value::String("Alix".into()))]);
    let gus = session.create_node_with_props(&["Person"], [("name", Value::String("Gus".into()))]);
    // Jules is isolated (no edges)
    session.create_node_with_props(&["Person"], [("name", Value::String("Jules".into()))]);

    session.create_edge(alix, gus, "KNOWS");

    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Person)-[:KNOWS]->(b:Person)
                COLUMNS (a.name AS src, b.name AS tgt)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 1, "only the one edge matches");
    assert_eq!(result.rows[0][0], Value::String("Alix".into()));
    assert_eq!(result.rows[0][1], Value::String("Gus".into()));
}

// ============================================================================
// COLUMNS clause with computed expressions involving edge properties
// ============================================================================

#[test]
fn test_columns_computed_from_edge_property() {
    let db = create_chain();
    let session = db.session();

    // Edge weight * 10 as a computed column
    let result = session
        .execute_sql(
            "SELECT * FROM GRAPH_TABLE (
                MATCH (a:Step)-[e:NEXT]->(b:Step)
                COLUMNS (a.name AS src, b.name AS tgt, e.weight * 10 AS scaled)
            )",
        )
        .unwrap();

    assert_eq!(result.row_count(), 4, "4 edges in the chain");

    // Find the A->B edge (weight 1) and verify computation
    let ab_row = result
        .rows
        .iter()
        .find(|r| r[0].as_str() == Some("A"))
        .expect("A->B row should exist");
    assert_eq!(ab_row[2], Value::Int64(10), "weight 1 * 10 = 10");
}
