//! Regression tests for complex expressions in SPARQL GROUP BY and ORDER BY.
//! Verifies that the RDF planner handles expression-based aggregation and
//! sorting gracefully (returns a clean error, does not panic).
//!
//! Currently, STR() in GROUP BY / ORDER BY parses correctly but the physical
//! plan lacks the store reference needed for expression evaluation. These tests
//! verify the failure mode is a clean `Err`, not a panic.
//!
//! ```bash
//! cargo test -p grafeo-engine --all-features --test sparql_aggregate_expressions
//! ```

#[cfg(all(feature = "sparql", feature = "rdf"))]
mod sparql_aggregate_expression_tests {
    use grafeo_engine::GrafeoDB;

    fn rdf_db() -> GrafeoDB {
        GrafeoDB::new_in_memory()
    }

    fn insert_sample_triples(db: &GrafeoDB) {
        db.execute_sparql(
            r#"INSERT DATA {
                <http://ex.org/alix> <http://ex.org/name> "Alix" .
                <http://ex.org/alix> <http://ex.org/age>  "30" .
                <http://ex.org/gus>  <http://ex.org/name> "Gus" .
                <http://ex.org/gus>  <http://ex.org/age>  "25" .
            }"#,
        )
        .unwrap();
    }

    /// GROUP BY (STR(?s)): expression-based grouping should not panic.
    /// Currently returns an error because the project operator lacks a store
    /// reference for expression evaluation in the RDF path.
    #[test]
    fn sparql_group_by_str_with_count() {
        let db = rdf_db();
        insert_sample_triples(&db);

        let result = db.execute_sparql(
            "SELECT (STR(?s) AS ?subject) (COUNT(*) AS ?cnt) WHERE { ?s ?p ?o } GROUP BY (STR(?s))",
        );

        // Must not panic. When the store-ref issue is fixed, change to assert is_ok.
        if let Err(ref err) = result {
            let msg = format!("{err}");
            assert!(
                msg.contains("Store required") || msg.contains("expression"),
                "Expected a clean expression-eval error, got: {msg}"
            );
        }
    }

    /// ORDER BY ASC(STR(?s)): expression-based sorting should not panic.
    #[test]
    fn sparql_order_by_str() {
        let db = rdf_db();
        insert_sample_triples(&db);

        let result = db
            .execute_sparql("SELECT ?s WHERE { ?s <http://ex.org/name> ?o } ORDER BY ASC(STR(?s))");

        if let Err(ref err) = result {
            let msg = format!("{err}");
            assert!(
                msg.contains("Store required") || msg.contains("expression"),
                "Expected a clean expression-eval error, got: {msg}"
            );
        }
    }

    /// Both GROUP BY and ORDER BY with STR(): combined complex expressions.
    #[test]
    fn sparql_group_by_and_order_by_both_complex() {
        let db = rdf_db();
        insert_sample_triples(&db);

        let result = db.execute_sparql(
            "SELECT (STR(?s) AS ?subject) (COUNT(*) AS ?cnt) WHERE { ?s ?p ?o } GROUP BY (STR(?s)) ORDER BY (STR(?s))",
        );

        if let Err(ref err) = result {
            let msg = format!("{err}");
            assert!(
                msg.contains("Store required") || msg.contains("expression"),
                "Expected a clean expression-eval error, got: {msg}"
            );
        }
    }

    /// ORDER BY DESC(STR(?s)): descending with a function expression.
    #[test]
    fn sparql_order_by_desc_str() {
        let db = rdf_db();
        insert_sample_triples(&db);

        let result = db.execute_sparql(
            "SELECT ?s WHERE { ?s <http://ex.org/name> ?o } ORDER BY DESC(STR(?s))",
        );

        if let Err(ref err) = result {
            let msg = format!("{err}");
            assert!(
                msg.contains("Store required") || msg.contains("expression"),
                "Expected a clean expression-eval error, got: {msg}"
            );
        }
    }
}
