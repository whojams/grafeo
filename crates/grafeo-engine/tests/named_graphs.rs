//! Integration tests for SPARQL named graphs (RDF Datasets).
//!
//! Exercises: SPARQL translator graph_context_stack, RdfTripleScanOperator
//! graph-aware mode, RdfCreateGraphOperator, RdfDropGraphOperator,
//! RdfClearGraphOperator, and RdfStore named graph API.

#[cfg(all(feature = "sparql", feature = "rdf"))]
mod tests {
    use grafeo_engine::{Config, GrafeoDB, GraphModel};

    fn rdf_db() -> GrafeoDB {
        GrafeoDB::with_config(Config::in_memory().with_graph_model(GraphModel::Rdf)).unwrap()
    }

    // ==================== CREATE / DROP GRAPH ====================

    #[test]
    fn create_graph_succeeds() {
        let db = rdf_db();
        let session = db.session();
        let result = session.execute_sparql("CREATE GRAPH <http://ex.org/g1>");
        assert!(result.is_ok());
    }

    #[test]
    fn create_duplicate_graph_errors() {
        let db = rdf_db();
        let session = db.session();
        session
            .execute_sparql("CREATE GRAPH <http://ex.org/g1>")
            .unwrap();
        // Non-silent CREATE of existing graph should fail
        let result = session.execute_sparql("CREATE GRAPH <http://ex.org/g1>");
        assert!(result.is_err());
    }

    #[test]
    fn create_duplicate_graph_silent_ok() {
        let db = rdf_db();
        let session = db.session();
        session
            .execute_sparql("CREATE GRAPH <http://ex.org/g1>")
            .unwrap();
        let result = session.execute_sparql("CREATE SILENT GRAPH <http://ex.org/g1>");
        assert!(result.is_ok());
    }

    #[test]
    fn drop_graph_succeeds() {
        let db = rdf_db();
        let session = db.session();
        session
            .execute_sparql("CREATE GRAPH <http://ex.org/g1>")
            .unwrap();
        let result = session.execute_sparql("DROP GRAPH <http://ex.org/g1>");
        assert!(result.is_ok());
    }

    #[test]
    fn drop_nonexistent_graph_errors() {
        let db = rdf_db();
        let session = db.session();
        let result = session.execute_sparql("DROP GRAPH <http://ex.org/nope>");
        assert!(result.is_err());
    }

    #[test]
    fn drop_nonexistent_graph_silent_ok() {
        let db = rdf_db();
        let session = db.session();
        let result = session.execute_sparql("DROP SILENT GRAPH <http://ex.org/nope>");
        assert!(result.is_ok());
    }

    #[test]
    fn drop_default_clears_default_graph() {
        let db = rdf_db();
        let session = db.session();
        session
            .execute_sparql(
                r#"INSERT DATA {
                    <http://ex.org/a> <http://ex.org/p> "1" .
                }"#,
            )
            .unwrap();

        session.execute_sparql("DROP DEFAULT").unwrap();

        let result = session
            .execute_sparql("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
            .unwrap();
        assert_eq!(
            result.row_count(),
            0,
            "DROP DEFAULT should clear the default graph"
        );
    }

    // ==================== CLEAR GRAPH ====================

    #[test]
    fn clear_default_graph() {
        let db = rdf_db();
        let session = db.session();
        session
            .execute_sparql(
                r#"INSERT DATA {
                    <http://ex.org/a> <http://ex.org/p> "1" .
                    <http://ex.org/b> <http://ex.org/p> "2" .
                }"#,
            )
            .unwrap();

        session.execute_sparql("CLEAR DEFAULT").unwrap();

        let result = session
            .execute_sparql("SELECT ?s WHERE { ?s ?p ?o }")
            .unwrap();
        assert_eq!(result.row_count(), 0);
    }

    #[test]
    fn clear_named_graph_preserves_default() {
        let db = rdf_db();
        let session = db.session();

        // Insert into default graph
        session
            .execute_sparql(
                r#"INSERT DATA {
                    <http://ex.org/a> <http://ex.org/p> "1" .
                }"#,
            )
            .unwrap();

        // Create a named graph and populate it via store API
        let rdf = db.rdf_store();
        rdf.graph_or_create("http://ex.org/g1");
        if let Some(g) = rdf.graph("http://ex.org/g1") {
            use grafeo_core::graph::rdf::{Literal, Term, Triple};
            g.insert(Triple::new(
                Term::iri("http://ex.org/b"),
                Term::iri("http://ex.org/p"),
                Term::Literal(Literal::simple("2")),
            ));
        }

        // Clear named graph only
        session
            .execute_sparql("CLEAR GRAPH <http://ex.org/g1>")
            .unwrap();

        // Default graph still has data
        let result = session
            .execute_sparql("SELECT ?s WHERE { ?s ?p ?o }")
            .unwrap();
        assert_eq!(result.row_count(), 1, "Default graph should be untouched");

        // Named graph is empty
        let named = session
            .execute_sparql(
                r#"SELECT ?s WHERE {
                    GRAPH <http://ex.org/g1> { ?s ?p ?o }
                }"#,
            )
            .unwrap();
        assert_eq!(named.row_count(), 0, "Named graph should be cleared");
    }

    // ==================== GRAPH clause queries ====================

    #[test]
    fn graph_clause_queries_named_graph() {
        let db = rdf_db();

        // Populate named graph via store API
        let rdf = db.rdf_store();
        let g = rdf.graph_or_create("http://ex.org/g1");
        {
            use grafeo_core::graph::rdf::{Literal, Term, Triple};
            g.insert(Triple::new(
                Term::iri("http://ex.org/alice"),
                Term::iri("http://ex.org/name"),
                Term::Literal(Literal::simple("Alice")),
            ));
        }

        let session = db.session();
        let result = session
            .execute_sparql(
                r#"SELECT ?s ?name WHERE {
                    GRAPH <http://ex.org/g1> { ?s <http://ex.org/name> ?name }
                }"#,
            )
            .unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn graph_variable_scans_all_graphs() {
        let db = rdf_db();
        let session = db.session();

        // Insert into default graph
        session
            .execute_sparql(
                r#"INSERT DATA {
                    <http://ex.org/alice> <http://ex.org/name> "Alice" .
                }"#,
            )
            .unwrap();

        // Populate named graphs via store API
        let rdf = db.rdf_store();
        {
            use grafeo_core::graph::rdf::{Literal, Term, Triple};
            let g1 = rdf.graph_or_create("http://ex.org/g1");
            g1.insert(Triple::new(
                Term::iri("http://ex.org/bob"),
                Term::iri("http://ex.org/name"),
                Term::Literal(Literal::simple("Bob")),
            ));
            let g2 = rdf.graph_or_create("http://ex.org/g2");
            g2.insert(Triple::new(
                Term::iri("http://ex.org/carol"),
                Term::iri("http://ex.org/name"),
                Term::Literal(Literal::simple("Carol")),
            ));
        }

        // GRAPH ?g scans all graphs (default + named)
        let result = session
            .execute_sparql(
                r#"SELECT ?g ?s ?name WHERE {
                    GRAPH ?g { ?s <http://ex.org/name> ?name }
                }"#,
            )
            .unwrap();

        assert!(
            result.row_count() >= 3,
            "Expected >= 3 rows from all graphs, got {}",
            result.row_count()
        );
    }

    #[test]
    fn graph_clause_isolates_from_default() {
        let db = rdf_db();
        let session = db.session();

        // Insert into default graph
        session
            .execute_sparql(
                r#"INSERT DATA {
                    <http://ex.org/alice> <http://ex.org/name> "Alice" .
                }"#,
            )
            .unwrap();

        // Populate named graph via store API
        let rdf = db.rdf_store();
        {
            use grafeo_core::graph::rdf::{Literal, Term, Triple};
            let g = rdf.graph_or_create("http://ex.org/g1");
            g.insert(Triple::new(
                Term::iri("http://ex.org/bob"),
                Term::iri("http://ex.org/name"),
                Term::Literal(Literal::simple("Bob")),
            ));
        }

        // Default graph: only Alice
        let default_result = session
            .execute_sparql(r#"SELECT ?s WHERE { ?s <http://ex.org/name> ?name }"#)
            .unwrap();
        assert_eq!(
            default_result.row_count(),
            1,
            "Default graph should have 1 triple"
        );

        // Named graph: only Bob
        let named_result = session
            .execute_sparql(
                r#"SELECT ?s WHERE {
                    GRAPH <http://ex.org/g1> { ?s <http://ex.org/name> ?name }
                }"#,
            )
            .unwrap();
        assert_eq!(
            named_result.row_count(),
            1,
            "Named graph should have 1 triple"
        );
    }
}
