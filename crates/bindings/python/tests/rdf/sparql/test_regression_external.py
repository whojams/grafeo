"""Regression tests for SPARQL query support, inspired by bugs in RDF databases.

Covers OPTIONAL, FILTER, aggregation, UNION, INSERT/DELETE DATA,
literal type handling, and blank node edge cases.

Run: pytest tests/rdf/sparql/test_regression_external.py -v
"""


# =============================================================================
# OPTIONAL clause producing correct NULL bindings
# Inspired by Jena and Blazegraph OPTIONAL bugs
# =============================================================================


class TestSparqlOptional:
    """OPTIONAL must produce NULL bindings for non-matching patterns."""

    def test_optional_unmatched_produces_null(self, db):
        """OPTIONAL that matches nothing should produce unbound variable."""
        db.execute_sparql('INSERT DATA { <http://ex.org/alix> <http://ex.org/name> "Alix" . }')
        result = list(
            db.execute_sparql(
                "SELECT ?name ?age WHERE { "
                "  <http://ex.org/alix> <http://ex.org/name> ?name . "
                "  OPTIONAL { <http://ex.org/alix> <http://ex.org/age> ?age } "
                "}"
            )
        )
        assert len(result) == 1
        assert result[0]["name"] == "Alix"
        assert result[0]["age"] is None

    def test_optional_matched_returns_value(self, db):
        db.execute_sparql(
            "INSERT DATA { "
            '  <http://ex.org/alix> <http://ex.org/name> "Alix" . '
            '  <http://ex.org/alix> <http://ex.org/age> "30" . '
            "}"
        )
        result = list(
            db.execute_sparql(
                "SELECT ?name ?age WHERE { "
                "  <http://ex.org/alix> <http://ex.org/name> ?name . "
                "  OPTIONAL { <http://ex.org/alix> <http://ex.org/age> ?age } "
                "}"
            )
        )
        assert len(result) == 1
        assert result[0]["name"] == "Alix"
        assert result[0]["age"] is not None


# =============================================================================
# FILTER edge cases
# =============================================================================


class TestSparqlFilter:
    """FILTER with various comparison and type operators."""

    def test_filter_greater_than(self, db):
        db.execute_sparql(
            "INSERT DATA { "
            '  <http://ex.org/a> <http://ex.org/val> "10" . '
            '  <http://ex.org/b> <http://ex.org/val> "20" . '
            '  <http://ex.org/c> <http://ex.org/val> "30" . '
            "}"
        )
        result = list(
            db.execute_sparql('SELECT ?s ?v WHERE { ?s <http://ex.org/val> ?v FILTER(?v > "15") }')
        )
        assert len(result) == 2

    def test_filter_regex(self, db):
        db.execute_sparql(
            "INSERT DATA { "
            '  <http://ex.org/a> <http://ex.org/name> "Alix" . '
            '  <http://ex.org/b> <http://ex.org/name> "Gus" . '
            '  <http://ex.org/c> <http://ex.org/name> "Vincent" . '
            "}"
        )
        result = list(
            db.execute_sparql(
                'SELECT ?n WHERE { ?s <http://ex.org/name> ?n FILTER(REGEX(?n, "^A")) }'
            )
        )
        names = [r["n"] for r in result]
        assert "Alix" in names
        assert "Gus" not in names

    def test_filter_not_exists(self, db):
        db.execute_sparql(
            "INSERT DATA { "
            '  <http://ex.org/a> <http://ex.org/name> "Alix" . '
            '  <http://ex.org/a> <http://ex.org/age> "30" . '
            '  <http://ex.org/b> <http://ex.org/name> "Gus" . '
            "}"
        )
        # Find subjects that have a name but no age
        result = list(
            db.execute_sparql(
                "SELECT ?n WHERE { "
                "  ?s <http://ex.org/name> ?n . "
                "  FILTER NOT EXISTS { ?s <http://ex.org/age> ?a } "
                "}"
            )
        )
        names = [r["n"] for r in result]
        assert "Gus" in names
        assert "Alix" not in names


# =============================================================================
# SPARQL aggregation
# =============================================================================


class TestSparqlAggregation:
    """GROUP BY, COUNT, SUM, AVG in SPARQL."""

    def test_count_with_group_by(self, db):
        db.execute_sparql(
            "INSERT DATA { "
            '  <http://ex.org/a> <http://ex.org/type> "Person" . '
            '  <http://ex.org/b> <http://ex.org/type> "Person" . '
            '  <http://ex.org/c> <http://ex.org/type> "City" . '
            "}"
        )
        result = list(
            db.execute_sparql(
                "SELECT ?t (COUNT(?s) AS ?cnt) WHERE { "
                "  ?s <http://ex.org/type> ?t "
                "} GROUP BY ?t ORDER BY ?t"
            )
        )
        assert len(result) == 2

    def test_count_on_empty_returns_zero(self, db):
        result = list(
            db.execute_sparql(
                "SELECT (COUNT(?s) AS ?cnt) WHERE { ?s <http://ex.org/nonexistent> ?o }"
            )
        )
        assert len(result) == 1
        assert result[0]["cnt"] == 0


# =============================================================================
# UNION semantics
# =============================================================================


class TestSparqlUnion:
    """SPARQL UNION must combine results from both branches."""

    def test_union_returns_both_branches(self, db):
        db.execute_sparql(
            "INSERT DATA { "
            '  <http://ex.org/a> <http://ex.org/name> "Alix" . '
            '  <http://ex.org/a> <http://ex.org/city> "Amsterdam" . '
            "}"
        )
        result = list(
            db.execute_sparql(
                "SELECT ?val WHERE { "
                "  { <http://ex.org/a> <http://ex.org/name> ?val } "
                "  UNION "
                "  { <http://ex.org/a> <http://ex.org/city> ?val } "
                "}"
            )
        )
        vals = {r["val"] for r in result}
        assert "Alix" in vals
        assert "Amsterdam" in vals


# =============================================================================
# INSERT DATA and DELETE DATA
# =============================================================================


class TestSparqlMutations:
    """INSERT DATA and DELETE DATA must be correct."""

    def test_insert_and_query(self, db):
        db.execute_sparql('INSERT DATA { <http://ex.org/x> <http://ex.org/p> "hello" . }')
        result = list(
            db.execute_sparql("SELECT ?o WHERE { <http://ex.org/x> <http://ex.org/p> ?o }")
        )
        assert len(result) == 1
        assert result[0]["o"] == "hello"

    def test_delete_data(self, db):
        db.execute_sparql('INSERT DATA { <http://ex.org/x> <http://ex.org/p> "hello" . }')
        db.execute_sparql('DELETE DATA { <http://ex.org/x> <http://ex.org/p> "hello" . }')
        result = list(
            db.execute_sparql("SELECT ?o WHERE { <http://ex.org/x> <http://ex.org/p> ?o }")
        )
        assert len(result) == 0

    def test_delete_then_reinsert(self, db):
        db.execute_sparql('INSERT DATA { <http://ex.org/x> <http://ex.org/p> "old" . }')
        db.execute_sparql('DELETE DATA { <http://ex.org/x> <http://ex.org/p> "old" . }')
        db.execute_sparql('INSERT DATA { <http://ex.org/x> <http://ex.org/p> "new" . }')
        result = list(
            db.execute_sparql("SELECT ?o WHERE { <http://ex.org/x> <http://ex.org/p> ?o }")
        )
        assert len(result) == 1
        assert result[0]["o"] == "new"


# =============================================================================
# DISTINCT in SPARQL
# =============================================================================


class TestSparqlDistinct:
    """SELECT DISTINCT must deduplicate results."""

    def test_distinct_removes_duplicates(self, db):
        db.execute_sparql(
            "INSERT DATA { "
            '  <http://ex.org/a> <http://ex.org/type> "Person" . '
            '  <http://ex.org/a> <http://ex.org/name> "Alix" . '
            '  <http://ex.org/b> <http://ex.org/type> "Person" . '
            '  <http://ex.org/b> <http://ex.org/name> "Gus" . '
            "}"
        )
        result = list(db.execute_sparql("SELECT DISTINCT ?t WHERE { ?s <http://ex.org/type> ?t }"))
        # Both subjects have type "Person", DISTINCT should collapse to 1
        assert len(result) == 1
        assert result[0]["t"] == "Person"


# =============================================================================
# LIMIT and OFFSET in SPARQL
# =============================================================================


class TestSparqlLimitOffset:
    """LIMIT and OFFSET must paginate correctly."""

    def test_limit(self, db):
        for i in range(10):
            db.execute_sparql(f'INSERT DATA {{ <http://ex.org/n{i}> <http://ex.org/val> "{i}" . }}')
        result = list(db.execute_sparql("SELECT ?s WHERE { ?s <http://ex.org/val> ?v } LIMIT 3"))
        assert len(result) == 3

    def test_offset(self, db):
        for i in range(5):
            db.execute_sparql(f'INSERT DATA {{ <http://ex.org/n{i}> <http://ex.org/val> "{i}" . }}')
        all_results = list(
            db.execute_sparql("SELECT ?v WHERE { ?s <http://ex.org/val> ?v } ORDER BY ?v")
        )
        offset_results = list(
            db.execute_sparql("SELECT ?v WHERE { ?s <http://ex.org/val> ?v } ORDER BY ?v OFFSET 2")
        )
        assert len(offset_results) == len(all_results) - 2


# =============================================================================
# ORDER BY in SPARQL
# =============================================================================


class TestSparqlOrderBy:
    """ORDER BY must sort results correctly."""

    def test_order_by_ascending(self, db):
        db.execute_sparql(
            "INSERT DATA { "
            '  <http://ex.org/c> <http://ex.org/name> "Vincent" . '
            '  <http://ex.org/a> <http://ex.org/name> "Alix" . '
            '  <http://ex.org/b> <http://ex.org/name> "Gus" . '
            "}"
        )
        result = list(
            db.execute_sparql("SELECT ?n WHERE { ?s <http://ex.org/name> ?n } ORDER BY ?n")
        )
        names = [r["n"] for r in result]
        assert names == sorted(names)
