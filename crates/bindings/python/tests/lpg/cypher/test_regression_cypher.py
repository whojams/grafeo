"""Cypher-specific regression tests.

Covers correlated EXISTS subqueries, CASE WHEN inside aggregates,
any() with IN lists, CASE WHEN inside reduce(), and target node
property filters on edge patterns.
"""


# =============================================================================
# Bug 1: Correlated EXISTS subquery
# =============================================================================


class TestCorrelatedExistsSubquery:
    """Correlated EXISTS subqueries where inner pattern references outer MATCH variables."""

    def test_not_exists_with_type_filter(self, db):
        """NOT EXISTS with WHERE type(r) filter should exclude matched pairs."""
        db.execute_cypher("CREATE (a {name: 'Alix'}), (b {name: 'Gus'}), (c {name: 'Vincent'})")
        db.execute_cypher("MATCH (a {name: 'Alix'}), (b {name: 'Gus'}) CREATE (a)-[:KNOWS]->(b)")
        db.execute_cypher(
            "MATCH (a {name: 'Alix'}), (c {name: 'Vincent'}) CREATE (a)-[:WORKS_WITH]->(c)"
        )

        result = list(
            db.execute_cypher(
                "MATCH (x), (y) WHERE x <> y "
                "AND NOT EXISTS { MATCH (x)-[r]->(y) WHERE type(r) = 'KNOWS' } "
                "RETURN x.name, y.name"
            )
        )
        pairs = {(r["x.name"], r["y.name"]) for r in result}
        # (Alix, Gus) should be excluded (KNOWS edge exists)
        assert ("Alix", "Gus") not in pairs
        # (Alix, Vincent) should be included (edge is WORKS_WITH, not KNOWS)
        assert ("Alix", "Vincent") in pairs

    def test_not_exists_bare_pattern(self, db):
        """NOT EXISTS with bare pattern (no MATCH keyword) should work identically."""
        db.execute_cypher("CREATE (a {name: 'Alix'}), (b {name: 'Gus'}), (c {name: 'Vincent'})")
        db.execute_cypher("MATCH (a {name: 'Alix'}), (b {name: 'Gus'}) CREATE (a)-[:KNOWS]->(b)")
        db.execute_cypher(
            "MATCH (a {name: 'Alix'}), (c {name: 'Vincent'}) CREATE (a)-[:WORKS_WITH]->(c)"
        )

        result = list(
            db.execute_cypher(
                "MATCH (x), (y) WHERE x <> y "
                "AND NOT EXISTS { (x)-[r]->(y) WHERE type(r) = 'KNOWS' } "
                "RETURN x.name, y.name"
            )
        )
        pairs = {(r["x.name"], r["y.name"]) for r in result}
        assert ("Alix", "Gus") not in pairs
        assert ("Alix", "Vincent") in pairs


# =============================================================================
# Bug 2: CASE WHEN inside aggregate functions
# =============================================================================


class TestCaseWhenInAggregate:
    """CASE WHEN expressions used inside aggregate functions like sum()."""

    def test_sum_case_when(self, db):
        """sum(CASE WHEN ... THEN 1 ELSE 0 END) should count matching rows."""
        db.execute_cypher("CREATE (d:Dir {name: 'src'})")
        db.execute_cypher("CREATE (f1:File {name: 'a.py', file_type: 'source'})")
        db.execute_cypher("CREATE (f2:File {name: 'b.md', file_type: 'docs'})")
        db.execute_cypher("CREATE (f3:File {name: 'c.py', file_type: 'source'})")
        db.execute_cypher(
            "MATCH (d:Dir {name: 'src'}), (f:File {name: 'a.py'}) CREATE (d)-[:CONTAINS]->(f)"
        )
        db.execute_cypher(
            "MATCH (d:Dir {name: 'src'}), (f:File {name: 'b.md'}) CREATE (d)-[:CONTAINS]->(f)"
        )
        db.execute_cypher(
            "MATCH (d:Dir {name: 'src'}), (f:File {name: 'c.py'}) CREATE (d)-[:CONTAINS]->(f)"
        )

        result = list(
            db.execute_cypher(
                "MATCH (d:Dir)-[:CONTAINS]->(f:File) "
                "WITH d, count(f) AS total, "
                "     sum(CASE WHEN f.file_type = 'source' THEN 1 ELSE 0 END) AS source_count "
                "RETURN d.name AS name, total, source_count"
            )
        )
        assert len(result) == 1
        assert result[0]["name"] == "src"
        assert result[0]["total"] == 3
        assert result[0]["source_count"] == 2


# =============================================================================
# Bug 3: any() with IN list
# =============================================================================


class TestAnyWithInList:
    """any() list predicate with IN list comparison inside WHERE clause."""

    def test_any_labels_in_list_matches(self, db):
        """any(lbl IN labels(n) WHERE lbl IN ['A', 'B']) should match node with label A."""
        db.execute_cypher("CREATE (:A:B:C {name: 'Test'})")

        result = list(
            db.execute_cypher(
                "MATCH (n) WHERE any(lbl IN labels(n) WHERE lbl IN ['A', 'B']) RETURN n.name"
            )
        )
        assert len(result) == 1
        assert result[0]["n.name"] == "Test"

    def test_any_labels_in_list_no_match(self, db):
        """any() should return 0 rows when no labels match the IN list."""
        db.execute_cypher("CREATE (:A:B:C {name: 'Test'})")

        result = list(
            db.execute_cypher(
                "MATCH (n) WHERE any(lbl IN labels(n) WHERE lbl IN ['X', 'Y']) RETURN n.name"
            )
        )
        assert len(result) == 0

    def test_any_with_single_match(self, db):
        """any() should work when only one label matches from the IN list."""
        db.execute_cypher("CREATE (:Model:Component {name: 'Widget'})")

        result = list(
            db.execute_cypher(
                "MATCH (n) WHERE any(lbl IN labels(n) WHERE lbl IN ['Model', 'Service']) "
                "RETURN n.name"
            )
        )
        assert len(result) == 1
        assert result[0]["n.name"] == "Widget"


# =============================================================================
# Bug 4: CASE WHEN inside reduce()
# =============================================================================


class TestCaseWhenInReduce:
    """CASE WHEN expressions used inside reduce() accumulator body."""

    def test_reduce_with_case_max(self, db):
        """reduce() with CASE WHEN should compute max value."""
        result = list(
            db.execute_cypher(
                "WITH [3, 1, 4, 1, 5] AS vals "
                "RETURN reduce(acc = 0, x IN vals | "
                "  CASE WHEN x > acc THEN x ELSE acc END) AS max_val"
            )
        )
        assert len(result) == 1
        assert result[0]["max_val"] == 5

    def test_reduce_with_case_min(self, db):
        """reduce() with CASE WHEN should compute min value."""
        result = list(
            db.execute_cypher(
                "WITH [3, 1, 4, 1, 5] AS vals "
                "RETURN reduce(acc = 999, x IN vals | "
                "  CASE WHEN x < acc THEN x ELSE acc END) AS min_val"
            )
        )
        assert len(result) == 1
        assert result[0]["min_val"] == 1

    def test_reduce_with_case_conditional_sum(self, db):
        """reduce() with CASE WHEN should conditionally sum values."""
        result = list(
            db.execute_cypher(
                "WITH [1, 2, 3, 4, 5] AS vals "
                "RETURN reduce(acc = 0, x IN vals | "
                "  CASE WHEN x > 2 THEN acc + x ELSE acc END) AS big_sum"
            )
        )
        assert len(result) == 1
        # Only 3+4+5 = 12
        assert result[0]["big_sum"] == 12


# =============================================================================
# Bug 5: Target node property filter ignored in edge patterns (#155)
# =============================================================================


class TestTargetNodePropertyFilter:
    """Target node property filters like ()-[r]->(o {name: 'X'}) must filter results."""

    def test_outgoing_target_property_filter(self, db):
        """MATCH (a)-[r]->(b {name: 'Gus'}) should only return edges to Gus."""
        db.execute_cypher(
            "CREATE (a:Person {name: 'Alix'}), (b:Person {name: 'Gus'}), "
            "(c:Person {name: 'Vincent'})"
        )
        db.execute_cypher(
            "MATCH (a:Person {name: 'Alix'}), (b:Person {name: 'Gus'}) CREATE (a)-[:KNOWS]->(b)"
        )
        db.execute_cypher(
            "MATCH (a:Person {name: 'Alix'}), (c:Person {name: 'Vincent'}) CREATE (a)-[:KNOWS]->(c)"
        )

        result = list(db.execute_cypher("MATCH (a)-[r]->(b {name: 'Gus'}) RETURN a.name, b.name"))
        assert len(result) == 1
        assert result[0]["a.name"] == "Alix"
        assert result[0]["b.name"] == "Gus"

    def test_incoming_target_property_filter(self, db):
        """MATCH (a)-[r]->(b {name: 'Gus'}) from the target's perspective via incoming."""
        db.execute_cypher(
            "CREATE (a:Person {name: 'Alix'}), (b:Person {name: 'Gus'}), "
            "(c:Person {name: 'Vincent'})"
        )
        db.execute_cypher(
            "MATCH (a:Person {name: 'Alix'}), (b:Person {name: 'Gus'}) CREATE (a)-[:KNOWS]->(b)"
        )
        db.execute_cypher(
            "MATCH (a:Person {name: 'Alix'}), (c:Person {name: 'Vincent'}) CREATE (a)-[:KNOWS]->(c)"
        )

        result = list(db.execute_cypher("MATCH (b {name: 'Gus'})<-[r]-(a) RETURN a.name, b.name"))
        assert len(result) == 1
        assert result[0]["a.name"] == "Alix"
        assert result[0]["b.name"] == "Gus"

    def test_target_property_filter_count(self, db):
        """count(r) with target property filter must not return ALL edges."""
        db.execute_cypher(
            "CREATE (a:Person {name: 'Alix'}), (b:Person {name: 'Gus'}), "
            "(c:Person {name: 'Vincent'})"
        )
        db.execute_cypher(
            "MATCH (a:Person {name: 'Alix'}), (b:Person {name: 'Gus'}) CREATE (a)-[:KNOWS]->(b)"
        )
        db.execute_cypher(
            "MATCH (a:Person {name: 'Alix'}), (c:Person {name: 'Vincent'}) CREATE (a)-[:KNOWS]->(c)"
        )

        result = list(db.execute_cypher("MATCH ()-[r]->(o {name: 'Gus'}) RETURN count(r) AS cnt"))
        assert len(result) == 1
        assert result[0]["cnt"] == 1

    def test_target_property_filter_no_match(self, db):
        """Target property filter that matches nothing should return 0 rows."""
        db.execute_cypher("CREATE (:Person {name: 'Alix'})-[:KNOWS]->(:Person {name: 'Gus'})")

        result = list(
            db.execute_cypher("MATCH ()-[r]->(o {name: 'Nobody'}) RETURN count(r) AS cnt")
        )
        assert len(result) == 1
        assert result[0]["cnt"] == 0

    def test_edge_property_filter(self, db):
        """Edge property filter -[r {since: 2020}]-> should filter by edge properties."""
        db.execute_cypher(
            "CREATE (a:Person {name: 'Alix'}), (b:Person {name: 'Gus'}), "
            "(c:Person {name: 'Vincent'})"
        )
        db.execute_cypher(
            "MATCH (a:Person {name: 'Alix'}), (b:Person {name: 'Gus'}) "
            "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
        )
        db.execute_cypher(
            "MATCH (a:Person {name: 'Alix'}), (c:Person {name: 'Vincent'}) "
            "CREATE (a)-[:KNOWS {since: 2023}]->(c)"
        )

        result = list(db.execute_cypher("MATCH (a)-[r {since: 2020}]->(b) RETURN a.name, b.name"))
        assert len(result) == 1
        assert result[0]["a.name"] == "Alix"
        assert result[0]["b.name"] == "Gus"


# =============================================================================
# OPTIONAL MATCH + aggregation scope
# Inspired by Memgraph #3970
# =============================================================================


class TestOptionalMatchAggregation:
    """Outer variable must survive OPTIONAL MATCH + aggregation."""

    def test_count_preserves_all_rows(self, db):
        db.execute_cypher("CREATE (:Person {name: 'Alix'})-[:FRIEND]->(:Person {name: 'Gus'})")
        db.execute_cypher("CREATE (:Person {name: 'Vincent'})")
        result = list(
            db.execute_cypher(
                "MATCH (p:Person) "
                "OPTIONAL MATCH (p)-[:FRIEND]->(f:Person) "
                "RETURN p.name, count(f) AS fc "
                "ORDER BY p.name"
            )
        )
        assert len(result) == 3
        assert result[0]["p.name"] == "Alix"
        assert result[0]["fc"] == 1
        assert result[2]["p.name"] == "Vincent"
        assert result[2]["fc"] == 0


# =============================================================================
# UNION correctness
# Inspired by Memgraph #3909
# =============================================================================


class TestUnionCorrectness:
    """UNION and UNION ALL semantics."""

    def test_union_deduplicates(self, db):
        result = list(db.execute_cypher("RETURN 1 AS x UNION RETURN 1 AS x"))
        assert len(result) == 1

    def test_union_all_preserves(self, db):
        result = list(db.execute_cypher("RETURN 1 AS x UNION ALL RETURN 1 AS x"))
        assert len(result) == 2


# =============================================================================
# Variable-length path vs explicit hop
# Inspired by Memgraph #3735
# =============================================================================


class TestVarLengthVsExplicit:
    """Variable-length *2 must match explicit two-hop count."""

    def test_two_hop_equivalence(self, db):
        db.execute_cypher("CREATE (:N {id: 1})-[:R]->(:N {id: 2})-[:R]->(:N {id: 3})")
        explicit = list(
            db.execute_cypher("MATCH (:N {id: 1})-[:R]->(mid)-[:R]->(c) RETURN count(*) AS cnt")
        )
        varlength = list(db.execute_cypher("MATCH (:N {id: 1})-[:R*2]->(c) RETURN count(*) AS cnt"))
        assert explicit[0]["cnt"] == varlength[0]["cnt"]


# =============================================================================
# MERGE after DELETE
# Inspired by Memgraph #2093
# =============================================================================


class TestMergeAfterDelete:
    """MERGE must not match a deleted node."""

    def test_merge_creates_new_after_delete(self, db):
        db.execute_cypher("CREATE (:Singleton {key: 'only'})")
        db.execute_cypher("MATCH (n:Singleton) DELETE n")
        db.execute_cypher("MERGE (:Singleton {key: 'only'})")
        result = list(db.execute_cypher("MATCH (n:Singleton) RETURN count(n) AS cnt"))
        assert result[0]["cnt"] == 1


# =============================================================================
# Delete + re-create edge
# Inspired by Dgraph #9422
# =============================================================================


class TestDeleteRecreateEdge:
    """Replacing an edge in sequence."""

    def test_replace_edge(self, db):
        db.execute_cypher("CREATE (:Person {name: 'Alix'})-[:LIKES]->(:Fruit {name: 'apple'})")
        db.execute_cypher("CREATE (:Fruit {name: 'banana'})")
        db.execute_cypher("MATCH (:Person {name: 'Alix'})-[r:LIKES]->() DELETE r")
        db.execute_cypher(
            "MATCH (p:Person {name: 'Alix'}), (f:Fruit {name: 'banana'}) CREATE (p)-[:LIKES]->(f)"
        )
        result = list(
            db.execute_cypher("MATCH (:Person {name: 'Alix'})-[:LIKES]->(f:Fruit) RETURN f.name")
        )
        assert len(result) == 1
        assert result[0]["f.name"] == "banana"


# =============================================================================
# Relationship direction
# =============================================================================


class TestRelationshipDirection:
    """Arrow direction in Cypher patterns."""

    def test_backward_arrow(self, db):
        db.execute_cypher("CREATE (:A {name: 'Alix'})-[:FOLLOWS]->(:B {name: 'Gus'})")
        fwd = list(db.execute_cypher("MATCH (a:A)-[:FOLLOWS]->(b:B) RETURN a.name"))
        bwd = list(db.execute_cypher("MATCH (b:B)<-[:FOLLOWS]-(a:A) RETURN a.name"))
        wrong = list(db.execute_cypher("MATCH (a:A)<-[:FOLLOWS]-(b:B) RETURN a.name"))
        assert len(fwd) == 1
        assert len(bwd) == 1
        assert len(wrong) == 0


# =============================================================================
# NULL predicate semantics
# =============================================================================


class TestNullPredicateSemantics:
    """NULL comparisons in Cypher WHERE."""

    def test_null_equals_null_is_unknown(self, db):
        db.execute_cypher("CREATE (:N {val: 1})")
        result = list(db.execute_cypher("MATCH (n:N) WHERE null = null RETURN n"))
        assert len(result) == 0

    def test_null_is_null_is_true(self, db):
        db.execute_cypher("CREATE (:N {val: 1})")
        result = list(db.execute_cypher("MATCH (n:N) WHERE null IS NULL RETURN n.val"))
        assert len(result) == 1


# =============================================================================
# Property type overwrite
# Inspired by JanusGraph #4141
# =============================================================================


class TestPropertyTypeOverwrite:
    """SET can change property type."""

    def test_bool_to_string(self, db):
        db.execute_cypher("CREATE (:Item {flag: true})")
        db.execute_cypher("MATCH (n:Item) SET n.flag = 'yes'")
        result = list(db.execute_cypher("MATCH (n:Item) RETURN n.flag"))
        assert result[0]["n.flag"] == "yes"

    def test_int_to_string(self, db):
        db.execute_cypher("CREATE (:Item {val: 42})")
        db.execute_cypher("MATCH (n:Item) SET n.val = 'forty-two'")
        result = list(db.execute_cypher("MATCH (n:Item) RETURN n.val"))
        assert result[0]["n.val"] == "forty-two"


# =============================================================================
# Type coercion: string vs boolean
# Inspired by JanusGraph #4220
# =============================================================================


class TestTypeCoercionStringBool:
    """String 'false' != boolean false."""

    def test_string_false_ne_bool_false(self, db):
        db.execute_cypher("CREATE (:N {val: false})")
        db.execute_cypher("CREATE (:N {val: 'false'})")
        result = list(db.execute_cypher("MATCH (n:N) WHERE n.val <> 'false' RETURN n.val"))
        assert len(result) == 1
        assert result[0]["n.val"] is False


# =============================================================================
# Inequality on missing property
# Inspired by JanusGraph #2205
# =============================================================================


class TestInequalityMissingProperty:
    """<> must not match when property is missing."""

    def test_neq_excludes_null(self, db):
        db.execute_cypher("CREATE (:Person {name: 'Alix'})")
        db.execute_cypher("CREATE (:Person {name: 'Gus'})")
        db.execute_cypher("CREATE (:Person {age: 30})")
        result = list(db.execute_cypher("MATCH (n:Person) WHERE n.name <> 'Alix' RETURN n.name"))
        assert len(result) == 1
        assert result[0]["n.name"] == "Gus"


# =============================================================================
# SKIP + LIMIT
# =============================================================================


class TestSkipLimit:
    """Pagination in Cypher."""

    def test_skip_plus_limit(self, db):
        for i in range(10):
            db.execute_cypher(f"CREATE (:Item {{seq: {i}}})")
        result = list(
            db.execute_cypher("MATCH (n:Item) RETURN n.seq ORDER BY n.seq SKIP 3 LIMIT 2")
        )
        assert len(result) == 2
        assert result[0]["n.seq"] == 3
        assert result[1]["n.seq"] == 4


# =============================================================================
# RETURN DISTINCT
# =============================================================================


class TestReturnDistinct:
    """DISTINCT deduplication in Cypher."""

    def test_distinct_values(self, db):
        db.execute_cypher("CREATE (:N {val: 1})")
        db.execute_cypher("CREATE (:N {val: 1})")
        db.execute_cypher("CREATE (:N {val: 2})")
        result = list(db.execute_cypher("MATCH (n:N) RETURN DISTINCT n.val ORDER BY n.val"))
        assert len(result) == 2

    def test_distinct_collapses_nulls(self, db):
        db.execute_cypher("CREATE (:N {val: 1})")
        db.execute_cypher("CREATE (:N {val: 1})")
        db.execute_cypher("CREATE (:N)")
        db.execute_cypher("CREATE (:N)")
        result = list(db.execute_cypher("MATCH (n:N) RETURN DISTINCT n.val"))
        assert len(result) == 2
