"""GQL implementation of advanced query tests."""

import pytest

from tests.bases.test_advanced_queries import BaseAdvancedQueriesTest

# Try to import grafeo
try:
    from grafeo import GrafeoDB

    GRAFEO_AVAILABLE = True
except ImportError:
    GRAFEO_AVAILABLE = False


@pytest.fixture
def db():
    if not GRAFEO_AVAILABLE:
        pytest.skip("grafeo not installed")
    return GrafeoDB()


class TestGQLAdvancedQueries(BaseAdvancedQueriesTest):
    def setup_social_graph(self, db):
        alix = db.create_node(["Person"], {"name": "Alix", "age": 30, "city": "NYC"})
        gus = db.create_node(["Person"], {"name": "Gus", "age": 25, "city": "LA"})
        harm = db.create_node(["Person"], {"name": "Harm", "age": 35, "city": "London"})
        db.create_edge(alix.id, gus.id, "KNOWS")
        db.create_edge(alix.id, harm.id, "KNOWS")
        db.create_edge(gus.id, harm.id, "KNOWS")
