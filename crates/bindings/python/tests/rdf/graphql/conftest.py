"""GraphQL on RDF pytest fixtures and configuration."""

import pytest

# Try to import grafeo
try:
    from grafeo import GrafeoDB

    GRAFEO_AVAILABLE = True
except ImportError:
    GRAFEO_AVAILABLE = False


@pytest.fixture
def db():
    """Create a fresh in-memory GrafeoDB instance."""
    if not GRAFEO_AVAILABLE:
        pytest.skip("grafeo not installed")
    return GrafeoDB()


@pytest.fixture
def rdf_graphql_db(db):
    """Create a database with RDF data for GraphQL queries."""
    # Create resources with URIs
    alix = db.create_node(
        ["Resource", "Person"],
        {"uri": "http://example.org/person/alix", "name": "Alix", "age": 30},
    )

    gus = db.create_node(
        ["Resource", "Person"],
        {"uri": "http://example.org/person/gus", "name": "Gus", "age": 25},
    )

    # Create knows relationship
    db.create_edge(alix.id, gus.id, "knows", {})

    return db
