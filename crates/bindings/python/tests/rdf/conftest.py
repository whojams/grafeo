"""RDF model pytest fixtures and configuration."""

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
def rdf_db(db):
    """Create a database with RDF-like test data."""
    # Create resources with URIs
    alix = db.create_node(
        ["Resource"],
        {
            "uri": "http://example.org/person/alix",
            "rdf:type": "http://xmlns.com/foaf/0.1/Person",
            "foaf:name": "Alix",
            "foaf:age": 30,
        },
    )

    gus = db.create_node(
        ["Resource"],
        {
            "uri": "http://example.org/person/gus",
            "rdf:type": "http://xmlns.com/foaf/0.1/Person",
            "foaf:name": "Gus",
            "foaf:age": 25,
        },
    )

    # Create foaf:knows relationship
    db.create_edge(alix.id, gus.id, "foaf:knows", {})

    return db
