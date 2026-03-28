# W3C SPARQL Test Suite Integration

This directory contains infrastructure for running the official W3C SPARQL
test suites against Grafeo's SPARQL implementation.

## Setup

The W3C RDF tests are available at https://github.com/w3c/rdf-tests.

To download them:

```bash
# Option 1: Git submodule (preferred)
git submodule add https://github.com/w3c/rdf-tests.git tests/spec/rdf/sparql/w3c/rdf-tests

# Option 2: Clone separately
git clone https://github.com/w3c/rdf-tests.git tests/spec/rdf/sparql/w3c/rdf-tests
```

## Converting W3C Manifests to .gtest Files

The conversion script reads W3C manifest.ttl files and generates .gtest files:

```bash
# Convert SPARQL 1.1 aggregates tests
python scripts/convert_w3c_sparql.py \
  --manifest tests/spec/rdf/sparql/w3c/rdf-tests/sparql/sparql11/data-sparql11/aggregates/manifest.ttl \
  --output tests/spec/rdf/sparql/w3c/aggregates.gtest

# Convert all SPARQL 1.1 tests
python scripts/convert_w3c_sparql.py --all
```

## Tracking Known Failures

`known_failures.toml` tracks tests that are expected to fail, with categorized reasons:

- `unimplemented`: Feature not yet supported
- `spec-disagreement`: Intentional deviation from spec
- `known-bug`: Known issue, tracked in GitHub

## Conformance Scorecard

Run `python scripts/sparql_conformance.py` to generate a compliance report
from the known_failures.toml file.
