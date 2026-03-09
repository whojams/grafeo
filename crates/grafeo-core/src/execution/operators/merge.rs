//! Merge operator for MERGE clause execution.
//!
//! The MERGE operator implements the Cypher MERGE semantics:
//! 1. Try to match the pattern in the graph
//! 2. If found, return existing element (optionally apply ON MATCH SET)
//! 3. If not found, create the element (optionally apply ON CREATE SET)

use super::{Operator, OperatorResult};
use crate::execution::chunk::DataChunkBuilder;
use crate::graph::GraphStoreMut;
use grafeo_common::types::{
    EdgeId, EpochId, LogicalType, NodeId, PropertyKey, TransactionId, Value,
};
use std::sync::Arc;

/// Configuration for a node merge operation.
pub struct MergeConfig {
    /// Variable name for the merged node.
    pub variable: String,
    /// Labels to match/create.
    pub labels: Vec<String>,
    /// Properties that must match (also used for creation).
    pub match_properties: Vec<(String, Value)>,
    /// Properties to set on CREATE.
    pub on_create_properties: Vec<(String, Value)>,
    /// Properties to set on MATCH.
    pub on_match_properties: Vec<(String, Value)>,
    /// Output schema (input columns + node column).
    pub output_schema: Vec<LogicalType>,
    /// Column index where the merged node ID is placed.
    pub output_column: usize,
}

/// Merge operator for MERGE clause.
///
/// Tries to match a node with the given labels and properties.
/// If found, returns the existing node. If not found, creates a new node.
///
/// When an input operator is provided (chained MERGE), input rows are
/// passed through with the merged node ID appended as an additional column.
pub struct MergeOperator {
    /// The graph store.
    store: Arc<dyn GraphStoreMut>,
    /// Optional input operator (for chained MERGE patterns).
    input: Option<Box<dyn Operator>>,
    /// Merge configuration.
    config: MergeConfig,
    /// Whether we've already executed (standalone mode only).
    executed: bool,
    /// Epoch for MVCC versioning.
    viewing_epoch: Option<EpochId>,
    /// Transaction ID for undo log tracking.
    transaction_id: Option<TransactionId>,
}

impl MergeOperator {
    /// Creates a new merge operator.
    pub fn new(
        store: Arc<dyn GraphStoreMut>,
        input: Option<Box<dyn Operator>>,
        config: MergeConfig,
    ) -> Self {
        Self {
            store,
            input,
            config,
            executed: false,
            viewing_epoch: None,
            transaction_id: None,
        }
    }

    /// Returns the variable name for the merged node.
    #[must_use]
    pub fn variable(&self) -> &str {
        &self.config.variable
    }

    /// Sets the transaction context for versioned mutations.
    pub fn with_transaction_context(
        mut self,
        epoch: EpochId,
        transaction_id: Option<TransactionId>,
    ) -> Self {
        self.viewing_epoch = Some(epoch);
        self.transaction_id = transaction_id;
        self
    }

    /// Tries to find a matching node.
    fn find_matching_node(&self) -> Option<NodeId> {
        let candidates: Vec<NodeId> = if let Some(first_label) = self.config.labels.first() {
            self.store.nodes_by_label(first_label)
        } else {
            self.store.node_ids()
        };

        for node_id in candidates {
            if let Some(node) = self.store.get_node(node_id) {
                let has_all_labels = self.config.labels.iter().all(|label| node.has_label(label));
                if !has_all_labels {
                    continue;
                }

                let has_all_props =
                    self.config
                        .match_properties
                        .iter()
                        .all(|(key, expected_value)| {
                            node.properties
                                .get(&PropertyKey::new(key.as_str()))
                                .is_some_and(|v| v == expected_value)
                        });

                if has_all_props {
                    return Some(node_id);
                }
            }
        }

        None
    }

    /// Creates a new node with the specified labels and properties.
    fn create_node(&self) -> NodeId {
        let mut all_props: Vec<(PropertyKey, Value)> = self
            .config
            .match_properties
            .iter()
            .map(|(k, v)| (PropertyKey::new(k.as_str()), v.clone()))
            .collect();

        for (k, v) in &self.config.on_create_properties {
            if let Some(existing) = all_props.iter_mut().find(|(key, _)| key.as_str() == k) {
                existing.1 = v.clone();
            } else {
                all_props.push((PropertyKey::new(k.as_str()), v.clone()));
            }
        }

        let labels: Vec<&str> = self.config.labels.iter().map(String::as_str).collect();
        self.store.create_node_with_props(&labels, &all_props)
    }

    /// Finds or creates a matching node, applying ON MATCH/ON CREATE as appropriate.
    fn merge_node(&self) -> NodeId {
        if let Some(existing_id) = self.find_matching_node() {
            self.apply_on_match(existing_id);
            existing_id
        } else {
            self.create_node()
        }
    }

    /// Applies ON MATCH properties to an existing node.
    fn apply_on_match(&self, node_id: NodeId) {
        for (key, value) in &self.config.on_match_properties {
            if let Some(tid) = self.transaction_id {
                self.store
                    .set_node_property_versioned(node_id, key.as_str(), value.clone(), tid);
            } else {
                self.store
                    .set_node_property(node_id, key.as_str(), value.clone());
            }
        }
    }
}

impl Operator for MergeOperator {
    fn next(&mut self) -> OperatorResult {
        // When we have an input operator, pass through input rows with the
        // merged node ID appended (used for chained inline MERGE patterns).
        if let Some(ref mut input) = self.input {
            if let Some(chunk) = input.next()? {
                // Merge the node (once, same node for all input rows)
                let node_id = self.merge_node();

                let mut builder =
                    DataChunkBuilder::with_capacity(&self.config.output_schema, chunk.row_count());

                for row in chunk.selected_indices() {
                    // Copy input columns to output
                    for col_idx in 0..chunk.column_count() {
                        if let (Some(src), Some(dst)) =
                            (chunk.column(col_idx), builder.column_mut(col_idx))
                        {
                            if let Some(val) = src.get_value(row) {
                                dst.push_value(val);
                            } else {
                                dst.push_value(Value::Null);
                            }
                        }
                    }

                    // Append the merged node ID
                    if let Some(dst) = builder.column_mut(self.config.output_column) {
                        dst.push_node_id(node_id);
                    }

                    builder.advance_row();
                }

                return Ok(Some(builder.finish()));
            }
            return Ok(None);
        }

        // Standalone mode (no input operator)
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        let node_id = self.merge_node();

        let mut builder = DataChunkBuilder::new(&self.config.output_schema);
        if let Some(dst) = builder.column_mut(self.config.output_column) {
            dst.push_node_id(node_id);
        }
        builder.advance_row();

        Ok(Some(builder.finish()))
    }

    fn reset(&mut self) {
        self.executed = false;
        if let Some(ref mut input) = self.input {
            input.reset();
        }
    }

    fn name(&self) -> &'static str {
        "Merge"
    }
}

/// Configuration for a relationship merge operation.
pub struct MergeRelationshipConfig {
    /// Column index for the source node ID in the input.
    pub source_column: usize,
    /// Column index for the target node ID in the input.
    pub target_column: usize,
    /// Relationship type to match/create.
    pub edge_type: String,
    /// Properties that must match (also used for creation).
    pub match_properties: Vec<(String, Value)>,
    /// Properties to set on CREATE.
    pub on_create_properties: Vec<(String, Value)>,
    /// Properties to set on MATCH.
    pub on_match_properties: Vec<(String, Value)>,
    /// Output schema (input columns + edge column).
    pub output_schema: Vec<LogicalType>,
    /// Column index for the edge variable in the output.
    pub edge_output_column: usize,
}

/// Merge operator for relationship patterns.
///
/// Takes input rows containing source and target node IDs, then for each row:
/// 1. Searches for an existing relationship matching the type and properties
/// 2. If found, applies ON MATCH properties and returns the existing edge
/// 3. If not found, creates a new relationship and applies ON CREATE properties
pub struct MergeRelationshipOperator {
    /// The graph store.
    store: Arc<dyn GraphStoreMut>,
    /// Input operator providing rows with source/target node columns.
    input: Box<dyn Operator>,
    /// Merge configuration.
    config: MergeRelationshipConfig,
    /// Epoch for MVCC versioning.
    viewing_epoch: Option<EpochId>,
    /// Transaction ID for undo log tracking.
    transaction_id: Option<TransactionId>,
}

impl MergeRelationshipOperator {
    /// Creates a new merge relationship operator.
    pub fn new(
        store: Arc<dyn GraphStoreMut>,
        input: Box<dyn Operator>,
        config: MergeRelationshipConfig,
    ) -> Self {
        Self {
            store,
            input,
            config,
            viewing_epoch: None,
            transaction_id: None,
        }
    }

    /// Sets the transaction context for versioned mutations.
    pub fn with_transaction_context(
        mut self,
        epoch: EpochId,
        transaction_id: Option<TransactionId>,
    ) -> Self {
        self.viewing_epoch = Some(epoch);
        self.transaction_id = transaction_id;
        self
    }

    /// Tries to find a matching relationship between source and target.
    fn find_matching_edge(&self, src: NodeId, dst: NodeId) -> Option<EdgeId> {
        use crate::graph::Direction;

        for (target, edge_id) in self.store.edges_from(src, Direction::Outgoing) {
            if target != dst {
                continue;
            }

            if let Some(edge) = self.store.get_edge(edge_id) {
                if edge.edge_type.as_str() != self.config.edge_type {
                    continue;
                }

                let has_all_props =
                    self.config.match_properties.iter().all(|(key, expected)| {
                        edge.get_property(key).is_some_and(|v| v == expected)
                    });

                if has_all_props {
                    return Some(edge_id);
                }
            }
        }

        None
    }

    /// Creates a new edge with the match properties and on_create properties.
    fn create_edge(&self, src: NodeId, dst: NodeId) -> EdgeId {
        let mut all_props: Vec<(PropertyKey, Value)> = self
            .config
            .match_properties
            .iter()
            .map(|(k, v)| (PropertyKey::new(k.as_str()), v.clone()))
            .collect();

        for (k, v) in &self.config.on_create_properties {
            if let Some(existing) = all_props.iter_mut().find(|(key, _)| key.as_str() == k) {
                existing.1 = v.clone();
            } else {
                all_props.push((PropertyKey::new(k.as_str()), v.clone()));
            }
        }

        self.store
            .create_edge_with_props(src, dst, &self.config.edge_type, &all_props)
    }

    /// Applies ON MATCH properties to an existing edge.
    fn apply_on_match(&self, edge_id: EdgeId) {
        for (key, value) in &self.config.on_match_properties {
            if let Some(tid) = self.transaction_id {
                self.store
                    .set_edge_property_versioned(edge_id, key.as_str(), value.clone(), tid);
            } else {
                self.store
                    .set_edge_property(edge_id, key.as_str(), value.clone());
            }
        }
    }
}

impl Operator for MergeRelationshipOperator {
    fn next(&mut self) -> OperatorResult {
        use super::OperatorError;

        if let Some(chunk) = self.input.next()? {
            let mut builder =
                DataChunkBuilder::with_capacity(&self.config.output_schema, chunk.row_count());

            for row in chunk.selected_indices() {
                let src_val = chunk
                    .column(self.config.source_column)
                    .and_then(|c| c.get_node_id(row))
                    .ok_or_else(|| OperatorError::TypeMismatch {
                        expected: "NodeId (source)".to_string(),
                        found: "None".to_string(),
                    })?;

                let dst_val = chunk
                    .column(self.config.target_column)
                    .and_then(|c| c.get_node_id(row))
                    .ok_or_else(|| OperatorError::TypeMismatch {
                        expected: "NodeId (target)".to_string(),
                        found: "None".to_string(),
                    })?;

                let edge_id = if let Some(existing) = self.find_matching_edge(src_val, dst_val) {
                    self.apply_on_match(existing);
                    existing
                } else {
                    self.create_edge(src_val, dst_val)
                };

                // Copy input columns to output, then add the edge column
                for col_idx in 0..self.config.output_schema.len() {
                    if col_idx == self.config.edge_output_column {
                        if let Some(dst_col) = builder.column_mut(col_idx) {
                            dst_col.push_edge_id(edge_id);
                        }
                    } else if let (Some(src_col), Some(dst_col)) =
                        (chunk.column(col_idx), builder.column_mut(col_idx))
                        && let Some(val) = src_col.get_value(row)
                    {
                        dst_col.push_value(val);
                    }
                }

                builder.advance_row();
            }

            return Ok(Some(builder.finish()));
        }

        Ok(None)
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn name(&self) -> &'static str {
        "MergeRelationship"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::lpg::LpgStore;

    #[test]
    fn test_merge_creates_new_node() {
        let store: Arc<dyn GraphStoreMut> = Arc::new(LpgStore::new().unwrap());

        // MERGE should create a new node since none exists
        let mut merge = MergeOperator::new(
            Arc::clone(&store),
            None,
            MergeConfig {
                variable: "n".to_string(),
                labels: vec!["Person".to_string()],
                match_properties: vec![("name".to_string(), Value::String("Alix".into()))],
                on_create_properties: vec![],
                on_match_properties: vec![],
                output_schema: vec![LogicalType::Node],
                output_column: 0,
            },
        );

        let result = merge.next().unwrap();
        assert!(result.is_some());

        // Verify node was created
        let nodes = store.nodes_by_label("Person");
        assert_eq!(nodes.len(), 1);

        let node = store.get_node(nodes[0]).unwrap();
        assert!(node.has_label("Person"));
        assert_eq!(
            node.properties.get(&PropertyKey::new("name")),
            Some(&Value::String("Alix".into()))
        );
    }

    #[test]
    fn test_merge_matches_existing_node() {
        let store: Arc<dyn GraphStoreMut> = Arc::new(LpgStore::new().unwrap());

        // Create an existing node
        store.create_node_with_props(
            &["Person"],
            &[(PropertyKey::new("name"), Value::String("Gus".into()))],
        );

        // MERGE should find the existing node
        let mut merge = MergeOperator::new(
            Arc::clone(&store),
            None,
            MergeConfig {
                variable: "n".to_string(),
                labels: vec!["Person".to_string()],
                match_properties: vec![("name".to_string(), Value::String("Gus".into()))],
                on_create_properties: vec![],
                on_match_properties: vec![],
                output_schema: vec![LogicalType::Node],
                output_column: 0,
            },
        );

        let result = merge.next().unwrap();
        assert!(result.is_some());

        // Verify only one node exists (no new node created)
        let nodes = store.nodes_by_label("Person");
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn test_merge_with_on_create() {
        let store: Arc<dyn GraphStoreMut> = Arc::new(LpgStore::new().unwrap());

        // MERGE with ON CREATE SET
        let mut merge = MergeOperator::new(
            Arc::clone(&store),
            None,
            MergeConfig {
                variable: "n".to_string(),
                labels: vec!["Person".to_string()],
                match_properties: vec![("name".to_string(), Value::String("Vincent".into()))],
                on_create_properties: vec![("created".to_string(), Value::Bool(true))],
                on_match_properties: vec![],
                output_schema: vec![LogicalType::Node],
                output_column: 0,
            },
        );

        let _ = merge.next().unwrap();

        // Verify node has both match properties and on_create properties
        let nodes = store.nodes_by_label("Person");
        let node = store.get_node(nodes[0]).unwrap();
        assert_eq!(
            node.properties.get(&PropertyKey::new("name")),
            Some(&Value::String("Vincent".into()))
        );
        assert_eq!(
            node.properties.get(&PropertyKey::new("created")),
            Some(&Value::Bool(true))
        );
    }

    #[test]
    fn test_merge_with_on_match() {
        let store: Arc<dyn GraphStoreMut> = Arc::new(LpgStore::new().unwrap());

        // Create an existing node
        let node_id = store.create_node_with_props(
            &["Person"],
            &[(PropertyKey::new("name"), Value::String("Jules".into()))],
        );

        // MERGE with ON MATCH SET
        let mut merge = MergeOperator::new(
            Arc::clone(&store),
            None,
            MergeConfig {
                variable: "n".to_string(),
                labels: vec!["Person".to_string()],
                match_properties: vec![("name".to_string(), Value::String("Jules".into()))],
                on_create_properties: vec![],
                on_match_properties: vec![("updated".to_string(), Value::Bool(true))],
                output_schema: vec![LogicalType::Node],
                output_column: 0,
            },
        );

        let _ = merge.next().unwrap();

        // Verify node has the on_match property added
        let node = store.get_node(node_id).unwrap();
        assert_eq!(
            node.properties.get(&PropertyKey::new("updated")),
            Some(&Value::Bool(true))
        );
    }
}
