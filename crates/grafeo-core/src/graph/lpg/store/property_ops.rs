//! Property operations for the LPG store.

use super::LpgStore;
use super::PropertyUndoEntry;
use grafeo_common::types::{EdgeId, NodeId, PropertyKey, TransactionId, Value};
use grafeo_common::utils::hash::FxHashMap;

impl LpgStore {
    /// Sets a property on a node.
    #[cfg(not(feature = "tiered-storage"))]
    pub fn set_node_property(&self, id: NodeId, key: &str, value: Value) {
        let prop_key: PropertyKey = key.into();

        // Update property index before setting the property (needs to read old value)
        self.update_property_index_on_set(id, &prop_key, &value);

        // Sync text index if applicable
        #[cfg(feature = "text-index")]
        self.update_text_index_on_set(id, key, &value);

        self.node_properties.set(id, prop_key, value);

        // Update props_count in record
        let count = self.node_properties.get_all(id).len() as u16;
        if let Some(chain) = self.nodes.write().get_mut(&id)
            && let Some(record) = chain.latest_mut()
        {
            record.props_count = count;
        }
    }

    /// Sets a property on a node.
    /// (Tiered storage version: properties stored separately, record is immutable)
    #[cfg(feature = "tiered-storage")]
    pub fn set_node_property(&self, id: NodeId, key: &str, value: Value) {
        let prop_key: PropertyKey = key.into();

        // Update property index before setting the property (needs to read old value)
        self.update_property_index_on_set(id, &prop_key, &value);

        // Sync text index if applicable
        #[cfg(feature = "text-index")]
        self.update_text_index_on_set(id, key, &value);

        self.node_properties.set(id, prop_key, value);
        // Note: props_count in record is not updated for tiered storage.
        // The record is immutable once allocated in the arena.
        // Property count can be derived from PropertyStorage if needed.
    }

    /// Sets a property on an edge.
    pub fn set_edge_property(&self, id: EdgeId, key: &str, value: Value) {
        self.edge_properties.set(id, key.into(), value);
    }

    /// Removes a property from a node.
    ///
    /// Returns the previous value if it existed, or None if the property didn't exist.
    #[cfg(not(feature = "tiered-storage"))]
    pub fn remove_node_property(&self, id: NodeId, key: &str) -> Option<Value> {
        let prop_key: PropertyKey = key.into();

        // Update property index before removing (needs to read old value)
        self.update_property_index_on_remove(id, &prop_key);

        // Sync text index if applicable
        #[cfg(feature = "text-index")]
        self.update_text_index_on_remove(id, key);

        let result = self.node_properties.remove(id, &prop_key);

        // Update props_count in record
        let count = self.node_properties.get_all(id).len() as u16;
        if let Some(chain) = self.nodes.write().get_mut(&id)
            && let Some(record) = chain.latest_mut()
        {
            record.props_count = count;
        }

        result
    }

    /// Removes a property from a node.
    /// (Tiered storage version)
    #[cfg(feature = "tiered-storage")]
    pub fn remove_node_property(&self, id: NodeId, key: &str) -> Option<Value> {
        let prop_key: PropertyKey = key.into();

        // Update property index before removing (needs to read old value)
        self.update_property_index_on_remove(id, &prop_key);

        // Sync text index if applicable
        #[cfg(feature = "text-index")]
        self.update_text_index_on_remove(id, key);

        self.node_properties.remove(id, &prop_key)
        // Note: props_count in record is not updated for tiered storage.
    }

    /// Removes a property from an edge.
    ///
    /// Returns the previous value if it existed, or None if the property didn't exist.
    pub fn remove_edge_property(&self, id: EdgeId, key: &str) -> Option<Value> {
        self.edge_properties.remove(id, &key.into())
    }

    /// Gets a single property from a node without loading all properties.
    ///
    /// This is O(1) vs O(properties) for `get_node().get_property()`.
    /// Use this for filter predicates where you only need one property value.
    ///
    /// # Example
    ///
    /// ```
    /// # use grafeo_core::graph::lpg::LpgStore;
    /// # use grafeo_common::types::{PropertyKey, Value};
    /// let store = LpgStore::new().expect("arena allocation");
    /// let node_id = store.create_node(&["Person"]);
    /// store.set_node_property(node_id, "age", Value::from(30i64));
    ///
    /// // Fast: Direct single-property lookup
    /// let age = store.get_node_property(node_id, &PropertyKey::new("age"));
    ///
    /// // Slow: Loads all properties, then extracts one
    /// let age = store.get_node(node_id).and_then(|n| n.get_property("age").cloned());
    /// ```
    #[must_use]
    pub fn get_node_property(&self, id: NodeId, key: &PropertyKey) -> Option<Value> {
        self.node_properties.get(id, key)
    }

    /// Gets a single property from an edge without loading all properties.
    ///
    /// This is O(1) vs O(properties) for `get_edge().get_property()`.
    #[must_use]
    pub fn get_edge_property(&self, id: EdgeId, key: &PropertyKey) -> Option<Value> {
        self.edge_properties.get(id, key)
    }

    // === Batch Property Operations ===

    /// Gets a property for multiple nodes in a single batch operation.
    ///
    /// More efficient than calling [`Self::get_node_property`] in a loop because it
    /// reduces lock overhead and enables better cache utilization.
    ///
    /// # Example
    ///
    /// ```
    /// use grafeo_core::graph::lpg::LpgStore;
    /// use grafeo_common::types::{NodeId, PropertyKey, Value};
    ///
    /// let store = LpgStore::new().expect("arena allocation");
    /// let n1 = store.create_node(&["Person"]);
    /// let n2 = store.create_node(&["Person"]);
    /// store.set_node_property(n1, "age", Value::from(25i64));
    /// store.set_node_property(n2, "age", Value::from(30i64));
    ///
    /// let ages = store.get_node_property_batch(&[n1, n2], &PropertyKey::new("age"));
    /// assert_eq!(ages, vec![Some(Value::from(25i64)), Some(Value::from(30i64))]);
    /// ```
    #[must_use]
    pub fn get_node_property_batch(&self, ids: &[NodeId], key: &PropertyKey) -> Vec<Option<Value>> {
        self.node_properties.get_batch(ids, key)
    }

    /// Gets all properties for multiple nodes in a single batch operation.
    ///
    /// Returns a vector of property maps, one per node ID (empty map if no properties).
    /// More efficient than calling [`Self::get_node`] in a loop.
    #[must_use]
    pub fn get_nodes_properties_batch(&self, ids: &[NodeId]) -> Vec<FxHashMap<PropertyKey, Value>> {
        self.node_properties.get_all_batch(ids)
    }

    /// Gets selected properties for multiple nodes (projection pushdown).
    ///
    /// This is more efficient than [`Self::get_nodes_properties_batch`] when you only
    /// need a subset of properties. It only iterates the requested columns instead of
    /// all columns.
    ///
    /// **Use this for**: Queries with explicit projections like `RETURN n.name, n.age`
    /// instead of `RETURN n` (which requires all properties).
    ///
    /// # Example
    ///
    /// ```
    /// use grafeo_core::graph::lpg::LpgStore;
    /// use grafeo_common::types::{PropertyKey, Value};
    ///
    /// let store = LpgStore::new().expect("arena allocation");
    /// let n1 = store.create_node(&["Person"]);
    /// store.set_node_property(n1, "name", Value::from("Alix"));
    /// store.set_node_property(n1, "age", Value::from(30i64));
    /// store.set_node_property(n1, "email", Value::from("alix@example.com"));
    ///
    /// // Only fetch name and age (faster than get_nodes_properties_batch)
    /// let keys = vec![PropertyKey::new("name"), PropertyKey::new("age")];
    /// let props = store.get_nodes_properties_selective_batch(&[n1], &keys);
    ///
    /// assert_eq!(props[0].len(), 2); // Only name and age, not email
    /// ```
    #[must_use]
    pub fn get_nodes_properties_selective_batch(
        &self,
        ids: &[NodeId],
        keys: &[PropertyKey],
    ) -> Vec<FxHashMap<PropertyKey, Value>> {
        self.node_properties.get_selective_batch(ids, keys)
    }

    /// Gets selected properties for multiple edges (projection pushdown).
    ///
    /// Edge-property version of [`Self::get_nodes_properties_selective_batch`].
    #[must_use]
    pub fn get_edges_properties_selective_batch(
        &self,
        ids: &[EdgeId],
        keys: &[PropertyKey],
    ) -> Vec<FxHashMap<PropertyKey, Value>> {
        self.edge_properties.get_selective_batch(ids, keys)
    }

    // === Versioned Property Operations (with undo log) ===

    /// Sets a node property within a transaction, recording the previous value
    /// in the undo log so it can be restored on rollback.
    pub fn set_node_property_versioned(
        &self,
        id: NodeId,
        key: &str,
        value: Value,
        transaction_id: TransactionId,
    ) {
        let prop_key: PropertyKey = key.into();

        // Capture the current value before overwriting
        let old_value = self.node_properties.get(id, &prop_key);

        // Record in undo log
        self.property_undo_log
            .write()
            .entry(transaction_id)
            .or_default()
            .push(PropertyUndoEntry::NodeProperty {
                node_id: id,
                key: prop_key,
                old_value,
            });

        // Delegate to the normal (unversioned) set
        self.set_node_property(id, key, value);
    }

    /// Sets an edge property within a transaction, recording the previous value
    /// in the undo log so it can be restored on rollback.
    pub fn set_edge_property_versioned(
        &self,
        id: EdgeId,
        key: &str,
        value: Value,
        transaction_id: TransactionId,
    ) {
        let prop_key: PropertyKey = key.into();

        // Capture the current value before overwriting
        let old_value = self.edge_properties.get(id, &prop_key);

        // Record in undo log
        self.property_undo_log
            .write()
            .entry(transaction_id)
            .or_default()
            .push(PropertyUndoEntry::EdgeProperty {
                edge_id: id,
                key: prop_key,
                old_value,
            });

        // Delegate to the normal (unversioned) set
        self.set_edge_property(id, key, value);
    }

    /// Removes a node property within a transaction, recording the previous value
    /// in the undo log so it can be restored on rollback.
    pub fn remove_node_property_versioned(
        &self,
        id: NodeId,
        key: &str,
        transaction_id: TransactionId,
    ) -> Option<Value> {
        let prop_key: PropertyKey = key.into();

        // Capture the current value before removing
        let old_value = self.node_properties.get(id, &prop_key);

        // Only record if the property actually exists
        if old_value.is_some() {
            self.property_undo_log
                .write()
                .entry(transaction_id)
                .or_default()
                .push(PropertyUndoEntry::NodeProperty {
                    node_id: id,
                    key: prop_key,
                    old_value: old_value.clone(),
                });
        }

        // Delegate to the normal (unversioned) remove
        self.remove_node_property(id, key)
    }

    /// Removes an edge property within a transaction, recording the previous value
    /// in the undo log so it can be restored on rollback.
    pub fn remove_edge_property_versioned(
        &self,
        id: EdgeId,
        key: &str,
        transaction_id: TransactionId,
    ) -> Option<Value> {
        let prop_key: PropertyKey = key.into();

        // Capture the current value before removing
        let old_value = self.edge_properties.get(id, &prop_key);

        // Only record if the property actually exists
        if old_value.is_some() {
            self.property_undo_log
                .write()
                .entry(transaction_id)
                .or_default()
                .push(PropertyUndoEntry::EdgeProperty {
                    edge_id: id,
                    key: prop_key,
                    old_value: old_value.clone(),
                });
        }

        // Delegate to the normal (unversioned) remove
        self.remove_edge_property(id, key)
    }

    /// Replays the undo log for a transaction in reverse order, restoring
    /// all property values to their pre-transaction state.
    ///
    /// Called during rollback.
    pub fn rollback_transaction_properties(&self, transaction_id: TransactionId) {
        let entries = self.property_undo_log.write().remove(&transaction_id);
        if let Some(entries) = entries {
            // Replay in reverse order: latest change first
            for entry in entries.into_iter().rev() {
                match entry {
                    PropertyUndoEntry::NodeProperty {
                        node_id,
                        key,
                        old_value,
                    } => {
                        if let Some(value) = old_value {
                            // Restore the old value (bypass undo log, write directly)
                            self.set_node_property(node_id, key.as_str(), value);
                        } else {
                            // Property did not exist before: remove it
                            self.remove_node_property(node_id, key.as_str());
                        }
                    }
                    PropertyUndoEntry::EdgeProperty {
                        edge_id,
                        key,
                        old_value,
                    } => {
                        if let Some(value) = old_value {
                            self.set_edge_property(edge_id, key.as_str(), value);
                        } else {
                            self.remove_edge_property(edge_id, key.as_str());
                        }
                    }
                    PropertyUndoEntry::LabelAdded { node_id, label } => {
                        // Label was added during the transaction: remove it
                        self.remove_label(node_id, &label);
                    }
                    PropertyUndoEntry::LabelRemoved { node_id, label } => {
                        // Label was removed during the transaction: add it back
                        self.add_label(node_id, &label);
                    }
                }
            }
        }
    }

    /// Discards the undo log entries for a committed transaction.
    ///
    /// Called during commit: properties are already written, so just
    /// clean up the log.
    pub fn commit_transaction_properties(&self, transaction_id: TransactionId) {
        self.property_undo_log.write().remove(&transaction_id);
    }

    /// Returns the current number of undo log entries for a transaction.
    ///
    /// Used by savepoints to record the position so that partial rollback
    /// can replay only entries added after the savepoint.
    #[must_use]
    pub fn property_undo_log_position(&self, transaction_id: TransactionId) -> usize {
        self.property_undo_log
            .read()
            .get(&transaction_id)
            .map_or(0, Vec::len)
    }

    /// Rolls back property mutations recorded after position `since` in the undo log.
    ///
    /// Replays entries from `since..end` in reverse order, then truncates the
    /// log to `since`. Used by savepoint rollback.
    pub fn rollback_transaction_properties_to(&self, transaction_id: TransactionId, since: usize) {
        let mut log = self.property_undo_log.write();
        if let Some(entries) = log.get_mut(&transaction_id)
            && since < entries.len()
        {
            // Take entries after the savepoint position
            let to_undo: Vec<PropertyUndoEntry> = entries.drain(since..).collect();
            // Drop the lock before replaying to avoid deadlock
            // (rollback methods need to acquire other locks)
            drop(log);
            // Replay in reverse order
            for entry in to_undo.into_iter().rev() {
                match entry {
                    PropertyUndoEntry::NodeProperty {
                        node_id,
                        key,
                        old_value,
                    } => {
                        if let Some(value) = old_value {
                            self.set_node_property(node_id, key.as_str(), value);
                        } else {
                            self.remove_node_property(node_id, key.as_str());
                        }
                    }
                    PropertyUndoEntry::EdgeProperty {
                        edge_id,
                        key,
                        old_value,
                    } => {
                        if let Some(value) = old_value {
                            self.set_edge_property(edge_id, key.as_str(), value);
                        } else {
                            self.remove_edge_property(edge_id, key.as_str());
                        }
                    }
                    PropertyUndoEntry::LabelAdded { node_id, label } => {
                        self.remove_label(node_id, &label);
                    }
                    PropertyUndoEntry::LabelRemoved { node_id, label } => {
                        self.add_label(node_id, &label);
                    }
                }
            }
        }
    }
}
