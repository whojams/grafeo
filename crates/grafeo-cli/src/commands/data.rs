//! Data export/import commands.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};

use anyhow::{Context, Result};

use grafeo_common::types::Value;

use crate::output;
use crate::{DataCommands, OutputFormat};

/// Run data commands.
pub fn run(cmd: DataCommands, _format: OutputFormat, quiet: bool) -> Result<()> {
    match cmd {
        DataCommands::Dump {
            path,
            output: out,
            export_format,
        } => {
            let format_name = export_format.as_deref().unwrap_or("json");
            if format_name != "json" && format_name != "jsonl" {
                anyhow::bail!(
                    "Only JSON Lines format is currently supported.\n\
                     Use `--export-format json` or omit for the default."
                );
            }

            let db = super::open_existing(&path)?;
            let file = std::fs::File::create(&out)
                .with_context(|| format!("Failed to create output file: {}", out.display()))?;
            let mut writer = std::io::BufWriter::new(file);

            let mut node_count = 0usize;
            for node in db.iter_nodes() {
                let labels: Vec<String> = node.labels.iter().map(|s| s.to_string()).collect();
                let properties: HashMap<String, Value> = node
                    .properties
                    .to_btree_map()
                    .into_iter()
                    .map(|(k, v)| (k.as_str().to_string(), v))
                    .collect();
                let record = serde_json::json!({
                    "type": "node",
                    "id": node.id.0,
                    "labels": labels,
                    "properties": properties,
                });
                serde_json::to_writer(&mut writer, &record)?;
                writeln!(writer)?;
                node_count += 1;
            }

            let mut edge_count = 0usize;
            for edge in db.iter_edges() {
                let properties: HashMap<String, Value> = edge
                    .properties
                    .to_btree_map()
                    .into_iter()
                    .map(|(k, v)| (k.as_str().to_string(), v))
                    .collect();
                let record = serde_json::json!({
                    "type": "edge",
                    "id": edge.id.0,
                    "source": edge.src.0,
                    "target": edge.dst.0,
                    "edge_type": edge.edge_type.as_str(),
                    "properties": properties,
                });
                serde_json::to_writer(&mut writer, &record)?;
                writeln!(writer)?;
                edge_count += 1;
            }

            writer.flush()?;
            output::status(
                &format!(
                    "Exported {} nodes and {} edges to {}",
                    node_count,
                    edge_count,
                    out.display()
                ),
                quiet,
            );
        }
        DataCommands::Load { input, path } => {
            let file = std::fs::File::open(&input)
                .with_context(|| format!("Failed to open input file: {}", input.display()))?;
            let reader = BufReader::new(file);

            let db = if path.exists() {
                super::open_existing(&path)?
            } else {
                grafeo_engine::GrafeoDB::open(&path)
                    .with_context(|| format!("Failed to create database at {}", path.display()))?
            };

            let mut node_count = 0usize;
            let mut edge_count = 0usize;

            for (line_num, line) in reader.lines().enumerate() {
                let line = line.with_context(|| format!("Failed to read line {}", line_num + 1))?;
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let record: serde_json::Value = serde_json::from_str(trimmed)
                    .with_context(|| format!("Invalid JSON on line {}", line_num + 1))?;

                match record.get("type").and_then(|t| t.as_str()) {
                    Some("node") => {
                        let labels: Vec<&str> = record
                            .get("labels")
                            .and_then(|l| l.as_array())
                            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();

                        let id = db.create_node(&labels);

                        if let Some(props) = record.get("properties").and_then(|p| p.as_object()) {
                            for (key, val) in props {
                                let value: Value =
                                    serde_json::from_value(val.clone()).unwrap_or(Value::Null);
                                db.set_node_property(id, key, value);
                            }
                        }
                        node_count += 1;
                    }
                    Some("edge") => {
                        let source = record
                            .get("source")
                            .and_then(|v| v.as_u64())
                            .map(grafeo_common::types::NodeId)
                            .ok_or_else(|| {
                                anyhow::anyhow!("Missing 'source' on line {}", line_num + 1)
                            })?;
                        let target = record
                            .get("target")
                            .and_then(|v| v.as_u64())
                            .map(grafeo_common::types::NodeId)
                            .ok_or_else(|| {
                                anyhow::anyhow!("Missing 'target' on line {}", line_num + 1)
                            })?;
                        let edge_type = record
                            .get("edge_type")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| {
                                anyhow::anyhow!("Missing 'edge_type' on line {}", line_num + 1)
                            })?;

                        let id = db.create_edge(source, target, edge_type);

                        if let Some(props) = record.get("properties").and_then(|p| p.as_object()) {
                            for (key, val) in props {
                                let value: Value =
                                    serde_json::from_value(val.clone()).unwrap_or(Value::Null);
                                db.set_edge_property(id, key, value);
                            }
                        }
                        edge_count += 1;
                    }
                    other => {
                        anyhow::bail!("Unknown record type {:?} on line {}", other, line_num + 1);
                    }
                }
            }

            output::status(
                &format!(
                    "Loaded {} nodes and {} edges into {}",
                    node_count,
                    edge_count,
                    path.display()
                ),
                quiet,
            );
        }
    }

    Ok(())
}
