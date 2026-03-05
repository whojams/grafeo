//! Database compaction command.

use std::path::Path;

use anyhow::Result;
use serde::Serialize;

use crate::OutputFormat;
use crate::output::{self, Format, format_bytes};

/// Compaction result output.
#[derive(Serialize)]
struct CompactionOutput {
    dry_run: bool,
    before_size_bytes: usize,
    after_size_bytes: Option<usize>,
}

/// Run the compact command.
pub fn run(path: &Path, dry_run: bool, format: OutputFormat, quiet: bool) -> Result<()> {
    let db = super::open_existing(path)?;
    let stats_before = db.detailed_stats();

    if dry_run {
        output::status("Dry run, no changes will be made", quiet);

        let result = CompactionOutput {
            dry_run: true,
            before_size_bytes: stats_before.memory_bytes,
            after_size_bytes: None,
        };

        let fmt: Format = format.into();
        match fmt {
            Format::Json => {
                if !quiet {
                    println!("{}", serde_json::to_string_pretty(&result)?);
                }
            }
            Format::Table | Format::Csv => {
                let items = vec![
                    ("Mode", "Dry Run".to_string()),
                    ("Current Size", format_bytes(stats_before.memory_bytes)),
                    ("Nodes", stats_before.node_count.to_string()),
                    ("Edges", stats_before.edge_count.to_string()),
                ];
                output::print_key_value_table(&items, fmt, quiet);
            }
        }
    } else {
        output::status("Compacting database...", quiet);
        db.gc();

        let stats_after = db.detailed_stats();

        let result = CompactionOutput {
            dry_run: false,
            before_size_bytes: stats_before.memory_bytes,
            after_size_bytes: Some(stats_after.memory_bytes),
        };

        let fmt: Format = format.into();
        match fmt {
            Format::Json => {
                if !quiet {
                    println!("{}", serde_json::to_string_pretty(&result)?);
                }
            }
            Format::Table | Format::Csv => {
                let reclaimed = stats_before
                    .memory_bytes
                    .saturating_sub(stats_after.memory_bytes);
                let items = vec![
                    ("Before", format_bytes(stats_before.memory_bytes)),
                    ("After", format_bytes(stats_after.memory_bytes)),
                    ("Reclaimed", format_bytes(reclaimed)),
                ];
                output::print_key_value_table(&items, fmt, quiet);
            }
        }
    }

    Ok(())
}
