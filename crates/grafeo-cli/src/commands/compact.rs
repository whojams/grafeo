//! Database compaction command.

use std::path::Path;

use anyhow::Result;
use grafeo_engine::GrafeoDB;
use serde::Serialize;

use crate::OutputFormat;
use crate::output::{self, Format, format_bytes};

/// Compaction result output.
#[derive(Serialize)]
struct CompactionOutput {
    dry_run: bool,
    before_size_bytes: usize,
}

/// Run the compact command.
pub fn run(path: &Path, dry_run: bool, format: OutputFormat, quiet: bool) -> Result<()> {
    if !dry_run {
        anyhow::bail!(
            "grafeo compact is not yet implemented. \
             Compaction APIs are planned for a future release."
        );
    }

    let db = GrafeoDB::open(path)?;
    let stats_before = db.detailed_stats();

    output::status("Dry run - no changes will be made", quiet);

    let output = CompactionOutput {
        dry_run: true,
        before_size_bytes: stats_before.memory_bytes,
    };

    let fmt: Format = format.into();
    match fmt {
        Format::Json => {
            if !quiet {
                println!("{}", serde_json::to_string_pretty(&output)?);
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

    Ok(())
}
