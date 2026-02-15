//! Index management commands.

use anyhow::Result;
use grafeo_engine::GrafeoDB;
use serde::Serialize;

use crate::output::{self, Format};
use crate::{IndexCommands, OutputFormat};

/// Index statistics output.
#[derive(Serialize)]
struct IndexStatsOutput {
    total_indexes: usize,
}

/// Run index commands.
pub fn run(cmd: IndexCommands, format: OutputFormat, quiet: bool) -> Result<()> {
    match cmd {
        IndexCommands::List { path } => {
            let db = GrafeoDB::open(&path)?;
            let stats = db.detailed_stats();

            let fmt: Format = format.into();
            match fmt {
                Format::Json => {
                    if !quiet {
                        let output = IndexStatsOutput {
                            total_indexes: stats.index_count,
                        };
                        println!("{}", serde_json::to_string_pretty(&output)?);
                    }
                }
                Format::Table | Format::Csv => {
                    if !quiet {
                        println!("Total indexes: {}", stats.index_count);
                        println!(
                            "\nNote: detailed index listing is not yet available. \
                             Use `grafeo index stats` for aggregate index statistics."
                        );
                    }
                }
            }
        }
        IndexCommands::Stats { path } => {
            let db = GrafeoDB::open(&path)?;
            let stats = db.detailed_stats();

            let fmt: Format = format.into();
            let items = vec![
                ("Total Indexes", stats.index_count.to_string()),
                ("Labels Indexed", stats.label_count.to_string()),
                ("Edge Types Indexed", stats.edge_type_count.to_string()),
            ];
            output::print_key_value_table(&items, fmt, quiet);
        }
    }

    Ok(())
}
