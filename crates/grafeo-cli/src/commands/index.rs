//! Index management commands.

use anyhow::Result;
use serde::Serialize;

use crate::output::{self, Format};
use crate::{IndexCommands, OutputFormat};

/// Index information for serialization.
#[derive(Serialize)]
struct IndexOutput {
    name: String,
    index_type: String,
    target: String,
    unique: bool,
}

/// Run index commands.
pub fn run(cmd: IndexCommands, format: OutputFormat, quiet: bool) -> Result<()> {
    match cmd {
        IndexCommands::List { path } => {
            let db = super::open_existing(&path)?;
            let indexes = db.list_indexes();

            let fmt: Format = format.into();
            match fmt {
                Format::Json => {
                    if !quiet {
                        let output: Vec<IndexOutput> = indexes
                            .iter()
                            .map(|idx| IndexOutput {
                                name: idx.name.clone(),
                                index_type: idx.index_type.clone(),
                                target: idx.target.clone(),
                                unique: idx.unique,
                            })
                            .collect();
                        println!("{}", serde_json::to_string_pretty(&output)?);
                    }
                }
                Format::Table | Format::Csv => {
                    if indexes.is_empty() {
                        if !quiet {
                            println!("No indexes.");
                        }
                    } else {
                        let headers = vec![
                            "Name".to_string(),
                            "Type".to_string(),
                            "Target".to_string(),
                            "Unique".to_string(),
                        ];
                        let rows: Vec<Vec<String>> = indexes
                            .iter()
                            .map(|idx| {
                                vec![
                                    idx.name.clone(),
                                    idx.index_type.clone(),
                                    idx.target.clone(),
                                    if idx.unique { "yes" } else { "no" }.to_string(),
                                ]
                            })
                            .collect();
                        output::print_result_table(&headers, &rows, fmt, quiet);
                        if !quiet {
                            println!(
                                "{} index{}",
                                indexes.len(),
                                if indexes.len() == 1 { "" } else { "es" }
                            );
                        }
                    }
                }
            }
        }
        IndexCommands::Stats { path } => {
            let db = super::open_existing(&path)?;
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
