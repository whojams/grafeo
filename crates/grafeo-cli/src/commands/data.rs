//! Data export/import commands.

use anyhow::Result;

use crate::output;
use crate::{DataCommands, OutputFormat};

/// Run data commands.
pub fn run(cmd: DataCommands, _format: OutputFormat, quiet: bool) -> Result<()> {
    match cmd {
        DataCommands::Dump {
            path,
            output: out,
            format: dump_format,
        } => {
            let format_name = dump_format.as_deref().unwrap_or("parquet");
            output::status(
                &format!(
                    "Export requested: {} to {} (format: {})",
                    path.display(),
                    out.display(),
                    format_name
                ),
                quiet,
            );

            anyhow::bail!(
                "grafeo data dump is not yet implemented. \
                 Format-specific export (Parquet, CSV, JSON) is planned for a future release. \
                 Use `grafeo backup` to create a binary snapshot instead."
            );
        }
        DataCommands::Load { input, path } => {
            output::status(
                &format!(
                    "Import requested: {} into {}",
                    input.display(),
                    path.display()
                ),
                quiet,
            );

            anyhow::bail!(
                "grafeo data load is not yet implemented. \
                 Format-specific import (Parquet, CSV, JSON) is planned for a future release. \
                 Use `grafeo backup --restore` to restore from a binary snapshot instead."
            );
        }
    }
}
