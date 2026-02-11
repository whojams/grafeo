//! In-process embedding generation for zero-API-dependency vector workflows.
//!
//! When the `embed` feature is enabled, you can load ONNX models and generate
//! embeddings directly in your application without calling external APIs.
//!
//! # Example
//!
//! ```ignore
//! use grafeo_engine::embedding::OnnxEmbeddingModel;
//!
//! let model = OnnxEmbeddingModel::from_files(
//!     "all-MiniLM-L6-v2",
//!     "model.onnx",
//!     "tokenizer.json",
//! )?;
//! let vectors = model.embed(&["Hello world", "Graph databases"])?;
//! assert_eq!(vectors.len(), 2);
//! assert_eq!(vectors[0].len(), model.dimensions());
//! ```

mod onnx;

pub use onnx::OnnxEmbeddingModel;

use grafeo_common::utils::error::Result;

/// Trait for embedding models that convert text to vectors.
///
/// Implement this trait for custom embedding backends (API-based, local
/// inference, etc.). The default implementation provided is [`OnnxEmbeddingModel`]
/// which runs ONNX models locally via the ONNX Runtime.
pub trait EmbeddingModel: Send + Sync {
    /// Generates embeddings for a batch of texts.
    ///
    /// Returns one vector per input text, each with [`dimensions()`] elements.
    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>>;

    /// Returns the dimensionality of the embedding vectors.
    fn dimensions(&self) -> usize;

    /// Returns the model name.
    fn name(&self) -> &str;
}
