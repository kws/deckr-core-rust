//! Embedded JSON Schema validation with host-supplied, in-memory resources.
//!
//! This module contains no Deckr schema bundle. Hosts may supply published
//! resources from their selected contract revision. Compilation never reads a
//! file or fetches a URL; missing references fail explicitly.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value;

use crate::{Error, Result};

/// Validate a schema against its declared dialect (2020-12 when omitted).
/// References to instance schemas are resolved later, when compiling it.
pub fn validate_schema(schema: &Value) -> Result<()> {
    jsonschema::meta::validate(schema)
        .map_err(|error| Error::Invalid(format!("invalid embedded JSON Schema: {error}")))
}

#[derive(Debug, Clone, Default)]
pub struct SchemaRegistry {
    resources: Arc<BTreeMap<String, Value>>,
}

impl SchemaRegistry {
    /// Add an exact URI resource. Duplicate registrations fail so the meaning
    /// of an already supplied reference cannot change silently.
    pub fn with_resource(mut self, uri: impl Into<String>, schema: Value) -> Result<Self> {
        let uri = uri.into();
        let parsed = jsonschema::Uri::parse(uri.clone()).map_err(|(error, _)| {
            Error::Invalid(format!("invalid schema resource URI: {error}"))
        })?;
        if parsed.fragment().is_some() {
            return Err(Error::Invalid(
                "schema resource URI must be absolute without a fragment".into(),
            ));
        }
        validate_schema(&schema)?;
        let resources = Arc::make_mut(&mut self.resources);
        if resources.contains_key(&uri) {
            return Err(Error::Invalid(format!("duplicate schema resource: {uri}")));
        }
        resources.insert(uri, schema);
        Ok(self)
    }

    /// Compile once at a managed boundary, then reuse for operations.
    pub fn compile(&self, schema: &Value) -> Result<CompiledSchema> {
        validate_schema(schema)?;
        let validator = jsonschema::options()
            .with_retriever(self.clone())
            .build(schema)
            .map_err(|error| {
                Error::Invalid(format!("cannot compile embedded JSON Schema: {error}"))
            })?;
        Ok(CompiledSchema(Arc::new(validator)))
    }
}

impl jsonschema::Retrieve for SchemaRegistry {
    fn retrieve(
        &self,
        uri: &jsonschema::Uri<String>,
    ) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        self.resources.get(uri.as_str()).cloned().ok_or_else(|| {
            Box::new(Error::Invalid(format!(
                "schema resource not supplied by host: {uri}"
            ))) as _
        })
    }
}

#[derive(Debug, Clone)]
pub struct CompiledSchema(Arc<jsonschema::Validator>);

impl CompiledSchema {
    pub fn validate(&self, instance: &Value) -> Result<()> {
        self.0.validate(instance).map_err(|error| {
            Error::Invalid(format!(
                "payload does not match schema at {}: {}",
                error.instance_path(),
                error.masked()
            ))
        })
    }
}
