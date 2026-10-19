use serde::{Deserialize, Serialize};

use crate::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContractPointer {
    pub contract_id: String,
    pub generation: u64,
}

impl ContractPointer {
    pub fn validate(&self) -> Result<()> {
        if self.contract_id.trim() != self.contract_id || self.contract_id.is_empty() {
            return Err(Error::Invalid("contract id must not be empty".to_string()));
        }
        if self.generation == 0 {
            return Err(Error::Invalid(
                "generation must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}
