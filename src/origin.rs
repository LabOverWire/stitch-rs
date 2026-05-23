use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Origin {
    #[default]
    Local,
    Remote,
    Load,
    Clear,
}

impl Origin {
    #[must_use]
    pub fn skips_persistence(self) -> bool {
        matches!(self, Origin::Remote | Origin::Load | Origin::Clear)
    }

    #[must_use]
    pub fn skips_remote(self) -> bool {
        !matches!(self, Origin::Local)
    }
}
