use thiserror::Error;

/// Shorthand for `std::result::Result<T, stitch::Error>`.
pub type Result<T> = std::result::Result<T, Error>;

/// Every fallible operation in stitch returns this error type. Use the
/// classifier methods ([`Error::is_transient`], [`Error::is_not_found`],
/// [`Error::is_conflict`], [`Error::is_ownership`],
/// [`Error::is_permanent_mutation`], [`Error::is_corruption`]) to branch on
/// recoverable vs terminal failures.
#[derive(Debug, Error)]
pub enum Error {
    #[error("not initialized")]
    NotInitialized,

    #[error("already initialized")]
    AlreadyInitialized,

    #[error("scope not active: {0}")]
    ScopeNotActive(String),

    #[error("entity not configured: {0}")]
    UnknownEntity(String),

    #[error("entity not found: {entity}/{id}")]
    NotFound { entity: String, id: String },

    #[error("ownership denied for {entity}/{id}")]
    Ownership { entity: String, id: String },

    #[error("conflict for {entity}/{id}")]
    Conflict { entity: String, id: String },

    #[error("mqdb error in {method}: {source}")]
    Mqdb {
        method: String,
        #[source]
        source: Box<mqdb_core::error::Error>,
    },

    #[error("mqtt error: {0}")]
    Mqtt(String),

    #[error("connection closed")]
    ConnectionClosed,

    #[error("request timeout after {0}ms")]
    Timeout(u64),

    #[error("invalid configuration: {0}")]
    Config(String),

    #[error("session invalid")]
    SessionInvalid,

    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    #[must_use]
    pub fn mqdb(method: impl Into<String>, source: mqdb_core::error::Error) -> Self {
        match source {
            mqdb_core::error::Error::NotFound { entity, id } => Self::NotFound { entity, id },
            other => Self::Mqdb {
                method: method.into(),
                source: Box::new(other),
            },
        }
    }

    /// `true` if a retry might succeed (timeout, dropped connection, generic
    /// MQTT error).
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Timeout(_) | Self::ConnectionClosed | Self::Mqtt(_)
        )
    }

    /// `true` if the server rejected the call because the authenticated user
    /// doesn't own the target row.
    #[must_use]
    pub fn is_ownership(&self) -> bool {
        matches!(self, Self::Ownership { .. })
    }

    /// `true` if the referenced row doesn't exist.
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
    }

    /// `true` if an insert collided with an existing row.
    #[must_use]
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict { .. })
    }

    /// `true` for schema-level rejections that won't be fixed by retrying:
    /// unique / foreign-key / not-null violations. The offline queue drops
    /// rows on these.
    #[must_use]
    pub fn is_permanent_mutation(&self) -> bool {
        match self {
            Self::Mqdb { source, .. } => matches!(
                source.as_ref(),
                mqdb_core::error::Error::Validation(_)
                    | mqdb_core::error::Error::ConstraintViolation(_)
                    | mqdb_core::error::Error::ForeignKeyViolation { .. }
                    | mqdb_core::error::Error::ForeignKeyRestrict { .. }
                    | mqdb_core::error::Error::NotNullViolation { .. }
                    | mqdb_core::error::Error::InvalidForeignKey
            ),
            _ => false,
        }
    }

    /// `true` if the underlying fjall database reported corruption. Callers
    /// should invoke [`Store::recover_persistence`](crate::Store::recover_persistence).
    #[must_use]
    pub fn is_corruption(&self) -> bool {
        match self {
            Self::Mqdb { source, .. } => matches!(
                source.as_ref(),
                mqdb_core::error::Error::Corruption { .. } | mqdb_core::error::Error::Storage(_)
            ),
            _ => false,
        }
    }
}
