use thiserror::Error;

/// Shorthand for `std::result::Result<T, stitch::Error>`.
pub type Result<T> = std::result::Result<T, Error>;

/// Every fallible operation in stitch returns this error type. Use the
/// classifier methods ([`Error::is_transient`], [`Error::is_not_found`],
/// [`Error::is_conflict`], [`Error::is_ownership`],
/// [`Error::is_permanent_mutation`], [`Error::is_terminal_publish_rejection`],
/// [`Error::is_corruption`]) to branch on recoverable vs terminal failures.
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

    #[error("precondition failed for {entity}/{id}")]
    PreconditionFailed { entity: String, id: String },

    #[error("mqdb error in {method}: {source}")]
    Mqdb {
        method: String,
        #[source]
        source: Box<mqdb_core::error::Error>,
    },

    #[error("mqtt error: {0}")]
    Mqtt(String),

    #[error("publish rejected with reason code {reason_code}")]
    PublishRejected { reason_code: u8 },

    #[error("rejected: {0}")]
    Rejected(String),

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

const TERMINAL_PUBLISH_REASON_CODES: [u8; 3] = [0x87, 0x90, 0x99];

fn publish_reason_is_terminal(reason_code: u8) -> bool {
    TERMINAL_PUBLISH_REASON_CODES.contains(&reason_code)
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
    /// MQTT error, or a publish rejection whose reason code is not terminal).
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Timeout(_) | Self::ConnectionClosed | Self::Mqtt(_) => true,
            Self::PublishRejected { reason_code } => !publish_reason_is_terminal(*reason_code),
            _ => false,
        }
    }

    /// `true` if the broker refused the publish with a reason code that a retry
    /// cannot fix (not authorized, invalid topic name, invalid payload format).
    /// The store rolls back the optimistic row and the offline queue drops the
    /// mutation instead of re-sending it.
    #[must_use]
    pub fn is_terminal_publish_rejection(&self) -> bool {
        matches!(self, Self::PublishRejected { reason_code } if publish_reason_is_terminal(*reason_code))
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

    /// `true` if a compare-and-set precondition (an expected row version) did
    /// not hold. Terminal — never retried.
    #[must_use]
    pub fn is_precondition_failed(&self) -> bool {
        matches!(self, Self::PreconditionFailed { .. })
    }

    /// `true` for schema-level rejections that won't be fixed by retrying:
    /// unique / foreign-key / not-null violations from the local backend, and
    /// remote validation rejections surfaced as [`Error::Rejected`] (a broker
    /// `400`). The offline queue drops rows on these.
    #[must_use]
    pub fn is_permanent_mutation(&self) -> bool {
        match self {
            Self::Rejected(_) => true,
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
            Self::Mqdb { source, .. } => {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    matches!(
                        source.as_ref(),
                        mqdb_core::error::Error::Corruption { .. }
                            | mqdb_core::error::Error::Storage(_)
                    )
                }
                #[cfg(target_arch = "wasm32")]
                {
                    matches!(source.as_ref(), mqdb_core::error::Error::Corruption { .. })
                }
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_authorized_publish_rejection_is_terminal_not_transient() {
        let err = Error::PublishRejected { reason_code: 0x87 };
        assert!(err.is_terminal_publish_rejection());
        assert!(!err.is_transient());
    }

    #[test]
    fn invalid_topic_and_payload_publish_rejections_are_terminal() {
        for code in [0x90, 0x99] {
            let err = Error::PublishRejected { reason_code: code };
            assert!(err.is_terminal_publish_rejection(), "0x{code:02x}");
            assert!(!err.is_transient(), "0x{code:02x}");
        }
    }

    #[test]
    fn quota_and_unknown_publish_rejections_are_retried_not_terminal() {
        for code in [0x97, 0x91, 0x80, 0x83] {
            let err = Error::PublishRejected { reason_code: code };
            assert!(err.is_transient(), "0x{code:02x}");
            assert!(!err.is_terminal_publish_rejection(), "0x{code:02x}");
        }
    }
}
