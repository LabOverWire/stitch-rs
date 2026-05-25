use serde::{Deserialize, Serialize};

/// Where a mutation came from. Threaded through every mutating `Store` call to
/// gate fan-out to the persistence and remote layers.
///
/// | Tag | memory | persistence | offline queue | remote |
/// |---|---|---|---|---|
/// | [`Origin::Local`] | yes | yes | yes | yes |
/// | [`Origin::Remote`] | yes | no | no | no |
/// | [`Origin::Load`] | yes | no | no | no |
/// | [`Origin::Clear`] | yes | no | no | no |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Origin {
    /// App-initiated mutation. Fan-out goes to every layer.
    #[default]
    Local,
    /// Mutation that arrived over MQTT from another client. Written to memory
    /// only; persistence is handled separately by the inbound pipeline.
    Remote,
    /// Snapshot replay (e.g. seeding memory from persistence on scope load).
    /// Memory only.
    Load,
    /// Memory-only deletion used during scope teardown. Skips persistence and
    /// remote.
    Clear,
}

impl Origin {
    /// `true` for origins that should not write through to persistence.
    #[must_use]
    pub fn skips_persistence(self) -> bool {
        matches!(self, Origin::Remote | Origin::Load | Origin::Clear)
    }

    /// `true` for origins that should not be enqueued or published to remote.
    #[must_use]
    pub fn skips_remote(self) -> bool {
        !matches!(self, Origin::Local)
    }
}
