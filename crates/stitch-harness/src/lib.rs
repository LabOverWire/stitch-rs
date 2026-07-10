//! Spawns a configured, in-process `mqdb` agent (MQTT broker + `$DB/` API) for
//! exercising stitch against a real broker in tests and examples. Configure
//! users, ACLs, the scope, and anonymous access, then [`BrokerHarness::start`]
//! binds an ephemeral loopback port and runs the agent until the returned
//! [`RunningBroker`] is dropped.
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! use stitch_harness::{Access, BrokerHarness};
//!
//! let broker = BrokerHarness::new()
//!     .scope("project", "projectId")
//!     .user("app", "secret")
//!     .allow("app", "$DB/#", Access::ReadWrite)
//!     .start()
//!     .await?;
//!
//! let url = broker.tcp_url(); // mqtt://127.0.0.1:<port>
//! // ... point a stitch Store at `url`, authenticate as app/secret ...
//! broker.shutdown().await;
//! # Ok(())
//! # }
//! ```

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};

use mqdb_agent::{Database, MqdbAgent};
use mqdb_core::types::ScopeConfig;
use mqtt5::broker::PasswordAuthProvider;
use tempfile::TempDir;
use tokio::task::JoinHandle;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

static NEXT_PORT: AtomicU16 = AtomicU16::new(28800);

/// Allocate a loopback TCP port that is free at call time.
#[must_use]
pub fn alloc_port() -> u16 {
    use std::net::TcpListener;
    for _ in 0..256 {
        let candidate = NEXT_PORT.fetch_add(1, Ordering::SeqCst);
        if candidate < 1024 {
            continue;
        }
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", candidate)) {
            drop(listener);
            return candidate;
        }
    }
    panic!("could not allocate a free loopback port")
}

/// Topic permission granted to a user in the broker ACL.
#[derive(Debug, Clone, Copy)]
pub enum Access {
    Read,
    Write,
    ReadWrite,
    Deny,
}

impl Access {
    fn token(self) -> &'static str {
        match self {
            Access::Read => "read",
            Access::Write => "write",
            Access::ReadWrite => "readwrite",
            Access::Deny => "deny",
        }
    }
}

struct AclEntry {
    user: String,
    topic: String,
    access: Access,
}

/// Builder for a local `mqdb` agent. Defaults: anonymous access allowed, rate
/// limiting off, scope root `project` / scope field `projectId`. Adding any user
/// switches anonymous access off unless re-enabled with [`BrokerHarness::anonymous`].
pub struct BrokerHarness {
    root_entity: String,
    scope_field: String,
    anonymous: bool,
    anonymous_set: bool,
    users: Vec<(String, String)>,
    acl: Vec<AclEntry>,
}

impl Default for BrokerHarness {
    fn default() -> Self {
        Self::new()
    }
}

impl BrokerHarness {
    #[must_use]
    pub fn new() -> Self {
        Self {
            root_entity: "project".to_string(),
            scope_field: "projectId".to_string(),
            anonymous: true,
            anonymous_set: false,
            users: Vec::new(),
            acl: Vec::new(),
        }
    }

    #[must_use]
    pub fn scope(mut self, root_entity: impl Into<String>, scope_field: impl Into<String>) -> Self {
        self.root_entity = root_entity.into();
        self.scope_field = scope_field.into();
        self
    }

    #[must_use]
    pub fn anonymous(mut self, allow: bool) -> Self {
        self.anonymous = allow;
        self.anonymous_set = true;
        self
    }

    #[must_use]
    pub fn user(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.users.push((username.into(), password.into()));
        if !self.anonymous_set {
            self.anonymous = false;
        }
        self
    }

    #[must_use]
    pub fn allow(
        mut self,
        username: impl Into<String>,
        topic: impl Into<String>,
        access: Access,
    ) -> Self {
        self.acl.push(AclEntry {
            user: username.into(),
            topic: topic.into(),
            access,
        });
        self
    }

    /// Bind an ephemeral loopback port, write the password/ACL files, and run the
    /// agent. The broker stays up until the returned [`RunningBroker`] is dropped
    /// or [`RunningBroker::shutdown`] is called.
    ///
    /// # Errors
    /// Returns an error if password hashing, opening the agent database, writing a
    /// config file, or starting the agent fails.
    pub async fn start(self) -> Result<RunningBroker, BoxError> {
        let port = alloc_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
        let dir = TempDir::new()?;

        let db = Database::open_without_background_tasks(dir.path().join("agent")).await?;
        let mut agent = MqdbAgent::new(db)
            .with_bind_address(addr)
            .with_anonymous(self.anonymous)
            .with_no_rate_limit()
            .with_scope_config(ScopeConfig::new(
                self.root_entity.clone(),
                self.scope_field.clone(),
            ));

        if !self.users.is_empty() {
            let mut contents = String::new();
            for (username, password) in &self.users {
                let hash = PasswordAuthProvider::hash_password(password)?;
                contents.push_str(&format!("{username}:{hash}\n"));
            }
            let path = dir.path().join("passwords");
            std::fs::write(&path, contents)?;
            agent = agent.with_password_file(path);
        }

        if !self.acl.is_empty() {
            let mut contents = String::new();
            for entry in &self.acl {
                contents.push_str(&format!(
                    "user {} topic {} permission {}\n",
                    entry.user,
                    entry.topic,
                    entry.access.token()
                ));
            }
            let path = dir.path().join("acl");
            std::fs::write(&path, contents)?;
            agent = agent.with_acl_file(path);
        }

        let (handle, mut ready_rx, shutdown_tx) = agent.start().await?;
        while !*ready_rx.borrow() {
            ready_rx.changed().await?;
        }

        Ok(RunningBroker {
            addr,
            _dir: dir,
            shutdown: shutdown_tx,
            handle: Some(handle),
        })
    }
}

/// A running local `mqdb` agent. Dropping it aborts the agent; call
/// [`RunningBroker::shutdown`] for a graceful stop.
pub struct RunningBroker {
    addr: SocketAddr,
    _dir: TempDir,
    shutdown: tokio::sync::broadcast::Sender<()>,
    handle: Option<JoinHandle<()>>,
}

impl RunningBroker {
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// TCP MQTT URL for the native stitch client (`mqtt://127.0.0.1:<port>`).
    #[must_use]
    pub fn tcp_url(&self) -> String {
        format!("mqtt://{}", self.addr)
    }

    /// Signal the agent to stop and wait briefly for its task to finish.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown.send(());
        if let Some(handle) = self.handle.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_millis(500), handle).await;
        }
    }
}

impl Drop for RunningBroker {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
