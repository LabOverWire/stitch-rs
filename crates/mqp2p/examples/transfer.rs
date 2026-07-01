use clap::{Parser, Subcommand};
use mqp2p::{Peer, PeerConfig, TransferProgress};
use std::path::PathBuf;
use tracing::info;

#[derive(Parser)]
#[command(
    name = "mqp2p-transfer",
    about = "P2P file transfer over MQTT signaling"
)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:1883")]
    broker: String,

    #[arg(long, default_value = "0.0.0.0:0")]
    bind: String,

    #[arg(long, default_value = "stun.l.google.com:19302")]
    stun: String,

    #[arg(long)]
    user: Option<String>,

    #[arg(long)]
    pass: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Send {
        #[arg(long)]
        name: String,
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        peer: String,
    },
    Receive {
        #[arg(long)]
        name: String,
        #[arg(long, default_value = ".")]
        output: PathBuf,
    },
}

fn progress_callback(p: TransferProgress) {
    let pct = if p.total_bytes > 0 {
        (p.bytes_transferred as f64 / p.total_bytes as f64) * 100.0
    } else {
        0.0
    };
    info!("{:.1}% ({}/{})", pct, p.bytes_transferred, p.total_bytes);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let bind_addr: std::net::SocketAddr = cli.bind.parse()?;

    match cli.command {
        Command::Send {
            name,
            file,
            peer: target_name,
        } => {
            let mut config = PeerConfig::new(&name, &cli.broker)
                .with_bind_addr(bind_addr)
                .with_stun_server(&cli.stun);
            if let (Some(user), Some(pass)) = (&cli.user, &cli.pass) {
                config = config.with_credentials(user, pass);
            }

            let mut peer = Peer::new(config).await?;
            let peer_id = peer.register().await?;
            info!(peer_id, "registered as sender");

            let peers = peer.discover_peers().await?;
            let target = peers
                .iter()
                .find(|p| p.name == target_name)
                .ok_or_else(|| format!("peer '{target_name}' not found"))?;

            info!(target = target.name, "connecting to peer");
            let conn = peer.connect_to(target).await?;

            info!(file = %file.display(), "sending file");
            let result = conn.send_file(&file, progress_callback).await?;
            info!(
                name = result.file_name,
                size = result.file_size,
                sha256 = result.sha256,
                "transfer complete"
            );

            conn.close()?;
            peer.shutdown().await?;
        }
        Command::Receive { name, output } => {
            let mut config = PeerConfig::new(&name, &cli.broker)
                .with_bind_addr(bind_addr)
                .with_stun_server(&cli.stun);
            if let (Some(user), Some(pass)) = (&cli.user, &cli.pass) {
                config = config.with_credentials(user, pass);
            }

            let mut peer = Peer::new(config).await?;
            let peer_id = peer.register().await?;
            info!(peer_id, "registered as receiver, waiting for connection");

            let conn = peer.accept_connection().await?;
            info!(
                remote = conn.remote_peer().id,
                "connection accepted, waiting for file"
            );

            let result = conn
                .receive_file(
                    &output,
                    |offer| {
                        info!(name = offer.name, size = offer.size, "accepting file offer");
                        true
                    },
                    progress_callback,
                )
                .await?;
            info!(
                name = result.file_name,
                size = result.file_size,
                sha256 = result.sha256,
                "file received"
            );

            conn.close()?;
            peer.shutdown().await?;
        }
    }

    Ok(())
}
