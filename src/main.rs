mod errors;
mod grpc_geyser;
mod leader_tracker;
mod multi_cache;
mod rpc_server;
mod solana_rpc;
mod transaction_store;
mod txn_sender;
mod utils;
mod vendor;

use std::{
    env,
    net::{IpAddr, Ipv4Addr, UdpSocket},
    str::FromStr,
    sync::Arc,
};

use cadence::{BufferedUdpMetricSink, QueuingMetricSink, StatsdClient};
use cadence_macros::set_global_default;
use figment::{providers::Env, Figment};
use futures::future::join_all;
use grpc_geyser::GrpcGeyserImpl;
use jsonrpsee::server::{middleware::ProxyGetRequestLayer, Server, ServerBuilder, ServerHandle};
use leader_tracker::{LeaderTracker, LeaderTrackerImpl};
use multi_cache::MultiCache;
use rpc_server::{AtlasTxnSenderImpl, AtlasTxnSenderServer};
use serde::Deserialize;
use solana_client::{connection_cache::ConnectionCache, rpc_client::RpcClient};
use solana_rpc::SolanaRpc;
use solana_sdk::signature::{read_keypair_file, Keypair};
use tokio::sync::RwLock;
use tower::layer::util::{Identity, Stack};
use tracing::{error, info};
use transaction_store::TransactionStoreImpl;
use txn_sender::TxnSenderImpl;
use yellowstone_grpc_client::GeyserGrpcClient;

#[derive(Debug, Deserialize, Clone)]
struct AtlasTxnSenderEnv {
    identity_keypair_file: Option<String>,
    grpc_url: Option<String>,
    rpc_url: Option<String>,
    port: Option<u16>,
    tpu_connection_pool_size: Option<usize>,
    x_token: Option<String>,
    num_leaders: Option<usize>,
    leader_offset: Option<i64>,
    txn_sender_threads: Option<usize>,
    max_txn_send_retries: Option<usize>,
    txn_send_retry_interval: Option<usize>,
    host_addr: Option<String>,
    extra_addrs: Option<Vec<String>>,
}

// Defualt on RPC is 4
pub const DEFAULT_TPU_CONNECTION_POOL_SIZE: usize = 4;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init metrics/logging
    let env: AtlasTxnSenderEnv = Figment::from(Env::raw()).extract().unwrap();
    let env_clone = env.clone();
    let env_filter = env::var("RUST_LOG")
        .or::<Result<String, ()>>(Ok("info".to_string()))
        .unwrap();
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .json()
        .init();
    new_metrics_client();

    let client = Arc::new(RwLock::new(
        GeyserGrpcClient::connect::<String, String>(
            env_clone.grpc_url.unwrap(),
            env_clone.x_token,
            None,
        )
        .unwrap(),
    ));
    let solana_rpc = Arc::new(GrpcGeyserImpl::new(client));
    let rpc_client = Arc::new(RpcClient::new(env_clone.rpc_url.unwrap()));
    let num_leaders = env_clone.num_leaders.unwrap_or(2);
    let leader_offset = env_clone.leader_offset.unwrap_or(0);
    let leader_tracker = Arc::new(LeaderTrackerImpl::new(
        rpc_client,
        solana_rpc.clone(),
        num_leaders,
        leader_offset,
    ));
    let tpu_connection_pool_size = env
        .tpu_connection_pool_size
        .unwrap_or(DEFAULT_TPU_CONNECTION_POOL_SIZE);

    let port = env.port.unwrap_or(4040);
    let host_addr = env_clone.host_addr.unwrap_or("0.0.0.0".to_string());
    let mut addrs: Vec<String> = env_clone
        .extra_addrs
        .unwrap_or(vec![])
        .iter()
        .map(|s| s.to_string())
        .filter(|s| s != "0.0.0.0" && s != "")
        .collect();
    addrs.push(host_addr);
    addrs.dedup();

    let connection_caches: Vec<ConnectionCache> = addrs
        .iter()
        .map(|addr| {
            let keypair = if let Some(identity_keypair_file) = env.identity_keypair_file.clone() {
                read_keypair_file(identity_keypair_file).expect("keypair file must exist")
            } else {
                Keypair::new()
            };
            let connection_cache = ConnectionCache::new_with_client_options(
                "atlas-txn-sender",
                tpu_connection_pool_size,
                None, // created if none specified
                Some((
                    &keypair,
                    IpAddr::V4(Ipv4Addr::from_str(&addr).expect("addr was not valid ipv4")),
                )),
                None, // not used as far as I can tell
            );
            return connection_cache;
        })
        .collect();
    let multi_cache = Arc::new(MultiCache::new(connection_caches));
    let transaction_store = Arc::new(TransactionStoreImpl::new());
    let txn_send_retry_interval_seconds = env.txn_send_retry_interval.unwrap_or(2);
    let txn_sender = Arc::new(TxnSenderImpl::new(
        leader_tracker,
        transaction_store,
        multi_cache,
        solana_rpc,
        env.txn_sender_threads.unwrap_or(1),
        txn_send_retry_interval_seconds,
    ));
    let max_txn_send_retries = env.max_txn_send_retries.unwrap_or(5);
    let atlas_txn_sender = AtlasTxnSenderImpl::new(txn_sender, max_txn_send_retries);

    let server = ServerBuilder::default()
        .set_middleware(
            tower::ServiceBuilder::new()
                // Proxy `GET /health` requests to internal `health` method.
                .layer(
                    ProxyGetRequestLayer::new("/health", "health")
                        .expect("expected health check to initialize"),
                ),
        )
        .max_request_body_size(15_000_000)
        .max_connections(10_000)
        .build(format!("0.0.0.0:{}", port))
        .await
        .expect(format!("failed to start server on port {}", port).as_str());

    let handle = server.start(atlas_txn_sender.into_rpc());
    handle.stopped().await;

    Ok(())
}

fn new_metrics_client() {
    let uri = env::var("METRICS_URI")
        .or::<String>(Ok("127.0.0.1".to_string()))
        .unwrap();
    let port = env::var("METRICS_PORT")
        .or::<String>(Ok("7998".to_string()))
        .unwrap()
        .parse::<u16>()
        .unwrap();
    info!("collecting metrics on: {}:{}", uri, port);
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.set_nonblocking(true).unwrap();

    let host = (uri, port);
    let udp_sink = BufferedUdpMetricSink::from(host, socket).unwrap();
    let queuing_sink = QueuingMetricSink::from(udp_sink);
    let builder = StatsdClient::builder("atlas_txn_sender", queuing_sink);
    let client = builder
        .with_error_handler(|e| error!("statsd metrics error: {}", e))
        .build();
    set_global_default(client);
}
