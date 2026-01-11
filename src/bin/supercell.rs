use anyhow::Result;
use sqlx::{sqlite::SqliteConnectOptions, SqlitePool};
use std::collections::HashMap;
use std::collections::HashSet;
use std::str::FromStr;
use std::env;
use supercell::cache::Cache;
use supercell::cache::CacheTask;
use supercell::cleanup::CleanTask;
use supercell::vmc::VerificationMethodCacheTask;
use tokio::net::TcpListener;
use tokio::signal;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing_subscriber::prelude::*;

use supercell::consumer::ConsumerTask;
use supercell::consumer::ConsumerTaskConfig;
use supercell::http::context::WebContext;
use supercell::http::server::build_router;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "supercell=debug,info".into()),
        ))
        .with(tracing_subscriber::fmt::layer().pretty())
        .init();

    let version = supercell::config::version()?;

    env::args().for_each(|arg| {
        if arg == "--version" {
            println!("{}", version);
            std::process::exit(0);
        }
    });

    let config = supercell::config::Config::new()?;

    let mut client_builder = reqwest::Client::builder();
    for ca_certificate in config.certificate_bundles.as_ref() {
        tracing::info!("Loading CA certificate: {:?}", ca_certificate);
        let cert = std::fs::read(ca_certificate)?;
        let cert = reqwest::Certificate::from_pem(&cert)?;
        client_builder = client_builder.add_root_certificate(cert);
    }

    client_builder = client_builder.user_agent(config.user_agent.clone());
    let http_client = client_builder.build()?;

    let connect_options = SqliteConnectOptions::from_str(&config.database_url)?
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .create_if_missing(true)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal);

    let pool = SqlitePool::connect_with(connect_options).await?;
    sqlx::migrate!().run(&pool).await?;

    let feeds: HashMap<String, (Option<String>, HashSet<String>)> = config
        .feeds
        .feeds
        .iter()
        .map(|feed| (feed.uri.clone(), (feed.deny.clone(), feed.allow.clone())))
        .collect();

    let all_dids = feeds
        .iter()
        .flat_map(|(_, (_, allow))| allow.iter().cloned())
        .collect::<HashSet<String>>();

    let cache = Cache::new(20);

    let web_context = WebContext::new(
        pool.clone(),
        config.external_base.as_str(),
        feeds,
        cache.clone(),
    );

    let app = build_router(web_context.clone());

    let tracker = TaskTracker::new();
    let token = CancellationToken::new();

    {
        let tracker = tracker.clone();
        let inner_token = token.clone();

        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        };

        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install signal handler")
                .recv()
                .await;
        };

        tokio::spawn(async move {
            tokio::select! {
                () = inner_token.cancelled() => { },
                _ = terminate => {},
                _ = ctrl_c => {},
            }

            tracker.close();
            inner_token.cancel();
        });
    }

    {
        let inner_config = config.clone();
        let task_enable = *inner_config.consumer_task_enable.as_ref();
        if task_enable {
            let consumer_task_config = ConsumerTaskConfig {
                user_agent: inner_config.user_agent.clone(),
                compression: *inner_config.compression.as_ref(),
                zstd_dictionary_location: inner_config.zstd_dictionary.clone(),
                jetstream_hostname: inner_config.jetstream_hostname.clone(),
                feeds: inner_config.feeds.clone(),
                collections: inner_config.collections.as_ref().clone(),
            };
            let task = ConsumerTask::new(pool.clone(), consumer_task_config, token.clone())?;
            let inner_token = token.clone();
            tracker.spawn(async move {
                if let Err(err) = task.run_background().await {
                    tracing::warn!(error = ?err, "consumer task error");
                }
                inner_token.cancel();
            });
        }
    }

    {
        let inner_config = config.clone();
        let task_enable = *inner_config.vmc_task_enable.as_ref();
        if task_enable {
            let task = VerificationMethodCacheTask::new(
                pool.clone(),
                http_client,
                inner_config.plc_hostname.clone(),
                all_dids,
                token.clone(),
            );
            task.main().await?;
            let inner_token = token.clone();
            tracker.spawn(async move {
                if let Err(err) = task.run_background(chrono::Duration::hours(4)).await {
                    tracing::warn!(error = ?err, "consumer task error");
                }
                inner_token.cancel();
            });
        }
    }

    {
        let inner_config = config.clone();
        let task_enable = *inner_config.cache_task_enable.as_ref();
        if task_enable {
            let task = CacheTask::new(
                pool.clone(),
                cache.clone(),
                inner_config.clone(),
                token.clone(),
            );
            task.main().await?;
            let inner_token = token.clone();
            let interval = *inner_config.cache_task_interval.as_ref();
            tracker.spawn(async move {
                if let Err(err) = task.run_background(interval).await {
                    tracing::warn!(error = ?err, "cache task error");
                }
                inner_token.cancel();
            });
        }
    }

    {
        let inner_config = config.clone();
        let task_enable = *inner_config.cleanup_task_enable.as_ref();
        let max_age = *inner_config.cleanup_task_max_age.as_ref();
        if task_enable {
            let task = CleanTask::new(pool.clone(), max_age, token.clone());
            task.main().await?;
            let inner_token = token.clone();
            let interval = *inner_config.cleanup_task_interval.as_ref();
            tracker.spawn(async move {
                if let Err(err) = task.run_background(interval).await {
                    tracing::warn!(error = ?err, "cleanup task error");
                }
                inner_token.cancel();
            });
        }
    }

    {
        let inner_config = config.clone();
        let http_port = *inner_config.http_port.as_ref();
        let inner_token = token.clone();
        tracker.spawn(async move {
            let listener = TcpListener::bind(&format!("0.0.0.0:{}", http_port))
                .await
                .unwrap();

            let shutdown_token = inner_token.clone();
            let result = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    tokio::select! {
                        () = shutdown_token.cancelled() => { }
                    }
                    tracing::info!("axum graceful shutdown complete");
                })
                .await;
            if let Err(err) = result {
                tracing::error!("axum task failed: {}", err);
            }

            inner_token.cancel();
        });
    }

    tracker.wait().await;

    tracing::info!("closing database connection pool");
    pool.close().await;

    tracing::info!("shutdown complete");

    Ok(())
}
