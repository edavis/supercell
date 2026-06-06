use std::collections::HashSet;
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use chrono::DateTime;
use futures_util::SinkExt;
use futures_util::StreamExt;
use http::HeaderValue;
use http::Uri;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, Instant};
use tokio_util::sync::CancellationToken;
use tokio_websockets::{ClientBuilder, Message};

use crate::config;
use crate::matcher::FeedMatchers;
use crate::matcher::Match;
use crate::storage;
use crate::storage::consumer_control_get;
use crate::storage::denylist_all;
use crate::storage::StoragePool;
use crate::writer::WriteCommand;

const MAX_MESSAGE_SIZE: usize = 25000;

#[derive(Clone)]
pub struct ConsumerTaskConfig {
    pub user_agent: String,
    pub compression: bool,
    pub zstd_dictionary_location: String,
    pub jetstream_hostname: String,
    pub feeds: config::Feeds,
    pub collections: Vec<String>,
}

pub struct ConsumerTask {
    cancellation_token: CancellationToken,
    pool: StoragePool,
    config: ConsumerTaskConfig,
    feed_matchers: FeedMatchers,
    write_tx: Sender<WriteCommand>,
}

impl ConsumerTask {
    pub fn new(
        pool: StoragePool,
        config: ConsumerTaskConfig,
        cancellation_token: CancellationToken,
        write_tx: Sender<WriteCommand>,
    ) -> Result<Self> {
        let feed_matchers = FeedMatchers::from_config(&config.feeds)?;

        Ok(Self {
            pool,
            cancellation_token,
            config,
            feed_matchers,
            write_tx,
        })
    }

    pub async fn run_background(&self) -> Result<()> {
        tracing::debug!("ConsumerTask started");

        let last_time_us =
            consumer_control_get(&self.pool, &self.config.jetstream_hostname).await?;

        tracing::info!(cursor = ?last_time_us, "loaded cursor from database");

        let mut denylist: HashSet<String> = denylist_all(&self.pool).await?;
        tracing::info!(count = denylist.len(), "loaded denylist from database");

        let cursor_param = if let Some(cursor) = last_time_us {
            format!("&cursor={}", cursor)
        } else {
            String::new()
        };

        let uri = Uri::from_str(&format!(
            "wss://{}/subscribe?compress={}&requireHello=true{}",
            self.config.jetstream_hostname, self.config.compression, cursor_param
        ))
        .context("invalid jetstream URL")?;

        tracing::info!(uri = %uri, "connecting to jetstream");

        let (mut client, _) = ClientBuilder::from_uri(uri)
            .add_header(
                http::header::USER_AGENT,
                HeaderValue::from_str(&self.config.user_agent)?,
            )
            .connect()
            .await
            .map_err(|err| anyhow::Error::new(err).context("cannot connect to jetstream"))?;

        let update = model::SubscriberSourcedMessage::Update {
            wanted_collections: self.config.collections.clone(),
            wanted_dids: vec![],
            max_message_size_bytes: MAX_MESSAGE_SIZE as u64,
            cursor: None,
        };
        let serialized_update = serde_json::to_string(&update)
            .map_err(|err| anyhow::Error::msg(err).context("cannot serialize update"))?;

        tracing::info!(message = %serialized_update, "sending subscription update to jetstream");

        client
            .send(Message::text(serialized_update))
            .await
            .map_err(|err| anyhow::Error::msg(err).context("cannot send update"))?;

        let mut decompressor = if self.config.compression {
            // mkdir -p data/ && curl -o data/zstd_dictionary https://github.com/bluesky-social/jetstream/raw/refs/heads/main/pkg/models/zstd_dictionary
            let data: Vec<u8> = std::fs::read(self.config.zstd_dictionary_location.clone())
                .context("unable to load zstd dictionary")?;
            zstd::bulk::Decompressor::with_dictionary(&data)
                .map_err(|err| anyhow::Error::msg(err).context("cannot create decompressor"))?
        } else {
            zstd::bulk::Decompressor::new()
                .map_err(|err| anyhow::Error::msg(err).context("cannot create decompressor"))?
        };

        let interval = std::time::Duration::from_secs(120);
        let sleeper = sleep(interval);
        tokio::pin!(sleeper);

        let heartbeat_interval = std::time::Duration::from_secs(15);
        let heartbeat_sleeper = sleep(heartbeat_interval);
        tokio::pin!(heartbeat_sleeper);

        let denylist_interval = std::time::Duration::from_secs(60);
        let denylist_sleeper = sleep(denylist_interval);
        tokio::pin!(denylist_sleeper);

        let mut time_usec = 0i64;

        // Diagnostics: socket drain rate and read-stall detection.
        let mut messages_read: u64 = 0;
        let mut messages_since_heartbeat: u64 = 0;
        let mut last_message_at: Option<Instant> = None;

        loop {
            tokio::select! {
                () = self.cancellation_token.cancelled() => {
                    break;
                },
                () = &mut sleeper => {
                        // Hand the cursor to the writer task, which flushes any
                        // buffered writes before persisting it.
                        self.write_tx
                            .send(WriteCommand::Checkpoint(time_usec))
                            .await
                            .map_err(|_| anyhow!("write channel closed"))?;
                        sleeper.as_mut().reset(Instant::now() + interval);
                },
                () = &mut denylist_sleeper => {
                        denylist = denylist_all(&self.pool).await?;
                        denylist_sleeper.as_mut().reset(Instant::now() + denylist_interval);
                },
                () = &mut heartbeat_sleeper => {
                    let timestamp = (time_usec > 0)
                        .then(|| DateTime::from_timestamp_micros(time_usec).map(|dt| dt.to_rfc3339()))
                        .flatten();
                    tracing::info!(
                        time_us = time_usec,
                        timestamp = ?timestamp,
                        messages_since_last = messages_since_heartbeat,
                        write_channel_available = self.write_tx.capacity(),
                        "consumer heartbeat"
                    );
                    messages_since_heartbeat = 0;
                    heartbeat_sleeper.as_mut().reset(Instant::now() + heartbeat_interval);
                },
                item = client.next() => {
                    if item.is_none() {
                        let since_last_ms = last_message_at.map(|t| t.elapsed().as_millis());
                        tracing::warn!(
                            messages_read,
                            since_last_message_ms = ?since_last_ms,
                            "jetstream connection closed"
                        );
                        break;
                    }
                    let item = item.unwrap();

                    if let Err(err) = item {
                        let since_last_ms = last_message_at.map(|t| t.elapsed().as_millis());
                        tracing::error!(error = ?err, messages_read, since_last_message_ms = ?since_last_ms, "error reading from jetstream");
                        continue;
                    }
                    let item = item.unwrap();

                    messages_read += 1;
                    messages_since_heartbeat += 1;
                    last_message_at = Some(Instant::now());

                    // Handle control frames once, independent of payload
                    // compression (control frames are never compressed).
                    if item.is_ping() {
                        // tokio-websockets queues a pong when it reads a ping
                        // but only sends it on flush. We otherwise never write
                        // after the initial subscription, so flush now or
                        // jetstream never sees our keepalive reply.
                        if let Err(err) = client.flush().await {
                            tracing::warn!(error = ?err, "failed to flush pong response");
                        }
                        continue;
                    }
                    if item.is_pong() {
                        continue;
                    }
                    if item.is_close() {
                        match item.as_close() {
                            Some((code, reason)) => {
                                tracing::warn!(?code, reason = %reason, "jetstream sent close frame");
                            }
                            None => tracing::warn!("jetstream sent close frame"),
                        }
                        continue;
                    }

                    let event = if self.config.compression {
                        if !item.is_binary() {
                            // Log unexpected non-binary message types
                            tracing::warn!("received unexpected non-binary message from jetstream (not ping/pong/close)");
                            continue;
                        }
                        let payload = item.into_payload();

                        let decoded = decompressor.decompress(&payload, MAX_MESSAGE_SIZE * 3);
                        if let Err(err) = decoded {
                            tracing::debug!(err = ?err, "cannot decompress message");
                            continue;
                        }
                        let decoded = decoded.unwrap();
                        serde_json::from_slice::<model::Event>(&decoded)
                        .context(anyhow!("cannot deserialize message"))
                    } else {
                        if !item.is_text() {
                            // Log unexpected non-text message types
                            tracing::warn!("received unexpected non-text message from jetstream (not ping/pong/close)");
                            continue;
                        }
                        item.as_text()
                            .ok_or(anyhow!("cannot convert message to text"))
                            .and_then(|value| {
                                serde_json::from_str::<model::Event>(value)
                                .context(anyhow!("cannot deserialize message"))
                            })
                    };
                    if let Err(err) = event {
                        tracing::error!(error = ?err, "error processing jetstream message");

                        continue;
                    }
                    let event = event.unwrap();

                    let previous_time_usec = time_usec;
                    time_usec = std::cmp::max(time_usec, event.time_us);

                    if previous_time_usec == 0 {
                        let datetime = DateTime::from_timestamp_micros(event.time_us)
                            .map(|dt| dt.to_rfc3339())
                            .unwrap_or_else(|| format!("{} microseconds", event.time_us));
                        tracing::info!(time_us = event.time_us, timestamp = %datetime, "received first event from jetstream");
                    }

                    if event.kind != "commit" {
                        continue;
                    }

                    let event_value = serde_json::to_value(&event);
                    if let Err(err) = event_value {
                        tracing::error!(error = ?err, "error processing jetstream message");
                        continue;
                    }
                    let event_value = event_value.unwrap();

                    // Assumption: Performing a query for each event will cost more in the
                    // long-term than evaluating each event against all matchers and if there's a
                    // match, then checking both the event DID and the AT-URI DID.
                    'matchers_loop: for feed_matcher in self.feed_matchers.0.iter() {
                        if let Some(Match(op, aturi)) = feed_matcher.matches(&event_value) {
                            tracing::debug!(feed_id = ?feed_matcher.feed, "matched event");

                            let aturi_did = did_from_aturi(&aturi);
                            if denylist.contains(event.did.as_str()) || denylist.contains(aturi_did.as_str()) {
                                break 'matchers_loop;
                            }

                            let feed_content = storage::model::FeedContent{
                                feed_id: feed_matcher.feed.clone(),
                                uri: aturi,
                                indexed_at: event.time_us,
                                score: 1,
                            };
                            self.write_tx
                                .send(WriteCommand::Content(feed_content, op))
                                .await
                                .map_err(|_| anyhow!("write channel closed"))?;
                        }
                    }
                }
            }
        }

        // Persist the cursor on the way out (including on a jetstream
        // disconnect) so progress survives a restart. The writer flushes any
        // remaining buffered writes before persisting it.
        let _ = self
            .write_tx
            .send(WriteCommand::Checkpoint(time_usec))
            .await;

        tracing::debug!("ConsumerTask stopped");

        Ok(())
    }
}

fn did_from_aturi(aturi: &str) -> String {
    let aturi_len = aturi.len();
    if aturi_len < 6 {
        return "".to_string();
    }
    let collection_start = aturi[5..]
        .find("/")
        .map(|value| value + 5)
        .unwrap_or(aturi_len);
    aturi[5..collection_start].to_string()
}

pub(crate) mod model {

    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "type", content = "payload")]
    pub(crate) enum SubscriberSourcedMessage {
        #[serde(rename = "options_update")]
        Update {
            #[serde(rename = "wantedCollections")]
            wanted_collections: Vec<String>,

            #[serde(rename = "wantedDids", skip_serializing_if = "Vec::is_empty", default)]
            wanted_dids: Vec<String>,

            #[serde(rename = "maxMessageSizeBytes")]
            max_message_size_bytes: u64,

            #[serde(skip_serializing_if = "Option::is_none")]
            cursor: Option<i64>,
        },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(crate) struct Facet {
        pub(crate) features: Vec<HashMap<String, String>>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(crate) struct StrongRef {
        pub(crate) uri: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(crate) struct Reply {
        pub(crate) root: Option<StrongRef>,
        pub(crate) parent: Option<StrongRef>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "$type")]
    pub(crate) enum Record {
        #[serde(rename = "app.bsky.feed.post")]
        Post {
            #[serde(flatten)]
            extra: HashMap<String, serde_json::Value>,
        },
        #[serde(rename = "app.bsky.feed.like")]
        Like {
            #[serde(flatten)]
            extra: HashMap<String, serde_json::Value>,
        },

        #[serde(untagged)]
        Other {
            #[serde(flatten)]
            extra: HashMap<String, serde_json::Value>,
        },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "operation")]
    pub(crate) enum CommitOp {
        #[serde(rename = "create")]
        Create {
            rev: String,
            collection: String,
            rkey: String,
            record: Record,
            cid: String,
        },
        #[serde(rename = "update")]
        Update {
            rev: String,
            collection: String,
            rkey: String,
            record: Record,
            cid: String,
        },
        #[serde(rename = "delete")]
        Delete {
            rev: String,
            collection: String,
            rkey: String,
        },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(crate) struct Event {
        pub(crate) did: String,
        pub(crate) kind: String,
        pub(crate) time_us: i64,
        pub(crate) commit: Option<CommitOp>,
    }
}
