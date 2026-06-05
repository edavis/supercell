use anyhow::Result;
use tokio::sync::mpsc::Receiver;
use tokio::time::{sleep, Instant};

use crate::matcher::MatchOperation;
use crate::storage;
use crate::storage::consumer_control_insert;
use crate::storage::feed_content_write_batch;
use crate::storage::StoragePool;

// Buffered writes are flushed in a single transaction once the buffer reaches
// this size or the flush interval elapses, whichever comes first. Batching
// keeps the high-volume like stream from issuing one SQLite transaction per
// event.
const WRITE_BATCH_MAX_SIZE: usize = 1000;

// Bound on the consumer -> writer channel. Large enough that the consumer never
// blocks on `send` in normal operation; if the database falls far enough behind
// to fill it, the consumer backpressures rather than growing memory unbounded.
pub const WRITE_CHANNEL_CAPACITY: usize = 50_000;

/// Commands sent from the consumer's read loop to the writer task. Keeping all
/// database writes on this task means the consumer never blocks its websocket
/// read loop on a database transaction.
pub enum WriteCommand {
    /// A matched feed entry to upsert or update.
    Content(storage::model::FeedContent, MatchOperation),

    /// Persist the consumer cursor. Buffered writes are flushed first so the
    /// cursor never advances past writes that are not yet durable.
    Checkpoint(i64),
}

pub struct WriterTask {
    pool: StoragePool,
    source: String,
    rx: Receiver<WriteCommand>,
}

impl WriterTask {
    pub fn new(pool: StoragePool, source: String, rx: Receiver<WriteCommand>) -> Self {
        Self { pool, source, rx }
    }

    pub async fn run_background(mut self) -> Result<()> {
        tracing::debug!("WriterTask started");

        let flush_interval = std::time::Duration::from_secs(1);
        let flush_sleeper = sleep(flush_interval);
        tokio::pin!(flush_sleeper);

        let mut buffer: Vec<(storage::model::FeedContent, MatchOperation)> = Vec::new();

        loop {
            tokio::select! {
                command = self.rx.recv() => {
                    match command {
                        // The consumer has stopped and dropped the sender.
                        None => break,
                        Some(WriteCommand::Content(feed_content, op)) => {
                            buffer.push((feed_content, op));
                            if buffer.len() >= WRITE_BATCH_MAX_SIZE {
                                feed_content_write_batch(&self.pool, &buffer).await?;
                                buffer.clear();
                            }
                        }
                        Some(WriteCommand::Checkpoint(time_us)) => {
                            // Flush before advancing the persisted cursor so the
                            // cursor never moves past buffered writes.
                            feed_content_write_batch(&self.pool, &buffer).await?;
                            buffer.clear();
                            consumer_control_insert(&self.pool, &self.source, time_us).await?;
                        }
                    }
                },
                () = &mut flush_sleeper => {
                    feed_content_write_batch(&self.pool, &buffer).await?;
                    buffer.clear();
                    flush_sleeper.as_mut().reset(Instant::now() + flush_interval);
                },
            }
        }

        // Flush whatever is still buffered before shutting down.
        feed_content_write_batch(&self.pool, &buffer).await?;

        tracing::debug!("WriterTask stopped");

        Ok(())
    }
}
