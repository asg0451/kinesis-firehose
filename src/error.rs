use crate::async_producer_pool::Message;
use rusoto_core::RusotoError;
use rusoto_firehose::PutRecordBatchError;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

// is there a way to get backtraces here?
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum Error {
    #[error("rusoto delivery error: {source}")]
    RusotoDelivery {
        #[from]
        source: RusotoError<PutRecordBatchError>,
    },
    #[error("record is too large ({size}): {record}")]
    RecordTooLarge { size: usize, record: String },

    #[error("ran out of attempts trying to deliver")]
    TooManyAttempts,

    #[error("pool channel send error: {source}")]
    PoolChannelSend {
        #[from]
        source: SendError<Message>,
    },

    #[error("join error: {source}")]
    Join {
        #[from]
        source: tokio::task::JoinError,
    },
    #[allow(clippy::upper_case_acronyms)]
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("pool was poisoned by: {source_str}")]
    Poisoned {
        // idk how to make this take an error
        source_str: String,
    },
}
// type Result<T> = std::result::Result<T, Error>;
