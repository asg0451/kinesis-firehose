use rusoto_core::RusotoError;
use rusoto_firehose::PutRecordBatchError;

use thiserror::Error;

// is there a way to get backtraces here?
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

    #[allow(clippy::upper_case_acronyms)]
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
}
// type Result<T> = std::result::Result<T, Error>;
