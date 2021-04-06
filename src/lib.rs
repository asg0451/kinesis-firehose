//! Kinesis Firehose producer utility
//!
//! Handles buffering, retries, and limits
//!
//! Available flavours:
//!   - [`async_producer::Producer`]
//!   - [`sync_producer::Producer`]
//!   - [`async_producer_pool::AsyncProducerPool`]
//!
//! Limitations / decisions:
//!   - Adds newlines (\n) to records on the way out
//!   - Attempts to buffer the maximum amount (500 records)
//!   - Will always retry requests 10 times, waiting a constant time between attempts
//!   - Probably others
//!
//! See examples for usage.

mod buffer;
pub mod error;
mod put_record_batcher;

pub mod async_producer;
pub mod async_producer_pool;
pub mod sync_producer;
