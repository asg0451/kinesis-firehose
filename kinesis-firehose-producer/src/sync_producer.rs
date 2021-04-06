//! Sync Producer
//!
//! This is just a wrapper around [`async_producer::Producer`] that creates a new Tokio [`Runtime`]
//! and wraps method calls in [`Runtime::block_on`].
//!
//! You probably want to use [`KinesisFirehoseProducer`], which is a type alias of [`Producer<KinesisFirehoseClient>`]

use fehler::throws;

use rusoto_firehose::KinesisFirehoseClient;
use tokio::runtime::{Builder, Runtime};

use crate::async_producer;
use crate::error::Error;
use crate::put_record_batcher::PutRecordBatcher;

/// The producer itself.
/// Create one with [`Producer<KinesisFirehoseClient>::new`] or [`Producer::with_client`]
///
/// Unlike the Async Producer, this one WILL flush on [`Drop`], so you don't need to flush it manually
/// Maybe you still should, though, in case it fails.
pub struct Producer<C: PutRecordBatcher> {
    async_producer: async_producer::Producer<C>,
    runtime: Runtime,
}

/// The type alias you'll probably want to use
pub type KinesisFirehoseProducer = Producer<KinesisFirehoseClient>;

impl Producer<KinesisFirehoseClient> {
    /// Create a Producer with a new [`KinesisFirehoseClient`] gleaned from the environment
    #[throws]
    pub fn new(stream_name: String) -> Self {
        Self {
            async_producer: async_producer::Producer::new(stream_name)?,
            runtime: make_runtime()?,
        }
    }
}

impl<C: PutRecordBatcher> Producer<C> {
    /// Buffer a record to be sent to Kinesis Firehose. If this record fills up the buffer, it will
    /// be flushed.
    ///
    /// This function WILL add newlines to the end of each record. Don't add them yourself.
    #[throws]
    pub fn produce(&mut self, rec: String) {
        self.runtime.block_on(self.async_producer.produce(rec))?;
    }

    /// Make the producer flush its buffer.
    #[throws]
    pub fn flush(&mut self) {
        self.runtime.block_on(self.async_producer.flush())?;
    }

    /// Create a Producer with an existing [`KinesisFirehoseClient`]
    #[throws]
    pub fn with_client(client: C, stream_name: String) -> Self {
        Self {
            async_producer: async_producer::Producer::with_client(client, stream_name)?,
            runtime: make_runtime()?,
        }
    }
}

/// Flush on Drop
impl<C: PutRecordBatcher> Drop for Producer<C> {
    fn drop(&mut self) {
        let res = self.flush();
        if let Err(err) = res {
            log::warn!("failed to flush producer on drop!: {}", err);
        }
    }
}

#[throws]
fn make_runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build()?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::put_record_batcher::MockPutRecordBatcher;

    #[test]
    fn it_works_or_something() {
        let mocker = MockPutRecordBatcher::new();
        let buf_ref = mocker.buf_ref();

        let mut producer = Producer::with_client(mocker, "mf-test-2".to_string()).unwrap();
        producer.produce("hi".to_string()).unwrap();

        producer.flush().expect("flush pls");
        let len = {
            let buf = buf_ref.lock().unwrap();
            let buf = buf.borrow();
            buf.len()
        };

        assert_eq!(len, 1);
    }

    #[test]
    fn it_flushes_on_drop() {
        let mocker = MockPutRecordBatcher::new();
        let buf_ref = mocker.buf_ref();

        let len = {
            let buf = buf_ref.lock().unwrap();
            let buf = buf.borrow();
            buf.len()
        };

        assert_eq!(len, 0);
        {
            let mut producer = Producer::with_client(mocker, "mf-test-2".to_string()).unwrap();
            producer.produce("hi".to_string()).unwrap();
            drop(producer);
        }

        let len = {
            let buf = buf_ref.lock().unwrap();
            let buf = buf.borrow();
            buf.len()
        };

        assert_eq!(len, 1);
    }
}
