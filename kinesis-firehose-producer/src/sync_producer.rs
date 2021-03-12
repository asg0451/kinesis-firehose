use fehler::throws;

use rusoto_firehose::KinesisFirehoseClient;
use tokio::runtime;

use crate::async_producer;
use crate::error::Error;
use crate::put_record_batcher::PutRecordBatcher;

// a synchronous buffering producer. TODO: async version?
// TODO: tests

// TODO: rewrite
// async, streams
// https://github.com/mre/futures-batch for turning a stream of incoming
//   strs into batched / times flushes
// re shutdown:
//// make users call it or shrug?
///// pub async fn close(self) -> Result<()>
//// channel nonsense
///// tokio-postgres eg https://www.reddit.com/r/rust/comments/m1t59y/what_is_the_proper_protocol_for_dropping/gqgac1y/
/////// there is a drop handler task waiting on a oneshot (tokio) channel
/////// drop: create a std oneshot channel, send its tx through the ^ other channel, blocking-recv on new channel
/////// handler: recvs, flushes, passes result back through the tx it just recvd
/////// drop: recvs, done
/////// also has to somehow make sure that no tasks are running or the stream is closed or something idk
/////// finnicky. error handling? timeout?
/////// can it even happen that we have a drop handler alive when drop is called? cause that handler will have a reference to the obj
/////// uhhhhhhhhh
//// context std BufWriter tries to flush on shutdown, swallows errors, warns in doc

// https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=d978da269ec2f069a24b81885b72ad19

// "don't block in drop" if code is to be called from async.

// where does this leave us?

pub struct Producer<C: PutRecordBatcher> {
    async_producer: async_producer::Producer<C>,
    runtime: runtime::Runtime,
}

pub type KinesisFirehoseProducer = Producer<KinesisFirehoseClient>;

impl<C: PutRecordBatcher> Producer<C> {
    #[throws]
    pub fn with_client(client: C, stream_name: String) -> Self {
        Self {
            async_producer: async_producer::Producer::with_client(client, stream_name)?,
            runtime: make_runtime()?,
        }
    }

    #[throws]
    pub fn produce(&mut self, rec: String) {
        self.runtime.block_on(self.async_producer.produce(rec))?;
    }

    #[throws]
    pub fn flush(&mut self) {
        self.runtime.block_on(self.async_producer.flush())?;
    }
}

impl Producer<KinesisFirehoseClient> {
    #[throws]
    pub fn new(stream_name: String) -> Self {
        Self {
            async_producer: async_producer::Producer::new(stream_name)?,
            runtime: make_runtime()?,
        }
    }
}

// this one does flush on drop, unlike the async one
impl<C: PutRecordBatcher> Drop for Producer<C> {
    fn drop(&mut self) {
        let res = self.flush();
        if let Err(err) = res {
            log::warn!("failed to flush producer on drop!: {}", err);
        }
    }
}

#[throws]
fn make_runtime() -> runtime::Runtime {
    runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
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
