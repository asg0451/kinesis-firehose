use fehler::{throw, throws};
use rusoto_core::{Region, RusotoError};
use rusoto_firehose::{KinesisFirehoseClient, PutRecordBatchError, PutRecordBatchInput};

use crate::buffer::Buffer;
use crate::error::Error;
use crate::put_record_batcher::PutRecordBatcher;

// this split is solely so producer.drop can work
// but we're no longer doing that, so they can be collapsed again
// alternatively, inner.is_none could -> closed.
// TODO
struct InnerProducer<C: PutRecordBatcher> {
    buf: Buffer,
    client: C,
    stream_name: String,
}

pub struct Producer<C: PutRecordBatcher> {
    inner: Option<InnerProducer<C>>,
}

pub type KinesisFirehoseProducer = Producer<KinesisFirehoseClient>;

impl<C: PutRecordBatcher> InnerProducer<C> {
    // this WILL add newlines. don't add them yourself
    #[throws]
    async fn produce(&mut self, mut rec: String) {
        rec += "\n";
        self.buf.check_record_too_large(&rec)?;

        if self.buf.is_overfull_with(&rec) {
            self.flush().await?;
        }
        self.buf.add_rec(rec);
    }

    // due to a bug in fehler (fixed on master but not in release) this warns unreachable_code
    #[allow(unreachable_code)]
    #[throws]
    async fn flush(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let recs = self.buf.as_owned_vec();
        let mut records_to_try = recs;
        let mut attempts_left: i32 = 10;

        while attempts_left > 0 {
            log::trace!("flushing; attempts_left: {}", attempts_left);
            attempts_left -= 1;

            let req = PutRecordBatchInput {
                delivery_stream_name: self.stream_name.clone(),
                // Bytes are supposed to be cheap to clone somehow. idk if theres a better way given
                // the interface
                records: records_to_try.clone(),
            };

            let res = self.client._put_record_batch(req).await;

            if let Err(RusotoError::Service(PutRecordBatchError::ServiceUnavailable(err))) = res {
                // back off and retry the whole request
                // TODO: is this a param? more intricate logic based on attempt num?
                log::debug!("service unavailable: {}. sleeping & retrying", err);
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }

            let res = res?;

            if res.failed_put_count > 0 {
                records_to_try = res
                    .request_responses
                    .into_iter()
                    .enumerate()
                    .filter_map(|(i, rr)| {
                        // TODO: how can this be better
                        rr.error_code.and_then(|_| Some(records_to_try[i].clone()))
                    })
                    .collect::<Vec<_>>();
                continue;
            }

            // success
            self.buf.clear();
            return;
        }

        throw!(Error::TooManyAttempts);
    }

    #[throws]
    async fn close(&mut self) {
        // TODO: set self.closed, error to produce on it
        self.flush().await?
    }
}

// this is 'static cause of a compiler error ofc. but is it a bad thing?
// we're only generic over the type of the rusoto client, and that can live forever i guess...?
// therefore we dont have Drop flush on this async producer
// therefore we dont need the 'static lifetime on C
impl<C: PutRecordBatcher> Producer<C> {
    #[throws]
    pub fn with_client(client: C, stream_name: String) -> Self {
        Self {
            inner: Some(InnerProducer {
                buf: Buffer::new(),
                client,
                stream_name,
            }),
        }
    }

    fn inner_mut(&mut self) -> &mut InnerProducer<C> {
        let ret = self
            .inner
            .as_mut()
            .expect("unexpectedly don't have an InnerProducer");
        ret
    }

    #[throws]
    pub async fn produce(&mut self, rec: String) {
        self.inner_mut().produce(rec).await?
    }

    #[throws]
    pub async fn flush(&mut self) {
        self.inner_mut().flush().await?
    }

    #[throws]
    pub async fn close(&mut self) {
        // TODO: set self.closed, error to produce on it
        self.inner_mut().close().await?
    }

    // NOTE: in a single-threaded tokio runtime this just hangs forever as the main thread is blocked
    // in rx.recv. therefore don't use this!!
    // fn sync_close(&mut self) {
    //     use std::sync::mpsc::channel;

    //     println!("sync_close");
    //     let (tx, rx) = channel();

    //     let mut inner = self
    //         .inner
    //         .take()
    //         .expect("failed to take InnerProducer in sync_close");

    //     println!("sync_close has inner");

    //     tokio::spawn(async move {
    //         println!("sync_close spawned");
    //         let res = inner.close().await;
    //         println!("sync_close spawned awaited");
    //         if let Err(err) = res {
    //             log::warn!("error synchronously closing: {}", err);
    //         }
    //         tx.send(()).expect("failed to send on conf channel");
    //     });
    //     println!("sync_close about to recv ");

    //     rx.recv().expect("failed to recv on conf channel");
    //     println!("/sync_close");
    // }
}

impl Producer<KinesisFirehoseClient> {
    #[throws]
    pub fn new(stream_name: String) -> Self {
        Self {
            inner: Some(InnerProducer {
                buf: Buffer::new(),
                client: KinesisFirehoseClient::new(Region::default()),
                stream_name,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::put_record_batcher::MockPutRecordBatcher;

    #[tokio::test] // #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_works_or_something() {
        let mocker = MockPutRecordBatcher::new();
        let buf_ref = mocker.buf_ref();

        let mut producer = Producer::with_client(mocker, "mf-test-2".to_string()).unwrap();
        producer.produce("hi".to_string()).await.unwrap();

        producer.flush().await.expect("flush pls");
        let len = {
            let buf = buf_ref.lock().unwrap();
            let buf = buf.borrow();
            buf.len()
        };

        assert_eq!(len, 1);
    }
}
