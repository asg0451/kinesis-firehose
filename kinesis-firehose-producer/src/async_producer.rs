use fehler::{throw, throws};
use rusoto_core::{Region, RusotoError};
use rusoto_firehose::{KinesisFirehoseClient, PutRecordBatchError, PutRecordBatchInput};

use crate::buffer::Buffer;
use crate::error::Error;
use crate::put_record_batcher::PutRecordBatcher;

pub struct Producer<C: PutRecordBatcher> {
    buf: Buffer,
    client: C,
    stream_name: String,
}

pub type KinesisFirehoseProducer = Producer<KinesisFirehoseClient>;

impl<C: PutRecordBatcher> Producer<C> {
    // this WILL add newlines. don't add them yourself
    #[throws]
    pub async fn produce(&mut self, mut rec: String) {
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
    pub async fn flush(&mut self) {
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
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
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
                        rr.error_code.map(|_| records_to_try[i].clone())
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
    pub async fn close(&mut self) {
        // TODO: set self.closed, error to produce on it
        self.flush().await?
    }

    #[throws]
    pub fn with_client(client: C, stream_name: String) -> Self {
        Self {
            buf: Buffer::new(),
            client,
            stream_name,
        }
    }
}

impl Producer<KinesisFirehoseClient> {
    #[throws]
    pub fn new(stream_name: String) -> Self {
        Self {
            buf: Buffer::new(),
            client: KinesisFirehoseClient::new(Region::default()),
            stream_name,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::put_record_batcher::MockPutRecordBatcher;

    macro_rules! assert_err {
        ($expression:expr, $($pattern:tt)+) => {
            match $expression {
                $($pattern)+ => (),
                ref e => panic!("expected `{}` but got `{:?}`", stringify!($($pattern)+), e),
            }
        }
    }

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

    // #[test_env_log::test(tokio::test)]
    #[tokio::test]
    async fn it_retries_and_fails() {
        let mocker = MockPutRecordBatcher::with_fail_times(100);

        let mut producer = Producer::with_client(mocker, "mf-test-2".to_string()).unwrap();
        producer.produce("message 1".to_string()).await.unwrap();

        let res = producer.flush().await.expect_err("expect err");

        assert_err!(res, Error::TooManyAttempts);
    }

    // #[test_env_log::test(tokio::test)]
    #[tokio::test]
    async fn it_retries_only_failed_records() {
        let _ = env_logger::builder().is_test(true).try_init();
        let mocker = MockPutRecordBatcher::with_fail_times(1);
        let buf_ref = mocker.buf_ref();

        let mut producer = Producer::with_client(mocker, "mf-test-2".to_string()).unwrap();
        producer.produce("message 1".to_string()).await.unwrap();
        producer.produce("message 2".to_string()).await.unwrap();

        producer.flush().await.expect("failed to flush");

        let buf = buf_ref.lock().unwrap();
        let buf = buf.borrow();
        assert_eq!(buf.len(), 2);
        assert_eq!(
            buf.iter()
                .map(|r| std::str::from_utf8(&r.data).unwrap())
                .collect::<Vec<_>>(),
            // reverse order because of the error -- the first one failed, but the second went
            // through, and then we resent the first
            vec!["message 2\n".to_string(), "message 1\n".to_string()]
        );
    }
}
