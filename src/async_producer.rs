//! Async Producer
//!
//! You probably want to use [`KinesisFirehoseProducer`], which is a type alias of [`Producer<KinesisFirehoseClient>`]

use fehler::{throw, throws};
use rand::Rng;
use rusoto_core::{Region, RusotoError};
use rusoto_firehose::{
    KinesisFirehoseClient, PutRecordBatchError, PutRecordBatchInput, PutRecordBatchResponseEntry,
};

use crate::buffer::Buffer;
use crate::error::Error;
use crate::put_record_batcher::PutRecordBatcher;

/// The producer itself.
/// Create one with [`Producer<KinesisFirehoseClient>::new`] or [`Producer::with_client`]
pub struct Producer<C: PutRecordBatcher> {
    buf: Buffer,
    client: C,
    stream_name: String,
}

/// The type alias you'll probably want to use
pub type KinesisFirehoseProducer = Producer<KinesisFirehoseClient>;

impl Producer<KinesisFirehoseClient> {
    /// Create a Producer with a new [`KinesisFirehoseClient`] gleaned from the environment
    #[throws]
    pub fn new(stream_name: String) -> Self {
        Self {
            buf: Buffer::new(),
            client: KinesisFirehoseClient::new(Region::default()),
            stream_name,
        }
    }
}

impl<C: PutRecordBatcher> Producer<C> {
    /// Buffer a record to be sent to Kinesis Firehose. If this record fills up the buffer, it will
    /// be flushed.
    ///
    /// This function WILL add newlines to the end of each record. Don't add them yourself.
    #[throws]
    pub async fn produce(&mut self, mut rec: String) {
        rec += "\n";
        self.buf.check_record_too_large(&rec)?;

        if self.buf.is_overfull_with(&rec) {
            self.flush().await?;
        }
        self.buf.add_rec(rec);
    }

    /// Make the producer flush its buffer.
    ///
    /// You MUST flush before dropping / when you're done
    /// otherwise buffered data will be lost. Blame the lack of async Drop.
    ///
    /// You may also want to do this on a timer if your data comes in slowly.
    // TODO: make us do that or something somehow
    // due to a bug in fehler (fixed on master but not in release) this warns unreachable_code
    #[allow(unreachable_code)]
    #[throws]
    pub async fn flush(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let recs = self.buf.as_owned_vec();
        let mut records_to_try = recs;

        let max_attempts = 10; // TODO: this should be a parameter. config? buf size too
        let mut attempts_left: i32 = max_attempts;

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
                let dur = Self::sleep_duration(max_attempts - attempts_left);
                log::debug!(
                    "service unavailable: {}. sleeping for {} millis & retrying",
                    err,
                    dur.as_millis()
                );
                tokio::time::sleep(dur).await;
                continue;
            }

            let res = res?;

            if res.failed_put_count > 0 {
                log::debug!(
                    "{} partial errors. first: {:?}",
                    res.failed_put_count,
                    res.request_responses
                        .iter()
                        .find(|rr| rr.error_code.is_some())
                );

                let rrs = res.request_responses;
                // there could be a partial error that is a throttling error
                let first_throttling_error = rrs
                    .iter()
                    .find(|rr| Self::response_entry_is_throttling_error(rr));

                if let Some(first_throttling_error) = first_throttling_error {
                    let dur = Self::sleep_duration(max_attempts - attempts_left);
                    log::debug!(
                        "service unavailable: {:?}. sleeping for {} millis & retrying",
                        first_throttling_error,
                        dur.as_millis()
                    );
                    tokio::time::sleep(dur).await;
                    continue;
                }

                records_to_try = rrs
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

    /// Create a Producer with an existing [`KinesisFirehoseClient`]
    #[throws]
    pub fn with_client(client: C, stream_name: String) -> Self {
        Self {
            buf: Buffer::new(),
            client,
            stream_name,
        }
    }

    fn sleep_duration(attempt: i32) -> tokio::time::Duration {
        let mut rng = rand::thread_rng();
        let rand_factor = 0.5;
        let absolute_max = 10_000; // 10 sec

        let ivl = (1. + (attempt as f64).powf(1.15)) * 100.;
        let rand_ivl = ivl * (rng.gen_range((1. - rand_factor)..(1. + rand_factor)));
        let millis = (rand_ivl.ceil() as u64).min(absolute_max);
        tokio::time::Duration::from_millis(millis)
    }

    fn response_entry_is_throttling_error(e: &PutRecordBatchResponseEntry) -> bool {
        matches!(
            e.error_code.as_ref().map(AsRef::as_ref),
            Some("ServiceUnavailableException")
        )
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

    #[tokio::test]
    async fn it_uses_exponential_backoff_for_req_errors() {
        let _ = env_logger::builder().is_test(true).try_init();
        let mocker = MockPutRecordBatcher::with_svc_and_fail_times(5, 0);

        let mut producer = Producer::with_client(mocker, "mf-test-2".to_string()).unwrap();
        producer.produce("message 1".to_string()).await.unwrap();

        let start = tokio::time::Instant::now();
        producer.flush().await.expect("should eventually succeed");
        let end = tokio::time::Instant::now();

        assert!(end - start > tokio::time::Duration::from_millis(200));
    }

    #[tokio::test]
    async fn it_uses_exponential_backoff_for_partial_errors() {
        let _ = env_logger::builder().is_test(true).try_init();
        let mocker = MockPutRecordBatcher::with_all(5, 0, true);

        let mut producer = Producer::with_client(mocker, "mf-test-2".to_string()).unwrap();
        producer.produce("message 1".to_string()).await.unwrap();
        producer.produce("message 2".to_string()).await.unwrap();

        let start = tokio::time::Instant::now();
        producer.flush().await.expect("should eventually succeed");
        let end = tokio::time::Instant::now();

        assert!(end - start > tokio::time::Duration::from_millis(200));
    }
}
