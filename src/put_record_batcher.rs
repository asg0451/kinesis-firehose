use async_trait::async_trait;
use rusoto_core::RusotoError;
use rusoto_firehose::{
    KinesisFirehose, KinesisFirehoseClient, PutRecordBatchError, PutRecordBatchInput,
    PutRecordBatchOutput,
};

#[cfg(test)]
use rusoto_firehose::{PutRecordBatchResponseEntry, Record};

#[cfg(test)]
use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
};

#[async_trait]
pub trait PutRecordBatcher: Send + Sync {
    async fn _put_record_batch(
        &self,
        req: PutRecordBatchInput,
    ) -> Result<PutRecordBatchOutput, RusotoError<PutRecordBatchError>>;
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct MockPutRecordBatcher {
    pub(crate) buf: Arc<Mutex<RefCell<Vec<Record>>>>,
    pub(crate) fail_times: Arc<Mutex<RefCell<i64>>>,
    pub(crate) svc_fail_times: Arc<Mutex<RefCell<i64>>>,
    pub(crate) partial_failures_throttled: bool,
}

#[cfg(test)]
impl MockPutRecordBatcher {
    pub(crate) fn new() -> Self {
        Self {
            buf: Arc::new(Mutex::new(RefCell::new(vec![]))),
            fail_times: Arc::new(Mutex::new(RefCell::new(0))),
            svc_fail_times: Arc::new(Mutex::new(RefCell::new(0))),
            partial_failures_throttled: false,
        }
    }

    pub(crate) fn with_fail_times(ft: i64) -> Self {
        Self {
            buf: Arc::new(Mutex::new(RefCell::new(vec![]))),
            fail_times: Arc::new(Mutex::new(RefCell::new(ft))),
            svc_fail_times: Arc::new(Mutex::new(RefCell::new(0))),
            partial_failures_throttled: false,
        }
    }

    pub(crate) fn with_svc_and_fail_times(sft: i64, ft: i64) -> Self {
        Self {
            buf: Arc::new(Mutex::new(RefCell::new(vec![]))),
            fail_times: Arc::new(Mutex::new(RefCell::new(ft))),
            svc_fail_times: Arc::new(Mutex::new(RefCell::new(sft))),
            partial_failures_throttled: false,
        }
    }

    pub(crate) fn with_all(
        fail_times: i64,
        svc_fail_times: i64,
        partial_failures_throttled: bool,
    ) -> Self {
        Self {
            buf: Arc::new(Mutex::new(RefCell::new(vec![]))),
            fail_times: Arc::new(Mutex::new(RefCell::new(fail_times))),
            svc_fail_times: Arc::new(Mutex::new(RefCell::new(svc_fail_times))),
            partial_failures_throttled,
        }
    }

    pub(crate) fn buf_ref(&self) -> Arc<Mutex<RefCell<Vec<Record>>>> {
        self.buf.clone()
    }
}

#[cfg(test)]
pub(crate) type BufRef = Arc<Mutex<RefCell<Vec<Record>>>>;

#[cfg(test)]
#[async_trait]
impl PutRecordBatcher for MockPutRecordBatcher {
    async fn _put_record_batch(
        &self,
        req: PutRecordBatchInput,
    ) -> Result<PutRecordBatchOutput, RusotoError<PutRecordBatchError>> {
        let records = req.records;

        let buf = self.buf.lock().expect("poisoned mutex");
        let mut buf = buf.borrow_mut();

        let svc_fail_times = self.svc_fail_times.lock().expect("poisoned mutex");
        let mut svc_fail_times = svc_fail_times.borrow_mut();

        if *svc_fail_times > 0 {
            *svc_fail_times -= 1;
            return Err(RusotoError::Service(
                PutRecordBatchError::ServiceUnavailable("svc exc".to_string()),
            ));
        }

        let fail_times = self.fail_times.lock().expect("poisoned mutex");
        let mut fail_times = fail_times.borrow_mut();

        // a nice successful response
        let rrs = records
            .iter()
            .map(|r| {
                log::trace!("fail_times: {}", fail_times);
                // TODO: realistic values
                if *fail_times > 0 {
                    *fail_times -= 1;
                    let error_code = if self.partial_failures_throttled {
                        "ServiceUnavailableException"
                    } else {
                        "some other error code"
                    };
                    PutRecordBatchResponseEntry {
                        error_code: Some(error_code.to_string()),
                        error_message: Some("some error message".to_string()),
                        record_id: Some("wa".to_string()),
                    }
                } else {
                    buf.push(r.clone());
                    PutRecordBatchResponseEntry {
                        error_code: None,
                        error_message: None,
                        record_id: Some("wa".to_string()),
                    }
                }
            })
            .collect::<Vec<_>>();
        let resp = PutRecordBatchOutput {
            encrypted: None,
            failed_put_count: rrs.iter().filter(|r| r.error_code.is_some()).count() as i64,
            request_responses: rrs,
        };
        Ok(resp)
    }
}

#[async_trait]
impl PutRecordBatcher for KinesisFirehoseClient {
    async fn _put_record_batch(
        &self,
        req: PutRecordBatchInput,
    ) -> Result<PutRecordBatchOutput, RusotoError<PutRecordBatchError>> {
        self.put_record_batch(req).await
    }
}
