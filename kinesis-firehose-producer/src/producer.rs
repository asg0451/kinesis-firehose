use fehler::{throw, throws};
use rusoto_core::{Region, RusotoError};
use rusoto_firehose::{
    KinesisFirehose, KinesisFirehoseClient, PutRecordBatchError, PutRecordBatchInput,
};
use tokio::runtime;

use crate::buffer::Buffer;
use crate::error::Error;

// a synchronous buffering producer. TODO: async version?
// TODO: generic over client so we can sub in a test mock
// TODO: tests
pub struct Producer {
    buf: Buffer,
    client: KinesisFirehoseClient,
    stream_name: String,
    runtime: runtime::Runtime,
}

impl Producer {
    #[throws]
    pub fn new(stream_name: String) -> Self {
        Self {
            buf: Buffer::new(),
            client: KinesisFirehoseClient::new(Region::default()),
            stream_name,
            runtime: runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
        }
    }

    #[throws]
    pub fn with_aws_region_client(region: Region, stream_name: String) -> Self {
        Self {
            buf: Buffer::new(),
            client: KinesisFirehoseClient::new(region),
            stream_name,
            runtime: runtime::Builder::new_current_thread().build()?,
        }
    }

    #[throws]
    pub fn with_client(client: KinesisFirehoseClient, stream_name: String) -> Self {
        Self {
            buf: Buffer::new(),
            client,
            stream_name,
            runtime: runtime::Builder::new_current_thread().build()?,
        }
    }

    // this WILL add newlines. don't add them yourself
    #[throws]
    pub fn produce(&mut self, mut rec: String) {
        rec += "\n";
        self.buf.check_record_too_large(&rec)?;

        if self.buf.is_overfull_with(&rec) {
            self.flush()?;
        }
        self.buf.add_rec(rec);
    }

    // due to a bug in fehler (fixed on master but not in release) this warns unreachable_code
    #[allow(unreachable_code)]
    #[throws]
    fn flush(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let recs = self.buf.as_owned_vec();
        let mut records_to_try = recs;
        let mut attempts_left = 10;

        while attempts_left > 0 {
            log::trace!("flushing; attempts_left: {}", attempts_left);
            attempts_left -= 1;

            let req = PutRecordBatchInput {
                delivery_stream_name: self.stream_name.clone(),
                // Bytes are supposed to be cheap to clone somehow. idk if theres a better way given
                // the interface
                records: records_to_try.clone(),
            };

            let res = self.runtime.block_on(self.client.put_record_batch(req));

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

        throw!(Error::TooManyAttempts)
    }
}
impl Drop for Producer {
    fn drop(&mut self) {
        let res = self.flush();
        if let Err(err) = res {
            log::warn!("failed to flush producer on drop!: {}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
