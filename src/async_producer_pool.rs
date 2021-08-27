//! A Pool of Async Producers
//!
//! This has a much higher throughput than a lone Producer, but at the cost of
//! non-local / trickier errors.

use crate::async_producer::Producer;
use crate::error::Error;
use crate::put_record_batcher::PutRecordBatcher;
use fehler::{throw, throws};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::JoinHandle;

#[cfg(test)]
use crate::put_record_batcher::{BufRef, MockPutRecordBatcher};

/// The internal type of messages sent around the pool
#[non_exhaustive]
#[derive(Debug)]
pub enum Message {
    Produce(String),
    Flush,
    Close,
}

#[derive(Debug)]
struct Member {
    tx: Sender<Message>,
    handle: JoinHandle<()>,
}

/// The Producer Pool.
/// Create one with [`AsyncProducerPool::of_size`]
///
/// # Panics
///
/// This can panic at pretty much any point if a member Producer throws an error.
/// Currently there is an error channel which waits for errors and panics, but
/// we could do something else here? If we get in that state, though, it's likely
/// the situation won't benefit from additional retries. So what else would we do?
#[derive(Debug)]
pub struct AsyncProducerPool {
    things: Vec<Member>,
    send_to_next: usize,
    err_watcher: JoinHandle<()>,
    shutdown: AtomicBool,
    poisoned: Arc<AtomicBool>,
    poison_err: Arc<Mutex<Option<Error>>>,
}

impl AsyncProducerPool {
    /// Create a pool of size `size`
    #[throws]
    pub async fn of_size(stream_name: String, size: usize) -> Self {
        assert!(size > 0);
        Self::of_size_with_make_producer(stream_name, size, Producer::new).await?
    }

    /// Create a pool of size `size` with a given client
    /// The client will be cloned for each pool member
    ///
    /// # Panics
    ///
    /// If size < 1
    #[throws]
    pub async fn of_size_with_client(
        stream_name: String,
        size: usize,
        client: rusoto_firehose::KinesisFirehoseClient,
    ) -> Self {
        assert!(size > 0);
        Self::of_size_with_make_producer(stream_name, size, |sn| {
            Producer::with_client(client.clone(), sn)
        })
        .await?
    }

    // this type nonsense is just so we can substitute a mock producer in tests
    #[throws]
    async fn of_size_with_make_producer<
        C: PutRecordBatcher + 'static,
        F: Fn(String) -> Result<Producer<C>, Error>,
    >(
        stream_name: String,
        size: usize,
        make_producer: F,
    ) -> Self {
        let poisoned = Arc::new(AtomicBool::new(false));
        let poison_err = Arc::new(Mutex::new(None));

        let (err_tx, mut err_rx) = channel::<Error>(1);
        let things = stream::iter(0..size)
            .then(|n| Self::create_member(stream_name.clone(), err_tx.clone(), n, &make_producer))
            .try_collect()
            .await?;
        // monitor err chan for errors. if there is one, panic
        // could attempt to flush or something, but at this point... just die
        let poison_err2 = Arc::clone(&poison_err);
        let poisoned2 = Arc::clone(&poisoned);
        let err_watcher = tokio::spawn(async move {
            log::trace!("err watcher set up");
            if let Some(err) = err_rx.recv().await {
                log::error!("err watcher got something: {:?}", err);
                poison_err2
                    .lock()
                    .expect("poison_err mutex is poisoned!")
                    .insert(err);
                poisoned2.store(true, Ordering::SeqCst);
                log::trace!("poison_err set");
            }
            log::trace!("err watcher exiting");
        });
        Self {
            things,
            send_to_next: 0,
            err_watcher,
            shutdown: AtomicBool::new(false),
            poison_err,
            poisoned,
        }
    }

    /// Produce a message; send a string to a producer. Panics if the pool has been shutdown
    #[throws]
    pub async fn produce(&mut self, rec: String) {
        if self.poisoned.load(Ordering::SeqCst) {
            log::debug!("produce was poisoned!");
            throw!(Error::Poisoned {
                source_str: self.poison_err_msg()
            })
        }
        if self.shutdown.load(Ordering::SeqCst) {
            panic!("pool has been shutdown");
        }
        let m = self.things.get(self.send_to_next).unwrap();
        m.tx.send(Message::Produce(rec)).await?;
        self.send_to_next = (self.send_to_next + 1) % self.things.len();
    }

    /// Queue a flush for each of the members. Doesn't wait for them to finish
    // TODO: could do this with oneshots but who cares
    #[throws]
    pub async fn flush(&mut self) {
        if self.poisoned.load(Ordering::SeqCst) {
            throw!(Error::Poisoned {
                source_str: self.poison_err_msg()
            })
        }
        for Member { tx, .. } in self.things.iter() {
            log::trace!("flushing member");
            tx.send(Message::Flush)
                .await
                .expect("failed to send flush message");
        }
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::SeqCst)
    }

    /// Shut down the pool.
    /// You MUST call this when you're done, otherwise we'll drop data.
    // thank rust's lack of async drop
    // this closes the pool-members serially for simplicity's sake
    #[throws]
    pub async fn shutdown(&mut self) {
        for Member { tx, handle, .. } in self.things.iter_mut() {
            log::trace!("closing member");
            tx.send(Message::Close)
                .await
                .expect("failed to send close message");
            tx.closed().await;
            handle.await?;
            log::trace!("closed member");
        }
        (&mut self.err_watcher).await?;
        self.shutdown.store(true, Ordering::SeqCst);

        if self.poisoned.load(Ordering::SeqCst) {
            throw!(Error::Poisoned {
                source_str: self.poison_err_msg()
            })
        }

        log::debug!("shutdown");
    }

    #[throws]
    async fn create_member<
        C: PutRecordBatcher + 'static,
        F: Fn(String) -> Result<Producer<C>, Error>,
    >(
        stream_name: String,
        err_tx: Sender<Error>,
        num: usize,
        make_producer: F,
    ) -> Member {
        let (tx, mut rx) = channel(100);
        let mut p: Producer<C> = make_producer(stream_name)?;
        let handle = tokio::spawn(async move {
            log::trace!("creating producer {}", num);
            while let Some(msg) = rx.recv().await {
                log::trace!("producer {} got msg: {:?}", num, msg);
                if let Message::Produce(rec) = msg {
                    let res = p.produce(rec).await;
                    if let Err(err) = res {
                        // ignore errors sending errors
                        log::trace!("member {} gonna send error from produce: {:?}", num, &err);
                        let _ = err_tx.send(err).await;
                        break;
                    }
                } else if let Message::Flush = msg {
                    let res = p.flush().await;
                    if let Err(err) = res {
                        log::trace!("member {} gonna send error from flush: {:?}", num, &err);
                        let _ = err_tx.send(err).await;
                        break;
                    }
                } else if let Message::Close = msg {
                    rx.close();
                    let res = p.flush().await;
                    if let Err(err) = res {
                        log::trace!("member {} gonna send error from close: {:?}", num, &err);
                        let _ = err_tx.send(err).await;
                    }
                    break;
                }
            }
        });

        Member { tx, handle }
    }

    fn poison_err_msg(&self) -> String {
        // ignore poisoned mutex
        let err = self.poison_err.lock().unwrap_or_else(|e| e.into_inner());
        let err = err.as_ref().expect("poison_err should have an err");
        format!("{:?}", err)
    }

    #[cfg(test)]
    #[throws]
    async fn of_size_mocked(
        stream_name: String,
        size: usize,
        fail_times: i64,
    ) -> (Self, Arc<Mutex<Vec<BufRef>>>) {
        use std::sync::{Arc, Mutex};
        let bufs = Arc::new(Mutex::new(Vec::with_capacity(size)));
        let b2 = bufs.clone();
        let pool = Self::of_size_with_make_producer(stream_name, size, move |sn| {
            let bufs = b2.clone();
            let mut bufs = bufs.lock().unwrap();
            let mocker = MockPutRecordBatcher::with_fail_times(fail_times);
            let buf_ref = mocker.buf_ref();
            bufs.push(buf_ref);
            Producer::with_client(mocker, sn)
        })
        .await?;
        (pool, bufs)
    }
}

impl Drop for AsyncProducerPool {
    fn drop(&mut self) {
        if !self.shutdown.load(Ordering::SeqCst) {
            log::error!("async pool being dropped without being shut down! data will be lost");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bufs_contents(buf_refs: Arc<Mutex<Vec<BufRef>>>) -> Vec<String> {
        let buf_refs = buf_refs.lock().unwrap();
        buf_refs
            .iter()
            .map(|b| {
                let buf = b.lock().unwrap();
                let buf = buf.borrow();
                let strs = buf
                    .clone()
                    .iter()
                    .map(|r| std::str::from_utf8(&r.data).unwrap().to_string())
                    .collect::<Vec<_>>();
                strs
            })
            .flatten()
            .collect::<Vec<_>>()
    }

    #[tokio::test]
    async fn it_works() {
        let (mut pool, buf_refs) = AsyncProducerPool::of_size_mocked("mf-test-2".to_string(), 2, 0)
            .await
            .unwrap();

        pool.produce("hello".to_string()).await.unwrap();
        pool.produce("world".to_string()).await.unwrap();
        pool.shutdown().await.unwrap();

        let data = bufs_contents(buf_refs);
        assert_eq!(&data, &vec!["hello\n".to_string(), "world\n".to_string()])
    }

    // #[should_panic]
    // #[tokio::test]
    // async fn it_panics_if_it_cant_flush() {
    //     let (mut pool, _buf_refs) =
    //         AsyncProducerPool::of_size_mocked("mf-test-2".to_string(), 2, 11)
    //             .await
    //             .unwrap();

    //     pool.produce("hello".to_string()).await.unwrap();
    //     pool.produce("world".to_string()).await.unwrap();

    //     pool.shutdown().await.unwrap();
    // }

    #[tokio::test]
    async fn it_poisons_on_shutdown_if_it_cant_flush() {
        let _ = env_logger::builder().is_test(true).try_init();
        let (mut pool, _buf_refs) =
            AsyncProducerPool::of_size_mocked("mf-test-2".to_string(), 2, 11)
                .await
                .unwrap();

        pool.produce("hello".to_string()).await.unwrap();
        pool.produce("world".to_string()).await.unwrap();

        let res = pool.shutdown().await;
        assert!(matches!(res, Err(Error::Poisoned { .. })));
    }

    #[tokio::test]
    async fn it_poisons_on_produce_eventually_if_it_cant_flush() {
        let _ = env_logger::builder().is_test(true).try_init();
        let (mut pool, _buf_refs) =
            AsyncProducerPool::of_size_mocked("mf-test-2".to_string(), 2, i64::MAX)
                .await
                .unwrap();

        let mut err = None;
        for i in 0..10_000i64 {
            let res = pool.produce(format!("{}", i)).await;
            if let Err(e) = res {
                err = Some(e);
                break;
            }
        }

        // this could be a PoolSendError -- since the produce failed to send to a live member
        assert!(err.is_some(), "it failed at some point");
        dbg!(&err);

        // subsequent produces should return the poison error
        let res = pool.produce("should be poisoned".to_string()).await;
        assert!(matches!(res, Err(Error::Poisoned { .. })));
        dbg!(&res);

        assert!(pool.is_poisoned());
    }
}
