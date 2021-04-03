mod buffer;
mod error;
mod put_record_batcher;

pub mod async_producer;
pub mod async_producer_pool;
pub mod sync_producer;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
