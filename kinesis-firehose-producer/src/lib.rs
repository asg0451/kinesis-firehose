mod buffer;
mod error;
mod producer;

mod put_record_batcher;

pub use producer::Producer;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
