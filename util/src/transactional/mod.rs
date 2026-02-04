pub mod btreemap;
pub mod hashmap;
pub mod option;
pub mod value;

pub trait Transactional {
    fn begin(&mut self);
    fn commit(&mut self);
    fn rollback(&mut self);
    fn in_tx(&self) -> bool;
}
