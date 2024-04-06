use crate::{error::Result, hash::Hash};

pub trait Entity {
    const NAME: &'static str;
    const KEY_PREFIX: &'static str;
}

pub trait EntityRecord<E: Entity> {
    fn size(&self) -> u64;
}

pub trait EntityIndex<E: Entity> {
    type Record: EntityRecord<E>;

    const KEY: &'static str;

    fn len(&self) -> usize;
    fn contains(&self, hash: &Hash<E>) -> bool;
    fn get(&self, hash: &Hash<E>) -> Option<&Self::Record>;
    fn get_mut(&mut self, hash: &Hash<E>) -> Option<&mut Self::Record>;
    fn insert(&mut self, hash: Hash<E>, record: Self::Record);
    fn remove(&mut self, hash: &Hash<E>) -> Result<Self::Record>;
}
