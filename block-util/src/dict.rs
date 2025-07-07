use tycho_types::cell::Lazy;
use tycho_types::dict::{
    AugDictExtra, DictKey, PartialSplitDict, SetMode, aug_dict_insert, aug_dict_merge_siblings,
    aug_dict_modify_from_sorted_iter, aug_dict_remove_owned, build_aug_dict_from_sorted_iter,
    dict_split_raw,
};
use tycho_types::error::Error;
use tycho_types::models::ShardIdent;
use tycho_types::prelude::*;
use tycho_util::FastHashMap;

pub struct RelaxedAugDict<K, A, V> {
    dict_root: Option<Cell>,
    _marker: std::marker::PhantomData<(K, A, V)>,
}

impl<K, A, V> Default for RelaxedAugDict<K, A, V> {
    fn default() -> Self {
        Self {
            dict_root: None,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<K, A: Default, V> RelaxedAugDict<K, A, V> {
    pub const fn new() -> Self {
        Self {
            dict_root: None,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn from_full(dict: &AugDict<K, A, V>) -> Self {
        Self {
            dict_root: dict.dict().clone().into_root(),
            _marker: std::marker::PhantomData,
        }
    }

    pub const fn is_empty(&self) -> bool {
        self.dict_root.is_none()
    }
}

impl<K, A, V> RelaxedAugDict<K, A, V>
where
    K: StoreDictKey + DictKey,
    for<'a> A: AugDictExtra + Store + Load<'a>,
{
    pub fn try_from_sorted_iter_lazy<'a, I>(iter: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (&'a K, &'a A, &'a Lazy<V>)>,
        K: StoreDictKey + Ord + 'a,
        A: 'a,
        V: 'a,
    {
        Ok(Self {
            dict_root: build_aug_dict_from_sorted_iter(
                iter.into_iter().map(|(k, a, v)| {
                    // SAFETY: We know that this cell is not a library cell.
                    let value = v.inner().as_slice_allow_exotic();
                    (k, a, value)
                }),
                A::comp_add,
                Cell::empty_context(),
            )?,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn try_from_sorted_iter_any<I>(iter: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (K, A, V)>,
        K: StoreDictKey + Ord,
        V: Store,
    {
        Ok(Self {
            dict_root: build_aug_dict_from_sorted_iter(iter, A::comp_add, Cell::empty_context())?,
            _marker: std::marker::PhantomData,
        })
    }

    /// Applies a sorted list of inserts/removes to the dictionary.
    /// Use this when you have a large set of known changes.
    ///
    /// Uses custom extracts for values.
    pub fn modify_with_sorted_iter<I>(&mut self, entries: I) -> Result<bool, Error>
    where
        I: IntoIterator<Item = (K, Option<(A, V)>)>,
        K: Clone + StoreDictKey + Ord,
        V: Store,
    {
        aug_dict_modify_from_sorted_iter(
            &mut self.dict_root,
            entries,
            |(key, _)| key.clone(),
            |(_, value)| Ok(value),
            A::comp_add,
            Cell::empty_context(),
        )
    }

    pub fn set_as_lazy(&mut self, key: &K, extra: &A, value: &Lazy<V>) -> Result<bool, Error> {
        self.set_any(key, extra, &value.inner().as_slice()?)
    }

    pub fn set_any(&mut self, key: &K, extra: &A, value: &dyn Store) -> Result<bool, Error> {
        let mut key_builder = CellDataBuilder::new();
        key.store_into_data(&mut key_builder)?;

        aug_dict_insert(
            &mut self.dict_root,
            &mut key_builder.as_data_slice(),
            K::BITS,
            extra,
            value,
            SetMode::Set,
            A::comp_add,
            Cell::empty_context(),
        )
    }

    pub fn remove(&mut self, key: &K) -> Result<bool, Error> {
        let mut key_builder = CellDataBuilder::new();
        key.store_into_data(&mut key_builder)?;

        let removed = aug_dict_remove_owned(
            &mut self.dict_root,
            &mut key_builder.as_data_slice(),
            K::BITS,
            false,
            A::comp_add,
            Cell::empty_context(),
        )?;

        Ok(removed.is_some())
    }

    pub fn build(self) -> Result<AugDict<K, A, V>, Error> {
        let mut res = AugDict::<K, A, V>::from_parts(Dict::from_raw(self.dict_root), A::default());
        res.update_root_extra()?;
        Ok(res)
    }
}

#[allow(clippy::type_complexity)]
pub fn split_aug_dict<K, A, V>(
    workchain: i32,
    dict: AugDict<K, A, V>,
    depth: u8,
) -> Result<Vec<(ShardIdent, AugDict<K, A, V>)>, Error>
where
    K: StoreDictKey,
    V: Store,
    for<'a> A: AugDictExtra + Store + Load<'a>,
{
    fn split_dict_impl<K, A, V>(
        shard: &ShardIdent,
        dict: AugDict<K, A, V>,
        depth: u8,
        shards: &mut Vec<(ShardIdent, AugDict<K, A, V>)>,
        builder: &mut CellDataBuilder,
    ) -> Result<(), Error>
    where
        K: StoreDictKey,
        V: Store,
        for<'a> A: AugDictExtra + Store + Load<'a>,
    {
        let (left_shard_ident, right_shard_ident) = 'split: {
            if depth > 0 {
                if let Some((left, right)) = shard.split() {
                    break 'split (left, right);
                }
            }
            shards.push((*shard, dict));
            return Ok(());
        };

        let (left_dict, right_dict) = {
            builder.clear_bits();
            let prefix_len = shard.prefix_len();
            if prefix_len > 0 {
                builder.store_uint(shard.prefix() >> (64 - prefix_len), prefix_len)?;
            }
            dict.split_by_prefix(&builder.as_data_slice())?
        };

        split_dict_impl(&left_shard_ident, left_dict, depth - 1, shards, builder)?;
        split_dict_impl(&right_shard_ident, right_dict, depth - 1, shards, builder)
    }

    let mut shards = Vec::with_capacity(2usize.pow(depth as _));

    split_dict_impl(
        &ShardIdent::new_full(workchain),
        dict,
        depth,
        &mut shards,
        &mut CellDataBuilder::new(),
    )?;

    Ok(shards)
}

pub fn split_aug_dict_raw<K, A, V>(
    dict: AugDict<K, A, V>,
    depth: u8,
) -> Result<FastHashMap<HashBytes, Cell>, Error>
where
    K: DictKey,
    A: Default,
{
    fn split_dict_impl(
        dict: Option<Cell>,
        key_bit_len: u16,
        depth: u8,
        shards: &mut FastHashMap<HashBytes, Cell>,
    ) -> Result<(), Error> {
        if dict.is_none() {
            return Ok(());
        }

        let Some(depth) = depth.checked_sub(1) else {
            if let Some(cell) = dict {
                shards.insert(*cell.repr_hash(), cell);
            }
            return Ok(());
        };

        let PartialSplitDict {
            remaining_bit_len,
            left_branch,
            right_branch,
        } = dict_split_raw(dict.as_ref(), key_bit_len, Cell::empty_context())?;

        split_dict_impl(left_branch, remaining_bit_len, depth, shards)?;
        split_dict_impl(right_branch, remaining_bit_len, depth, shards)
    }

    let mut shards =
        FastHashMap::with_capacity_and_hasher(2usize.pow(depth as _), Default::default());

    let (dict_root, _) = dict.into_parts();
    split_dict_impl(dict_root.into_root(), K::BITS, depth, &mut shards)?;

    Ok(shards)
}

/// Merges multiple `RelaxedAugDict` into a single one using a binary tree approach.
pub fn merge_relaxed_aug_dicts<I, K, A, V>(shards: I) -> Result<AugDict<K, A, V>, Error>
where
    I: IntoIterator<Item = (ShardIdent, RelaxedAugDict<K, A, V>)>,
    K: StoreDictKey,
    V: Store,
    for<'a> A: AugDictExtra + Store + Load<'a>,
{
    let mut stack = Vec::<(ShardIdent, Option<Cell>)>::new();

    for (shard, dict) in shards {
        stack.push((shard, dict.dict_root));

        while let [.., (left_shard, left_root), (right_shard, right_root)] = stack.as_slice() {
            let Some(opposite) = left_shard.opposite() else {
                // There cannot be two items in the stack when one of them is a "full" shard.
                return Err(Error::InvalidData);
            };

            if *right_shard == opposite {
                let merged_shard = left_shard.merge().unwrap();
                let merged_root = aug_dict_merge_siblings(
                    left_root,
                    right_root,
                    K::BITS,
                    A::comp_add,
                    Cell::empty_context(),
                )?;

                stack.pop();
                stack.pop();
                stack.push((merged_shard, merged_root));
            } else {
                break;
            }
        }
    }

    let mut stack = stack.into_iter();
    let Some((merged_shard, merged_root)) = stack.next() else {
        return Err(Error::InvalidData);
    };
    if !merged_shard.is_full() || stack.next().is_some() {
        return Err(Error::InvalidData);
    }

    let mut res = AugDict::from_parts(Dict::from_raw(merged_root), A::default());
    res.update_root_extra()?;

    Ok(res)
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use tycho_types::models::DepthBalanceInfo;

    use super::*;

    #[test]
    fn split_merge_works() -> anyhow::Result<()> {
        type MyAugDict = AugDict<u32, DepthBalanceInfo, ()>;

        fn test_split_merge(dict: MyAugDict, depth: u8) {
            let shards = split_aug_dict(0, dict.clone(), depth).unwrap();
            println!("SHARD LEN: {}", shards.len());

            let merged = merge_relaxed_aug_dicts(shards.into_iter().map(|(shard, dict)| {
                let (dict, _) = dict.into_parts();
                (shard, RelaxedAugDict {
                    dict_root: dict.into_root(),
                    _marker: PhantomData::<(u32, DepthBalanceInfo, ())>,
                })
            }))
            .unwrap();

            assert_eq!(dict, merged);
        }

        let mut dict = MyAugDict::new();
        for i in 0..100 {
            dict.set(i, DepthBalanceInfo::default(), ())?;
        }

        for depth in 0..7 {
            test_split_merge(dict.clone(), depth);
        }

        Ok(())
    }
}
