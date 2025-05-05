use everscale_types::cell::Lazy;
use everscale_types::dict::{
    aug_dict_insert, aug_dict_modify_from_sorted_iter, aug_dict_remove_owned,
    build_aug_dict_from_sorted_iter, AugDictExtra, DictKey, SetMode,
};
use everscale_types::error::Error;
use everscale_types::prelude::*;

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
