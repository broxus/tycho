use everscale_types::dict::{
    aug_dict_insert, aug_dict_remove_owned, build_aug_dict_from_sorted_iter, AugDictExtra, DictKey,
    SetMode,
};
use everscale_types::error::Error;
use everscale_types::models::Lazy;
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
    K: Store + DictKey,
    for<'a> A: AugDictExtra + Store + Load<'a>,
{
    pub fn try_from_sorted_iter_lazy<'a, I>(iter: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (&'a K, &'a A, &'a Lazy<V>)>,
        K: Ord + 'a,
        A: 'a,
        V: 'a,
    {
        Ok(Self {
            dict_root: build_aug_dict_from_sorted_iter(
                iter.into_iter().map(|(k, a, v)| {
                    // SAFETY: We know that this cell is not a library cell.
                    let value = v.inner().as_slice_allow_pruned();
                    (k, a, value)
                }),
                K::BITS,
                A::comp_add,
                &mut Cell::empty_context(),
            )?,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn try_from_sorted_iter_any<'a, I>(iter: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (&'a K, &'a A, &'a dyn Store)>,
        K: Ord + 'a,
        A: 'a,
    {
        Ok(Self {
            dict_root: build_aug_dict_from_sorted_iter(
                iter,
                K::BITS,
                A::comp_add,
                &mut Cell::empty_context(),
            )?,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn set_as_lazy(&mut self, key: &K, extra: &A, value: &Lazy<V>) -> Result<bool, Error> {
        self.set_any(key, extra, &value.inner().as_slice()?)
    }

    pub fn set_any(&mut self, key: &K, extra: &A, value: &dyn Store) -> Result<bool, Error> {
        let cx = &mut Cell::empty_context();

        let mut key_builder = CellBuilder::new();
        key.store_into(&mut key_builder, cx)?;

        let inserted = aug_dict_insert(
            &mut self.dict_root,
            &mut key_builder.as_data_slice(),
            K::BITS,
            extra,
            value,
            SetMode::Set,
            A::comp_add,
            cx,
        )?;

        Ok(inserted)
    }

    pub fn remove(&mut self, key: &K) -> Result<bool, Error> {
        let cx = &mut Cell::empty_context();

        let mut key_builder = CellBuilder::new();
        key.store_into(&mut key_builder, cx)?;

        let removed = aug_dict_remove_owned(
            &mut self.dict_root,
            &mut key_builder.as_data_slice(),
            K::BITS,
            false,
            A::comp_add,
            cx,
        )?;

        Ok(removed.is_some())
    }

    pub fn build(self) -> Result<AugDict<K, A, V>, Error> {
        let mut res = AugDict::<K, A, V>::from_parts(Dict::from_raw(self.dict_root), A::default());
        res.update_root_extra()?;
        Ok(res)
    }
}
