use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Field, Fields, GenericArgument, PathArguments, Type};

pub fn impl_transactional(input: DeriveInput) -> Result<TokenStream, syn::Error> {
    let name = &input.ident;
    let vis = &input.vis;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let tx_name = format_ident!("{}Tx", name);

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &input,
                    "only named structs supported",
                ));
            }
        },
        _ => return Err(syn::Error::new_spanned(&input, "only structs supported")),
    };

    let tx_field = fields
        .iter()
        .find(|f| has_attr(f, "tx", "state"))
        .ok_or_else(|| syn::Error::new_spanned(&input, "missing #[tx(state)] field"))?;
    let tx_field_name = tx_field.ident.as_ref().unwrap();

    let mut tx_fields = Vec::new();
    let mut begin_init = Vec::new();
    let mut begin_calls = Vec::new();
    let mut commit_calls = Vec::new();
    let mut rollback_calls = Vec::new();
    let mut rollback_restore = Vec::new();
    let mut helper_methods = Vec::new();

    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap();
        let field_type = &field.ty;

        if has_attr(field, "tx", "state") || has_attr(field, "tx", "skip") {
            continue;
        }

        let is_collection = has_attr(field, "tx", "collection");
        let is_transactional = has_attr(field, "tx", "transactional");

        if is_collection {
            let (map_kind, key_type, value_type) = extract_map_types(field_type)?;
            let added_name = format_ident!("added_{}", field_name);
            let removed_name = format_ident!("removed_{}", field_name);
            let insert_method = format_ident!("tx_insert_{}", field_name);
            let remove_method = format_ident!("tx_remove_{}", field_name);
            let get_method = format_ident!("tx_get_{}", field_name);
            let get_mut_method = format_ident!("tx_get_mut_{}", field_name);

            let removed_type = match map_kind {
                MapKind::BTreeMap => quote! { std::collections::BTreeMap<#key_type, #value_type> },
                MapKind::HashMap | MapKind::FastHashMap => {
                    quote! { tycho_util::FastHashMap<#key_type, #value_type> }
                }
            };

            tx_fields.push(quote! {
                #added_name: tycho_util::FastHashSet<#key_type>,
                #removed_name: #removed_type
            });

            let removed_init = match map_kind {
                MapKind::BTreeMap => quote! { std::collections::BTreeMap::new() },
                MapKind::HashMap | MapKind::FastHashMap => {
                    quote! { tycho_util::FastHashMap::default() }
                }
            };
            begin_init.push(quote! {
                #added_name: tycho_util::FastHashSet::default(),
                #removed_name: #removed_init
            });

            begin_calls.push(quote! {
                tycho_util::transactional_types::TransactionalCollection::begin_all(&mut self.#field_name);
            });

            commit_calls.push(quote! {
                tycho_util::transactional_types::TransactionalCollection::commit_all(&mut self.#field_name);
            });

            rollback_calls.push(quote! {
                for (_, value) in self.#field_name.iter_mut() {
                    if tycho_util::transactional_types::Transactional::is_in_transaction(value) {
                        tycho_util::transactional_types::Transactional::rollback(value);
                    }
                }
                for key in tx.#added_name {
                    self.#field_name.remove(&key);
                }
                for (key, value) in tx.#removed_name {
                    self.#field_name.insert(key, value);
                }
            });

            helper_methods.push(quote! {
                pub fn #insert_method(&mut self, key: #key_type, value: #value_type) {
                    let is_new = !self.#field_name.contains_key(&key);
                    if let Some(tx) = &mut self.#tx_field_name {
                        let was_removed = tx.#removed_name.contains_key(&key);
                        if is_new && !was_removed {
                            tx.#added_name.insert(key.clone());
                        }
                    }
                    self.#field_name.insert(key, value);
                }

                pub fn #remove_method(&mut self, key: &#key_type) -> bool {
                    if let Some(removed) = self.#field_name.remove(key) {
                        if let Some(tx) = &mut self.#tx_field_name {
                            if !tx.#added_name.remove(key) && !tx.#removed_name.contains_key(key) {
                                tx.#removed_name.insert(key.clone(), removed);
                            }
                        }
                        true
                    } else {
                        false
                    }
                }

                #[inline]
                pub fn #get_method(&self, key: &#key_type) -> Option<&#value_type> {
                    self.#field_name.get(key)
                }

                #[inline]
                pub fn #get_mut_method(&mut self, key: &#key_type) -> Option<&mut #value_type> {
                    self.#field_name.get_mut(key)
                }
            });
        } else if is_transactional {
            if is_option_type(field_type) {
                let snapshot_name = format_ident!("snapshot_{}", field_name);
                let set_method = format_ident!("tx_set_{}", field_name);

                // Option<Option<T>> (outer: snapshot taken; inner: original value)
                tx_fields.push(quote! { #snapshot_name: Option<#field_type> });
                begin_init.push(quote! { #snapshot_name: None });

                // begin(): only affects existing Some(T); if None, it's a no-op
                begin_calls.push(quote! {
                    tycho_util::transactional_types::Transactional::begin(&mut self.#field_name);
                });

                commit_calls.push(quote! {
                    if let Some(tx) = &self.#tx_field_name {
                        if tx.#snapshot_name.is_none() {
                            tycho_util::transactional_types::Transactional::commit(&mut self.#field_name);
                        }
                    }
                });

                rollback_calls.push(quote! {
                    if let Some(snapshot) = tx.#snapshot_name {
                        self.#field_name = snapshot;
                        // clear possible tx-state if restored value is Some(T) and had begun
                        if tycho_util::transactional_types::Transactional::is_in_transaction(&self.#field_name) {
                            tycho_util::transactional_types::Transactional::rollback(&mut self.#field_name);
                        }
                    } else {
                        tycho_util::transactional_types::Transactional::rollback(&mut self.#field_name);
                    }
                });

                helper_methods.push(quote! {
                    pub fn #set_method(&mut self, new: #field_type) {
                        if let Some(tx) = &mut self.#tx_field_name {
                            if tx.#snapshot_name.is_none() {
                                tx.#snapshot_name = Some(self.#field_name.take());
                            }
                        }
                        self.#field_name = new;
                    }
                });
            } else {
                begin_calls
                    .push(quote! { tycho_util::transactional_types::Transactional::begin(&mut self.#field_name); });
                commit_calls
                    .push(quote! { tycho_util::transactional_types::Transactional::commit(&mut self.#field_name); });
                rollback_calls
                    .push(quote! { tycho_util::transactional_types::Transactional::rollback(&mut self.#field_name); });
            }
        } else {
            let snapshot_name = format_ident!("snapshot_{}", field_name);
            tx_fields.push(quote! { #snapshot_name: #field_type });
            begin_init.push(quote! { #snapshot_name: self.#field_name.clone() });
            rollback_restore.push(quote! { self.#field_name = tx.#snapshot_name; });
        }
    }

    let tx_struct = if tx_fields.is_empty() {
        quote! { #vis struct #tx_name; }
    } else {
        quote! { #vis struct #tx_name { #(#tx_fields),* } }
    };

    let tx_init = if begin_init.is_empty() {
        quote! { #tx_name }
    } else {
        quote! { #tx_name { #(#begin_init),* } }
    };

    Ok(quote! {
        #tx_struct

        impl #impl_generics tycho_util::transactional_types::Transactional for #name #ty_generics #where_clause {
            fn begin(&mut self) {
                assert!(self.#tx_field_name.is_none(), "transaction already in progress");
                self.#tx_field_name = Some(#tx_init);
                #(#begin_calls)*
            }

            fn commit(&mut self) {
                assert!(self.#tx_field_name.is_some(), "no active transaction to commit");
                #(#commit_calls)*
                self.#tx_field_name = None;
            }

            fn rollback(&mut self) {
                let Some(tx) = self.#tx_field_name.take() else {
                    panic!("no active transaction to rollback")
                };
                #(#rollback_calls)*
                #(#rollback_restore)*
            }

            fn is_in_transaction(&self) -> bool {
                self.#tx_field_name.is_some()
            }
        }

        impl #impl_generics #name #ty_generics #where_clause {
            #(#helper_methods)*
        }
    })
}

fn has_attr(field: &Field, name: &str, value: &str) -> bool {
    field.attrs.iter().any(|attr| {
        if !attr.path().is_ident(name) {
            return false;
        }
        attr.parse_args::<syn::Ident>()
            .map(|id| id == value)
            .unwrap_or(false)
    })
}

enum MapKind {
    BTreeMap,
    HashMap,
    FastHashMap,
}

fn is_option_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            return seg.ident == "Option";
        }
    }
    false
}

fn extract_map_types(ty: &Type) -> Result<(MapKind, &Type, &Type), syn::Error> {
    if let Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            let kind = if seg.ident == "BTreeMap" {
                Some(MapKind::BTreeMap)
            } else if seg.ident == "HashMap" {
                Some(MapKind::HashMap)
            } else if seg.ident == "FastHashMap" {
                Some(MapKind::FastHashMap)
            } else {
                None
            };

            if let Some(kind) = kind {
                if let PathArguments::AngleBracketed(args) = &seg.arguments {
                    let mut iter = args.args.iter();
                    if let (Some(GenericArgument::Type(k)), Some(GenericArgument::Type(v))) =
                        (iter.next(), iter.next())
                    {
                        return Ok((kind, k, v));
                    }
                }
            }
        }
    }
    Err(syn::Error::new_spanned(
        ty,
        "expected BTreeMap<K, V>, HashMap<K, V> or FastHashMap<K, V>",
    ))
}
