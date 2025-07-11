use proc_macro2::TokenStream;
use quote::quote;

use crate::internals::{ast, ctxt, symbol};

// TODO: Allow custom derive for generated struct.

pub fn impl_derive(input: syn::DeriveInput) -> Result<TokenStream, Vec<syn::Error>> {
    let cx = ctxt::Ctxt::new();
    let container = match ast::Container::from_ast(&cx, &input) {
        Some(container) => container,
        None => return Err(cx.check().unwrap_err()),
    };
    cx.check()?;

    let vis = &input.vis;

    let ident = &container.ident;
    let (impl_generics, ty_generics, where_clause) = container.generics.split_for_impl();

    let partial_ident = quote::format_ident!("{ident}Partial");

    let result = match container.data {
        ast::Data::Struct(_style, fields) => {
            let value_ident = quote::format_ident!("value");
            let (members, readers) = build_fields(&fields, quote! { #value_ident. });

            quote! {
                #[derive(::tycho_util::__internal::serde::Serialize)]
                #[allow(unused)]
                #vis struct #partial_ident {
                    #(#members,)*
                }

                #[automatically_derived]
                impl<#impl_generics> ::tycho_util::config::PartialConfig for #ident {
                    type Partial = #partial_ident<#ty_generics>;

                    #[inline]
                    fn into_partial(self) -> Self::Partial {
                        self.into()
                    }
                }

                #[automatically_derived]
                impl<#impl_generics> From<#ident> for #partial_ident<#ty_generics> #where_clause {
                    #[inline]
                    fn from(#value_ident: #ident) -> Self {
                        Self {
                            #(#readers,)*
                        }
                    }
                }
            }
        }
        ast::Data::Enum(_variants) => {
            return Err(vec![syn::Error::new_spanned(
                container.original,
                "enums are not supported by `PartialConfig` yet",
            )]);
        }
    };

    Ok(result)
}

fn build_fields(
    fields: &[ast::Field<'_>],
    original: TokenStream,
) -> (Vec<TokenStream>, Vec<TokenStream>) {
    let mut skipped = 0usize;
    fields
        .iter()
        .filter_map(|field| {
            let idx;
            let (partial_ident, ident) = match &field.member {
                ident @ syn::Member::Named(_) => (ident, ident),
                ident @ syn::Member::Unnamed(n) => {
                    idx = syn::Member::from(n.index as usize - skipped);
                    (&idx, ident)
                }
            };

            let ty = field.ty;

            let serde_attrs = field
                .original
                .attrs
                .iter()
                .filter(|item| item.path() == symbol::SERDE);

            if field.attrs.important {
                let member = quote! {
                    #(#serde_attrs)*
                    #partial_ident: #ty
                };
                let read = quote! { #partial_ident: #original #ident };
                Some((member, read))
            } else if field.attrs.partial {
                let member = quote! {
                    #(#serde_attrs)*
                    #partial_ident: <#ty as ::tycho_util::config::PartialConfig>::Partial
                };
                let read = quote! {
                    #partial_ident: <#ty as ::tycho_util::config::PartialConfig>::into_partial(
                        #original #ident
                    )
                };
                Some((member, read))
            } else {
                skipped += 1;
                None
            }
        })
        .unzip()
}
