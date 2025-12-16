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
        ast::Data::Struct(style, fields) => {
            let value_ident = quote::format_ident!("value");
            let (members, readers) = build_fields(style, &fields, quote! { #value_ident. });

            let partial_struct = match style {
                ast::Style::Struct | ast::Style::Unit => quote! {
                    #[derive(::tycho_util::__internal::serde::Serialize)]
                    #[allow(unused)]
                    #vis struct #partial_ident {
                        #(#members,)*
                    }
                },
                ast::Style::Tuple => quote! {
                    #[derive(::tycho_util::__internal::serde::Serialize)]
                    #[allow(unused)]
                    #vis struct #partial_ident(#(#members,)*);
                },
            };

            quote! {
                #partial_struct

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
    style: ast::Style,
    fields: &[ast::Field<'_>],
    original: TokenStream,
) -> (Vec<TokenStream>, Vec<TokenStream>) {
    let mut skipped = 0usize;
    let is_struct = style != ast::Style::Tuple;
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

            if field.attrs.partial {
                let member = if is_struct {
                    quote! {
                        #(#serde_attrs)*
                        #partial_ident: <#ty as ::tycho_util::config::PartialConfig>::Partial
                    }
                } else {
                    quote! {
                        #(#serde_attrs)*
                        <#ty as ::tycho_util::config::PartialConfig>::Partial
                    }
                };
                let read = quote! {
                    #partial_ident: <#ty as ::tycho_util::config::PartialConfig>::into_partial(
                        #original #ident
                    )
                };
                Some((member, read))
            } else if field.attrs.important || !is_struct {
                let member = if is_struct {
                    quote! {
                        #(#serde_attrs)*
                        #partial_ident: #ty
                    }
                } else {
                    quote! {
                        #(#serde_attrs)*
                        #ty
                    }
                };
                let read = quote! { #partial_ident: #original #ident };
                Some((member, read))
            } else {
                skipped += 1;
                None
            }
        })
        .unzip()
}
