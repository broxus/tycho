use quote::quote;
use syn::{Data, DeriveInput, Field, Fields};
pub fn impl_transactional(input: DeriveInput) -> Result<proc_macro2::TokenStream, syn::Error> {
    let name = &input.ident;

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

    let tx_fields: Vec<_> = fields.iter().filter(|f| !has_skip_attr(f)).collect();

    let tx_idents: Vec<_> = tx_fields.iter().filter_map(|f| f.ident.as_ref()).collect();

    let mut generics = input.generics.clone();
    {
        let where_clause = generics.make_where_clause();
        for field in &tx_fields {
            let ty = &field.ty;
            where_clause
                .predicates
                .push(syn::parse_quote!(#ty: tycho_util::transactional::Transactional));
        }
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let in_tx_body = if tx_idents.is_empty() {
        quote! { false }
    } else {
        let checks = tx_idents.iter().map(|field| quote! { self.#field.in_tx() });
        quote! { #(#checks)||* }
    };

    Ok(quote! {
        impl #impl_generics tycho_util::transactional::Transactional for #name #ty_generics #where_clause {
            fn begin(&mut self) {
                #( self.#tx_idents.begin(); )*
            }

            fn commit(&mut self) {
                #( self.#tx_idents.commit(); )*
            }

            fn rollback(&mut self) {
                #( self.#tx_idents.rollback(); )*
            }

            fn in_tx(&self) -> bool {
                #in_tx_body
            }
        }
    })
}

fn has_skip_attr(field: &Field) -> bool {
    field.attrs.iter().any(|attr| {
        attr.path().is_ident("tx")
            && attr
                .parse_args::<syn::Ident>()
                .map(|id| id == "skip")
                .unwrap_or(false)
    })
}
