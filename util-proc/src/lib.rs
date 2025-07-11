use proc_macro::TokenStream;
use quote::quote;

mod derive_partial_config;
mod internals {
    pub mod ast;
    pub mod attr;
    pub mod ctxt;
    pub mod symbol;
}

#[proc_macro_derive(PartialConfig, attributes(partial, important))]
pub fn derive_partial_config(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    derive_partial_config::impl_derive(input)
        .unwrap_or_else(to_compile_errors)
        .into()
}

fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}
