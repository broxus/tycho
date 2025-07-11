use proc_macro2::TokenStream;
use quote::ToTokens;

use crate::internals::ctxt::Ctxt;
use crate::internals::symbol::*;

// TODO: Allow specifying a custom name for the generated struct.

pub struct Field {
    pub partial: bool,
    pub important: bool,
}

impl Field {
    pub fn from_ast(cx: &Ctxt, field: &syn::Field) -> Self {
        let mut partial = BoolAttr::none(cx, PARTIAL);
        let mut important = BoolAttr::none(cx, IMPORTANT);

        for attr in &field.attrs {
            let s = if attr.path() == PARTIAL {
                &mut partial
            } else if attr.path() == IMPORTANT {
                &mut important
            } else {
                continue;
            };

            if !matches!(&attr.meta, syn::Meta::Path(_)) {
                cx.error_spanned_by(
                    &attr.meta,
                    format!("expected attribute to be a simple `#[{}]`", s.0.name),
                );
                continue;
            }

            s.set_true(attr);
        }

        Self {
            partial: partial.get(),
            important: important.get(),
        }
    }
}

#[allow(unused)]
struct BoolAttr<'c>(Attr<'c, ()>);

#[allow(unused)]
impl<'c> BoolAttr<'c> {
    fn none(cx: &'c Ctxt, name: Symbol) -> Self {
        BoolAttr(Attr::none(cx, name))
    }

    fn set_true<O>(&mut self, object: O)
    where
        O: ToTokens,
    {
        self.0.set(object, ());
    }

    fn get(&self) -> bool {
        self.0.value.is_some()
    }
}

struct Attr<'c, T> {
    cx: &'c Ctxt,
    name: Symbol,
    tokens: TokenStream,
    value: Option<T>,
}

impl<'c, T> Attr<'c, T> {
    fn none(cx: &'c Ctxt, name: Symbol) -> Self {
        Self {
            cx,
            name,
            tokens: TokenStream::new(),
            value: None,
        }
    }

    fn set<O>(&mut self, object: O, value: T)
    where
        O: ToTokens,
    {
        let tokens = object.into_token_stream();

        if self.value.is_some() {
            self.cx
                .error_spanned_by(tokens, format!("duplicated attribute `{}`", self.name));
        } else {
            self.tokens = tokens;
            self.value = Some(value);
        }
    }

    #[allow(unused)]
    fn set_opt<O>(&mut self, object: O, value: Option<T>)
    where
        O: ToTokens,
    {
        if let Some(value) = value {
            self.set(object, value);
        }
    }

    #[allow(unused)]
    fn set_if_none(&mut self, value: T) {
        if self.value.is_none() {
            self.value = Some(value);
        }
    }

    #[allow(unused)]
    fn get(self) -> Option<T> {
        self.value
    }

    #[allow(unused)]
    fn get_with_tokens(self) -> Option<(TokenStream, T)> {
        match self.value {
            Some(value) => Some((self.tokens, value)),
            None => None,
        }
    }
}
