use syn::Token;
use syn::punctuated::Punctuated;

use crate::internals::attr;
use crate::internals::ctxt::Ctxt;

pub struct Container<'a> {
    pub ident: syn::Ident,
    // pub attrs: attr::Container,
    pub data: Data<'a>,
    pub generics: &'a syn::Generics,
    #[allow(unused)]
    pub original: &'a syn::DeriveInput,
}

impl<'a> Container<'a> {
    pub(crate) fn from_ast(cx: &Ctxt, item: &'a syn::DeriveInput) -> Option<Self> {
        let data = match &item.data {
            syn::Data::Enum(data) => Data::Enum(enum_from_ast(cx, &data.variants)),
            syn::Data::Struct(data) => {
                let (style, fields) = struct_from_ast(cx, &data.fields);
                Data::Struct(style, fields)
            }
            syn::Data::Union(_) => {
                cx.error_spanned_by(item, "tl doesn't support derive for unions");
                return None;
            }
        };

        let container = Self {
            ident: item.ident.clone(),
            data,
            generics: &item.generics,
            original: item,
        };

        container.validate(cx);

        Some(container)
    }

    fn validate(&self, cx: &Ctxt) {
        match &self.data {
            Data::Enum(variants) => {
                for var in variants {
                    for field in &var.fields {
                        if field.attrs.important && field.attrs.partial {
                            cx.error_spanned_by(
                                field.original,
                                "field cannot be marked as `important` and `partial` at the same time",
                            );
                        }
                    }
                }
            }
            Data::Struct(_, fields) => {
                for field in fields {
                    if field.attrs.important && field.attrs.partial {
                        cx.error_spanned_by(
                            field.original,
                            "field cannot be marked as `important` and `partial` at the same time",
                        );
                    }
                }
            }
        }
    }
}

fn enum_from_ast<'a>(
    cx: &Ctxt,
    variants: &'a Punctuated<syn::Variant, Token![,]>,
) -> Vec<Variant<'a>> {
    variants
        .iter()
        .map(|variant| {
            let (style, fields) = struct_from_ast(cx, &variant.fields);
            Variant {
                ident: variant.ident.clone(),
                style,
                fields,
                original: variant,
            }
        })
        .collect()
}

fn struct_from_ast<'a>(cx: &Ctxt, fields: &'a syn::Fields) -> (Style, Vec<Field<'a>>) {
    match fields {
        syn::Fields::Named(fields) => (Style::Struct, fields_from_ast(cx, &fields.named)),
        syn::Fields::Unnamed(fields) => (Style::Tuple, fields_from_ast(cx, &fields.unnamed)),
        syn::Fields::Unit => (Style::Unit, Vec::new()),
    }
}

fn fields_from_ast<'a>(cx: &Ctxt, fields: &'a Punctuated<syn::Field, Token![,]>) -> Vec<Field<'a>> {
    fields
        .iter()
        .enumerate()
        .map(|(i, field)| Field {
            member: match &field.ident {
                Some(ident) => syn::Member::Named(ident.clone()),
                None => syn::Member::Unnamed(i.into()),
            },
            attrs: attr::Field::from_ast(cx, field),
            ty: &field.ty,
            original: field,
        })
        .collect()
}

pub enum Data<'a> {
    Enum(Vec<Variant<'a>>),
    Struct(Style, Vec<Field<'a>>),
}

pub struct Variant<'a> {
    #[allow(unused)]
    pub ident: syn::Ident,
    // pub attrs: attr::Variant,
    #[allow(unused)]
    pub style: Style,
    pub fields: Vec<Field<'a>>,
    #[allow(unused)]
    pub original: &'a syn::Variant,
}

pub struct Field<'a> {
    pub member: syn::Member,
    pub attrs: attr::Field,
    pub ty: &'a syn::Type,
    pub original: &'a syn::Field,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Style {
    Struct,
    Tuple,
    Unit,
}
