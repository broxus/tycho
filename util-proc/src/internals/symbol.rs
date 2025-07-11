pub const SERDE: Symbol = Symbol("serde");

pub const PARTIAL: Symbol = Symbol("partial");
pub const IMPORTANT: Symbol = Symbol("important");

#[derive(Copy, Clone)]
pub struct Symbol(&'static str);

impl PartialEq<Symbol> for syn::Ident {
    fn eq(&self, other: &Symbol) -> bool {
        self == other.0
    }
}

impl PartialEq<Symbol> for &syn::Ident {
    fn eq(&self, other: &Symbol) -> bool {
        *self == other.0
    }
}

impl PartialEq<Symbol> for syn::Path {
    fn eq(&self, other: &Symbol) -> bool {
        self.is_ident(other.0)
    }
}

impl PartialEq<Symbol> for &syn::Path {
    fn eq(&self, other: &Symbol) -> bool {
        self.is_ident(other.0)
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
