#[macro_export]
macro_rules! impl_enum_try_into {
    ($enum:ty, $variant:ident, $type:ty) => {
        impl TryFrom<$enum> for $type {
            type Error = anyhow::Error;
            fn try_from(value: $enum) -> Result<Self, Self::Error> {
                use $enum::*;
                match value {
                    $variant(res) => Ok(res),
                    _ => Err(anyhow!(
                        "enum value is not {}::{}",
                        stringify!($enum),
                        stringify!($variant),
                    )),
                }
            }
        }
    };
}
