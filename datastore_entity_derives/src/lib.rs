// Based on this blog post
// https://cprimozic.net/blog/writing-a-hashmap-to-struct-procedural-macro-in-rust/
#![recursion_limit = "128"]

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{quote};
use syn::{DeriveInput, Expr, Ident};

struct FieldMeta {
    ident: Ident,
    /// When reading from datastore properties and creating a struct,
    /// use this expression.
    into_property: Expr,
    /// When reading from a struct and creating datastore properties,
    /// use this expression.
    from_property: Expr,
}

#[proc_macro_derive(DatastoreManaged, attributes(kind, key))]
pub fn datastore_managed(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as DeriveInput);

    let mut kind: Option<String> = None;
    let mut key_field: Option<String> = None;

    let fields: Vec<FieldMeta> = match ast.data {
        syn::Data::Struct(vdata) => {
            let mut field_metas = Vec::new();

            for ref attr in ast.attrs {
                match attr.parse_meta().unwrap() {
                    syn::Meta::NameValue(ref name_value) => {
                        match name_value.path.get_ident().unwrap().to_string().as_str() {
                            "kind" => {
                                if let syn::Lit::Str(lit_str) = name_value.lit.clone() {
                                    kind = Some(lit_str.value());
                                }
                            },
                            _ => (),
                        }
                    },
                    _ => (),
                }
            }

            for ref field in vdata.fields.iter() {
                for ref attr in &field.attrs {
                    match attr.parse_meta().unwrap() {
                        syn::Meta::Path(ref path) => {
                            match path.get_ident().unwrap().to_string().as_str() {
                                "key" => {
                                    key_field = Some(field.ident.as_ref().unwrap().clone().to_string());
                                },
                                _ => (),
                            }
                        },
                        _ => (),
                    }
                }
                match &field.ty {
                    syn::Type::Path(p) => {
                        match p.path.segments.first().unwrap().ident.to_string().as_str() {
                            "String" => {
                                let ident = field.ident.as_ref().unwrap().clone();
                                let key = ident.to_string();
                                let into_property_expr_string = format!("properties.get_string(\"{}\")", key);
                                let from_property_expr_string = format!("properties.set_string(\"{}\", entity.{})", key, key);
                                field_metas.push(FieldMeta {
                                    ident,
                                    into_property: parse_expr(&into_property_expr_string),
                                    from_property: parse_expr(&from_property_expr_string),
                                });
                            },
                            "i64" => {
                                let ident = field.ident.as_ref().unwrap().clone();
                                let key = ident.to_string();
                                let into_property_expr_string = format!("properties.get_integer(\"{}\")", key);
                                let from_property_expr_string = format!("properties.set_integer(\"{}\", entity.{})", key, key);
                                field_metas.push(FieldMeta {
                                    ident,
                                    into_property: parse_expr(&into_property_expr_string),
                                    from_property: parse_expr(&from_property_expr_string),
                                });
                            },
                            "bool" => {
                                let ident = field.ident.as_ref().unwrap().clone();
                                let key = ident.to_string();
                                let into_property_expr_string = format!("properties.get_bool(\"{}\")", key);
                                let from_property_expr_string = format!("properties.set_bool(\"{}\", entity.{})", key, key);
                                field_metas.push(FieldMeta {
                                    ident,
                                    into_property: parse_expr(&into_property_expr_string),
                                    from_property: parse_expr(&from_property_expr_string),
                                });
                            },
                            _ => (), // Ignore
                        }
                    },
                    _ => (), // Ignore
                }
            }
            field_metas
        },
        syn::Data::Enum(_) => panic!("You can only derive this on structs!"),
        syn::Data::Union(_) => panic!("You can only derive this on structs!"),
    };

    let name = &ast.ident;
    let idents = fields.iter().map(|f| f.ident.clone()).collect::<Vec<_>>();
    let into_properties = fields.iter().map(|f| f.into_property.clone()).collect::<Vec<_>>();
    let from_properties = fields.iter().map(|f| f.from_property.clone()).collect::<Vec<_>>();

    let kind_str = kind.unwrap();
    let key_field_str = key_field.unwrap();
    let key_field_expr = parse_expr(&key_field_str);
    let self_key_field_expr = parse_expr(&format!("self.{}.as_ref()", key_field_str));
    let entity_key_field_expr = parse_expr(&format!("entity.{}", key_field_str));

    let tokens = quote! {
        impl #name {
            pub const fn kind(&self) -> &'static str {
                #kind_str
            }

            pub fn id(&self) -> core::option::Option<&google_datastore1::schemas::Key> {
                #self_key_field_expr
            }
        }

        impl core::convert::TryFrom<datastore_entity::DatastoreEntity> for #name {
            type Error = datastore_entity::DatastoreParseError;

            fn try_from(mut entity: datastore_entity::DatastoreEntity) -> Result<Self, Self::Error> {
                let key = entity.key();
                let mut properties = datastore_entity::DatastoreProperties::from(entity)
                    .ok_or_else(|| datastore_entity::DatastoreParseError::NoProperties)?;
                Ok(
                    #name {
                        #key_field_expr: key,
                        #(
                            #idents: #into_properties?,
                        )*
                    }
                )
            }
        }

        impl core::convert::From<#name> for datastore_entity::DatastoreEntity {
            fn from(entity: #name) -> Self {
                let mut properties = datastore_entity::DatastoreProperties::new();
                #(
                    #from_properties;
                )*
                datastore_entity::DatastoreEntity::from(
                    #entity_key_field_expr,
                    properties,
                )
            }
        }
    };

    TokenStream::from(tokens)
}

fn parse_expr(expr_string: &str) -> Expr {
    syn::parse_str::<Expr>(expr_string).expect("failed to parse expression")
}
