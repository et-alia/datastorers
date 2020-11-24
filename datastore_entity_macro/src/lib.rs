// Based on this blog post
// https://cprimozic.net/blog/writing-a-hashmap-to-struct-procedural-macro-in-rust/
#![recursion_limit = "128"]

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{quote};
use syn::{DeriveInput, Expr};

struct FieldMeta {
    ident: syn::Ident,
    /// When reading from datastore properties and creating a struct,
    /// use this expression.
    into_property: Expr,
    /// When reading from a struct and creating datastore properties,
    /// use this expression.
    from_property: Expr,
}

#[proc_macro_derive(DatastoreEntity)]
pub fn datastore_entity(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as DeriveInput);

    let fields: Vec<FieldMeta> = match ast.data {
        syn::Data::Struct(vdata) => {
            let mut field_metas = Vec::new();

            for ref field in vdata.fields.iter() {
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

    let tokens = quote! {
        /// Force DatastoreEntity to be implemented
        impl #name where #name: DatastoreEntity {}

        impl core::convert::TryFrom<datastore_entity::DatastoreProperties> for #name {
            type Error = datastore_entity::DatastoreParseError;

            fn try_from(mut properties: datastore_entity::DatastoreProperties) -> Result<Self, Self::Error> {
                Self::try_from(&mut properties)
            }
        }

        impl core::convert::TryFrom<&mut datastore_entity::DatastoreProperties> for #name {
            type Error = datastore_entity::DatastoreParseError;

            fn try_from(properties: &mut datastore_entity::DatastoreProperties) -> Result<Self, Self::Error> {
                Ok(
                    #name {
                        #(
                            #idents: #into_properties?,
                        )*
                    }
                )
            }
        }

        impl core::convert::From<#name> for datastore_entity::DatastoreProperties {
            fn from(entity: #name) -> Self {
                let mut properties = datastore_entity::DatastoreProperties::new();
                #(
                    #from_properties;
                )*
                properties
            }
        }
    };

    TokenStream::from(tokens)
}

fn parse_expr(expr_string: &str) -> Expr {
    syn::parse_str::<Expr>(expr_string).expect("failed to parse expression")
}
