// Based on this blog post
// https://cprimozic.net/blog/writing-a-hashmap-to-struct-procedural-macro-in-rust/
#![recursion_limit = "128"]

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{quote};
use syn::{DeriveInput, Expr, Ident};
use std::iter::repeat;

struct EntityGetter {
    key_type: Expr,
    get_one_method_name: Expr,
}

struct FieldMeta {
    // Property name in the rust struct
    ident: Ident,
    // Property name (key) in the datastore entity
    key: Expr,
    /// When reading from datastore properties and creating a struct,
    /// use this expression.
    into_property: Expr,
    /// When reading from a struct and creating datastore properties,
    /// use this expression.
    from_property: Expr,
    // Data used to build datastore getters
    entity_getter: Option<EntityGetter>,
}


fn generate_entity_getter(indexed: bool, property_type: &str, key: &String) -> Option<EntityGetter> {
    if indexed {
        Some(EntityGetter{
            key_type: parse_expr("String"),
            get_one_method_name: parse_expr(&format!("get_one_by_{}", key)),
        })
    } else {
        None
    }
}

#[proc_macro_derive(DatastoreManaged, attributes(kind, key, indexed))]
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
                let mut indexed: bool = false;

                for ref attr in &field.attrs {
                    match attr.parse_meta().unwrap() {
                        syn::Meta::Path(ref path) => {
                            match path.get_ident().unwrap().to_string().as_str() {
                                "key" => {
                                    key_field = Some(field.ident.as_ref().unwrap().clone().to_string());
                                },
                                "indexed" => {
                                    indexed = true;
                                }
                                _ => (),
                            }
                        },
                        _ => (),
                    }
                }
                match &field.ty {
                    syn::Type::Path(p) => {
                        let ident = field.ident.as_ref().unwrap().clone();
                        let key = ident.to_string();
                        match p.path.segments.first().unwrap().ident.to_string().as_str() {
                            "String" => {
                                let into_property_expr_string = format!("properties.get_string(\"{}\")", key);
                                let from_property_expr_string = format!("properties.set_string(\"{}\", entity.{})", key, key);
                                field_metas.push(FieldMeta {
                                    ident,
                                    key: parse_expr(&format!("\"{}\"", key)),
                                    into_property: parse_expr(&into_property_expr_string),
                                    from_property: parse_expr(&from_property_expr_string),
                                    entity_getter: generate_entity_getter(indexed, "String", &key),
                                });
                            },
                            "i64" => {
                                let into_property_expr_string = format!("properties.get_integer(\"{}\")", key);
                                let from_property_expr_string = format!("properties.set_integer(\"{}\", entity.{})", key, key);
                                field_metas.push(FieldMeta {
                                    ident,
                                    key: parse_expr(&format!("\"{}\"", key)),
                                    into_property: parse_expr(&into_property_expr_string),
                                    from_property: parse_expr(&from_property_expr_string),
                                    entity_getter: generate_entity_getter(indexed, "i64", &key),
                                });
                            },
                            "bool" => {
                                let into_property_expr_string = format!("properties.get_bool(\"{}\")", key);
                                let from_property_expr_string = format!("properties.set_bool(\"{}\", entity.{})", key, key);
                                field_metas.push(FieldMeta {
                                    ident,
                                    key: parse_expr(&format!("\"{}\"", key)),
                                    into_property: parse_expr(&into_property_expr_string),
                                    from_property: parse_expr(&from_property_expr_string),
                                    entity_getter: generate_entity_getter(indexed, "bool", &key),
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
    let keys = fields.iter().map(|f| f.key.clone()).collect::<Vec<_>>();

    let into_properties = fields.iter().map(|f| f.into_property.clone()).collect::<Vec<_>>();
    let from_properties = fields.iter().map(|f| f.from_property.clone()).collect::<Vec<_>>();

    let kind_str = kind.unwrap();
    let key_field_str = key_field.unwrap();
    let key_field_expr = parse_expr(&key_field_str);
    let self_key_field_expr = parse_expr(&format!("self.{}.as_ref()", key_field_str));
    let entity_key_field_expr = parse_expr(&format!("entity.{}", key_field_str));

    let entity_getters = fields.iter()
        .filter(|f| f.entity_getter.is_some())
        .map(|f| f.entity_getter.as_ref().unwrap().get_one_method_name.clone())
        .collect::<Vec<_>>();
    let entity_getter_key_types = fields.iter()
        .filter(|f| f.entity_getter.is_some())
        .map(|f| f.entity_getter.as_ref().unwrap().key_type.clone())
        .collect::<Vec<_>>();
    let kind_strs = repeat(kind_str.clone());
    let names = repeat(name);

    let tokens = quote! {
        impl #name {
            pub const fn kind(&self) -> &'static str {
                #kind_str
            }

            pub fn id(&self) -> core::option::Option<&google_datastore1::schemas::Key> {
                #self_key_field_expr
            }

            pub fn get_one_by_id<A>(id: i64, token: A, project_name: &String) -> Result<#name, String> 
                where A: ::google_api_auth::GetAccessToken + 'static
            {
                let datastoreEntity = datastore_entity::get_one_by_id(id, #kind_str.to_string(), token, project_name)?;
                let result: #name = datastoreEntity
                    .try_into()
                    .map_err(|_e: <#name as std::convert::TryFrom<DatastoreEntity>>::Error| -> String {"Failed to fetch entity".to_string()})
                    .unwrap();
                return Ok(result)
            }
            #(
                // TODO - only do this for indexed properties
                pub fn #entity_getters<A>(value: #entity_getter_key_types, token: A, project_name: &String) -> Result<#name, String> 
                    where A: ::google_api_auth::GetAccessToken + 'static
                {
                    let datastoreEntity = datastore_entity::get_one_by_property(#keys.to_string(), value, #kind_str.to_string(), token, project_name)?;
                    let result: #name = datastoreEntity
                        .try_into()
                        .map_err(|e: <#name as std::convert::TryFrom<DatastoreEntity>>::Error| -> String {
                            println!("Error fetching data {:?}", e);
                            return "Failed to fetch entity".to_string();
                        })
                        .unwrap();
                    return Ok(result)
                }
            )* 
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
