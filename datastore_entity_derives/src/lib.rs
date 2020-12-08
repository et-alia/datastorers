// Based on this blog post
// https://cprimozic.net/blog/writing-a-hashmap-to-struct-procedural-macro-in-rust/
#![recursion_limit = "128"]

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Expr, Ident};

struct EntityGetter {
    // Property name in the datastore entity
    datastore_property: Expr,
    // Property type
    property_type: Expr,
    get_one_method_name: Expr,
}

struct FieldMeta {
    // Property name in the rust struct
    ident: Ident,
    /// When reading from datastore properties and creating a struct,
    /// use this expression.
    into_property: Expr,
    /// When reading from a struct and creating datastore properties,
    /// use this expression.
    from_property: Expr,
    // Data used to build datastore getters
    entity_getter: Option<EntityGetter>,
}

fn generate_entity_getter(
    indexed: bool,
    property_type: &str,
    struct_prop_name: &String,
    ds_prop_name: &String,
) -> Option<EntityGetter> {
    if indexed {
        Some(EntityGetter {
            property_type: parse_expr(property_type),
            get_one_method_name: parse_expr(&format!("get_one_by_{}", struct_prop_name)),
            datastore_property: parse_expr(&format!("\"{}\"", ds_prop_name)),
        })
    } else {
        None
    }
}

#[proc_macro_derive(DatastoreManaged, attributes(kind, key, indexed, property))]
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
                            }
                            _ => (),
                        }
                    }
                    _ => (),
                }
            }

            for ref field in vdata.fields.iter() {
                let mut indexed: bool = false;
                let mut property_name: Option<String> = None;

                for ref attr in &field.attrs {
                    match attr.parse_meta().unwrap() {
                        syn::Meta::Path(ref path) => {
                            match path.get_ident().unwrap().to_string().as_str() {
                                "key" => {
                                    key_field =
                                        Some(field.ident.as_ref().unwrap().clone().to_string());
                                }
                                "indexed" => {
                                    indexed = true;
                                }
                                _ => (),
                            }
                        }
                        syn::Meta::NameValue(ref name_value) => {
                            match name_value.path.get_ident().unwrap().to_string().as_str() {
                                "property" => match &name_value.lit {
                                    syn::Lit::Str(lit_str) => {
                                        property_name = Some(lit_str.value());
                                    }
                                    _ => panic!("invalid value type for property attribute"),
                                },
                                _ => (),
                            }
                        }
                        _ => (),
                    }
                }
                match &field.ty {
                    syn::Type::Path(p) => {
                        let ident = field.ident.as_ref().unwrap().clone();
                        let struct_property_name = ident.to_string();
                        let datastore_property_name =
                            property_name.unwrap_or(struct_property_name.clone());
                        match p.path.segments.first().unwrap().ident.to_string().as_str() {
                            "String" => {
                                let into_property_expr_string = format!(
                                    "properties.get_string(\"{}\")",
                                    datastore_property_name
                                );
                                let from_property_expr_string = format!(
                                    "properties.set_string(\"{}\", entity.{})",
                                    datastore_property_name, struct_property_name
                                );
                                field_metas.push(FieldMeta {
                                    ident,
                                    into_property: parse_expr(&into_property_expr_string),
                                    from_property: parse_expr(&from_property_expr_string),
                                    entity_getter: generate_entity_getter(
                                        indexed,
                                        "String",
                                        &struct_property_name,
                                        &datastore_property_name,
                                    ),
                                });
                            }
                            "i64" => {
                                let into_property_expr_string = format!(
                                    "properties.get_integer(\"{}\")",
                                    datastore_property_name
                                );
                                let from_property_expr_string = format!(
                                    "properties.set_integer(\"{}\", entity.{})",
                                    datastore_property_name, struct_property_name
                                );
                                field_metas.push(FieldMeta {
                                    ident,
                                    into_property: parse_expr(&into_property_expr_string),
                                    from_property: parse_expr(&from_property_expr_string),
                                    entity_getter: generate_entity_getter(
                                        indexed,
                                        "i64",
                                        &struct_property_name,
                                        &datastore_property_name,
                                    ),
                                });
                            }
                            "bool" => {
                                let into_property_expr_string =
                                    format!("properties.get_bool(\"{}\")", datastore_property_name);
                                let from_property_expr_string = format!(
                                    "properties.set_bool(\"{}\", entity.{})",
                                    datastore_property_name, struct_property_name
                                );
                                field_metas.push(FieldMeta {
                                    ident,
                                    into_property: parse_expr(&into_property_expr_string),
                                    from_property: parse_expr(&from_property_expr_string),
                                    entity_getter: generate_entity_getter(
                                        indexed,
                                        "bool",
                                        &struct_property_name,
                                        &datastore_property_name,
                                    ),
                                });
                            }
                            _ => (), // Ignore
                        }
                    }
                    _ => (), // Ignore
                }
            }
            field_metas
        }
        syn::Data::Enum(_) => panic!("You can only derive this on structs!"),
        syn::Data::Union(_) => panic!("You can only derive this on structs!"),
    };

    let name = &ast.ident;
    let idents = fields.iter().map(|f| f.ident.clone()).collect::<Vec<_>>();

    let into_properties = fields
        .iter()
        .map(|f| f.into_property.clone())
        .collect::<Vec<_>>();
    let from_properties = fields
        .iter()
        .map(|f| f.from_property.clone())
        .collect::<Vec<_>>();

    let kind_str = kind.unwrap();
    let key_field_str = key_field.unwrap();
    let key_field_expr = parse_expr(&key_field_str);
    let self_key_field_expr = parse_expr(&format!("self.{}.as_ref()", key_field_str));
    let entity_key_field_expr = parse_expr(&format!("entity.{}", key_field_str));

    let entity_getters = fields
        .iter()
        .filter(|f| f.entity_getter.is_some())
        .map(|f| {
            f.entity_getter
                .as_ref()
                .unwrap()
                .get_one_method_name
                .clone()
        })
        .collect::<Vec<_>>();
    let entity_getter_key_types = fields
        .iter()
        .filter(|f| f.entity_getter.is_some())
        .map(|f| f.entity_getter.as_ref().unwrap().property_type.clone())
        .collect::<Vec<_>>();
    let ds_property_names = fields
        .iter()
        .filter(|f| f.entity_getter.is_some())
        .map(|f| f.entity_getter.as_ref().unwrap().datastore_property.clone())
        .collect::<Vec<_>>();

    let tokens = quote! {
        impl #name {
            pub const fn kind(&self) -> &'static str {
                #kind_str
            }

            pub fn id(&self) -> core::option::Option<&google_datastore1::schemas::Key> {
                #self_key_field_expr
            }

            pub fn get_one_by_id<T>(id: i64, connection: &T) -> Result<#name, datastore_entity::DatastorersError>
                where T: datastore_entity::DatastoreConnection
            {
                let datastoreEntity = datastore_entity::get_one_by_id(id, #kind_str.to_string(), connection)?;
                let result: #name = datastoreEntity
                    .try_into()?;
                return Ok(result)
            }
            #(
                pub fn #entity_getters<T>(value: #entity_getter_key_types, connection: &T) -> Result<#name, datastore_entity::DatastorersError>
                    where T: datastore_entity::DatastoreConnection
                {
                    let datastoreEntity = datastore_entity::get_one_by_property(#ds_property_names.to_string(), value, #kind_str.to_string(), connection)?;
                    let result: #name = datastoreEntity
                        .try_into()?;
                    return Ok(result)
                }
            )*

            pub fn commit<T>(self, connection: &T) -> Result<#name, datastore_entity::DatastorersError>
                where T: datastore_entity::DatastoreConnection
            {
                let result_entity = datastore_entity::commit_one(
                    self.into(),
                    #kind_str.to_string(),
                    connection
                )?;
                let result: #name = result_entity
                    .try_into()?;
                return Ok(result)
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
