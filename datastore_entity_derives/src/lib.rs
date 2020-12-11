// Based on this blog post
// https://cprimozic.net/blog/writing-a-hashmap-to-struct-procedural-macro-in-rust/
#![recursion_limit = "128"]

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Expr, GenericArgument, Ident, Lit, Meta, PathArguments, Type, TypePath,
};

struct EntityGetter {
    // Property name in the datastore entity
    datastore_property: Expr,
    // Property type
    property_type: Expr,
    get_one_method_name: Expr,
    get_method_name: Expr,
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

fn get_generic_argument(typepath: &TypePath) -> Option<&TypePath> {
    let type_params = &typepath.path.segments.first().unwrap().arguments;
    let generic_arg = match type_params {
        PathArguments::AngleBracketed(params) => params.args.first().unwrap(),
        _ => panic!("Expected a generic type argument"),
    };
    // This argument must be a type:
    match generic_arg {
        GenericArgument::Type(ty) => match ty {
            Type::Path(p) => Some(p),
            _ => None,
        },
        _ => None,
    }
}

fn build_proprty_expr(
    datastore_property_name: &String,
    struct_property_name: &String,
    optional: bool,
    property_operator_suffix: &String,
) -> (String, String) {
    if optional {
        (
            format!(
                "properties.get_{}(\"{}\").map_or_else(optional_err, optional_ok)",
                property_operator_suffix, datastore_property_name
            ),
            format!(
                "if let Some(val) = entity.{} {{properties.set_{}(\"{}\", val);}}",
                struct_property_name, property_operator_suffix, datastore_property_name
            )
        )
    } else {
        (
            format!(
                "properties.get_{}(\"{}\")",
                property_operator_suffix, datastore_property_name
            ),
            format!(
                "properties.set_{}(\"{}\", entity.{})",
                property_operator_suffix, datastore_property_name, struct_property_name
            )
        )
    }
}

fn build_field_meta(
    ident: Ident,
    datastore_property_name: &String,
    struct_property_name: &String,
    optional: bool,
    property_operator_suffix: &String,
    indexed: bool,
    getter_value_type: &'static str,
) -> FieldMeta {
    
    let (into_property_expr_string, from_property_expr_string) = build_proprty_expr(
        datastore_property_name,
        struct_property_name,
        optional,
        property_operator_suffix,
    );
    let entity_getter = match indexed {
        true => Some(EntityGetter {
            property_type: parse_expr(getter_value_type),
            get_one_method_name: parse_expr(&format!("get_one_by_{}", struct_property_name)),
            get_method_name: parse_expr(&format!("get_by_{}", struct_property_name)),
            datastore_property: parse_expr(&format!("\"{}\"", datastore_property_name)),
        }),
        false => None,
    };
    FieldMeta {
        ident,
        into_property: parse_expr(&into_property_expr_string),
        from_property: parse_expr(&from_property_expr_string),
        entity_getter: entity_getter,
    }
}

fn recurse_property_path(
    path: &TypePath
) -> Option<(&'static str, &'static str, &'static str, bool)> {
    recurse_property(
        Some(path),
        path.path.segments.first().unwrap().ident.to_string().as_str(),
        "",
        false
    )
}

fn recurse_property(
    path: Option<&TypePath>,
    segment_str: &str,
    getter_suffix: &'static str,
    optional: bool
) -> Option<(&'static str, &'static str, &'static str, bool)> {
    match segment_str {
        "String" => Some(("string", "String", getter_suffix, optional)),
        "i64" => Some(("integer", "i64", getter_suffix, optional)),
        "bool" => Some(("bool", "bool", getter_suffix, optional)),
        "Vec" => recurse_generic(path, "_array", optional),
        "Option" => recurse_generic(path, getter_suffix, true),
        _ => None, // Ignore
    }
}

fn recurse_generic(
    path: Option<&TypePath>,
    getter_suffix: &'static str,
    optional: bool
) -> Option<(&'static str, &'static str, &'static str, bool)> {
    if let Some(p) = path {
        if let Some(generic) = get_generic_argument(p) {
            let generic_type = generic.path.segments.first().unwrap().ident.to_string();
            recurse_property(
                Some(generic),
                &generic_type,
                getter_suffix,
                optional
            )
        } else {
            // No valid generic type set, no need to continue iteration
            None
        }
    } else {
        // No path, no need to go deeper
        None
    }
}

#[proc_macro_derive(DatastoreManaged, attributes(kind, key, indexed, property, page_size))]
pub fn datastore_managed(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as DeriveInput);

    let mut kind: Option<String> = None;
    let mut key_field: Option<String> = None;
    let mut page_size: Expr = parse_expr("None");

    let fields: Vec<FieldMeta> = match ast.data {
        Data::Struct(vdata) => {
            let mut field_metas = Vec::new();

            for ref attr in ast.attrs {
                match attr.parse_meta().unwrap() {
                    Meta::NameValue(ref name_value) => {
                        match name_value.path.get_ident().unwrap().to_string().as_str() {
                            "kind" => {
                                if let Lit::Str(lit_str) = name_value.lit.clone() {
                                    kind = Some(lit_str.value());
                                }
                            },
                            "page_size" => {
                                if let Lit::Int(lit_int) = name_value.lit.clone() {
                                    page_size = parse_expr(&format!("Some({})", lit_int));
                                }
                            },
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
                        Meta::Path(ref path) => {
                            match path.get_ident().unwrap().to_string().as_str() {
                                "key" => {
                                    key_field =
                                        Some(field.ident.as_ref().unwrap().clone().to_string());
                                },
                                "indexed" => {
                                    indexed = true;
                                },
                                _ => (),
                            }
                        }
                        Meta::NameValue(ref name_value) => {
                            match name_value.path.get_ident().unwrap().to_string().as_str() {
                                "property" => match &name_value.lit {
                                    Lit::Str(lit_str) => {
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
                    Type::Path(p) => {
                        let ident = field.ident.as_ref().unwrap().clone();
                        let struct_property_name = ident.to_string();
                        let datastore_property_name = property_name.unwrap_or(struct_property_name.clone());

                        if let Some(property_data) = recurse_property_path(&p) {
                            let (property_operator_suffix, getter_value_type, operator_suffix, optional) = property_data;
                           
                            field_metas.push(build_field_meta(
                                ident,
                                &datastore_property_name,
                                &struct_property_name,
                                optional,
                                &format!("{}{}", property_operator_suffix, operator_suffix),
                                indexed,
                                getter_value_type,
                            ));
                        }
                    }
                    _ => (), // Ignore
                }
            }
            field_metas
        }
        Data::Enum(_) => panic!("You can only derive this on structs!"),
        Data::Union(_) => panic!("You can only derive this on structs!"),
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
    let entity_collection_getters = fields
        .iter()
        .filter(|f| f.entity_getter.is_some())
        .map(|f| {
            f.entity_getter
                .as_ref()
                .unwrap()
                .get_method_name
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

            pub fn id(&self) -> std::option::Option<&google_datastore1::schemas::Key> {
                #self_key_field_expr
            }

            pub fn get_one_by_id(id: i64, connection: &impl datastore_entity::DatastoreConnection) -> Result<#name, datastore_entity::DatastorersError>
            {
                let datastore_entity = datastore_entity::get_one_by_id(id, #kind_str.to_string(), connection)?;
                let result: #name = datastore_entity
                    .try_into()?;
                return Ok(result)
            }
            #(
                pub fn #entity_getters(value: #entity_getter_key_types, connection: &impl datastore_entity::DatastoreConnection) -> Result<#name, datastore_entity::DatastorersError>
                {
                    let datastore_entity = datastore_entity::get_one_by_property(#ds_property_names.to_string(), value, #kind_str.to_string(), connection)?;
                    let result: #name = datastore_entity
                        .try_into()?;
                    return Ok(result)
                }
            )*

            #(
                pub fn #entity_collection_getters(value: #entity_getter_key_types, connection: &impl datastore_entity::DatastoreConnection) -> Result<datastore_entity::ResultCollection<#name>, datastore_entity::DatastorersError>
                {
                    let entities = datastore_entity::get_by_property(#ds_property_names.to_string(), value, #kind_str.to_string(), #page_size, connection)?;
                    let result: datastore_entity::ResultCollection<#name> = entities
                        .try_into()?;
                    return Ok(result)
                }
            )*

            pub fn commit(self, connection: &impl datastore_entity::DatastoreConnection) -> Result<#name, datastore_entity::DatastorersError>
            {
                let result_entity = datastore_entity::commit_one(
                    self.into(),
                    connection
                )?;
                let result: #name = result_entity
                    .try_into()?;
                return Ok(result)
            }

            pub fn delete(self, connection: &impl datastore_entity::DatastoreConnection) -> Result<(), datastore_entity::DatastorersError>
            {
                datastore_entity::delete_one(self.into(), connection)
            }
        }

        impl core::convert::TryFrom<datastore_entity::DatastoreEntity> for #name {
            type Error = datastore_entity::DatastoreParseError;

            fn try_from(mut entity: datastore_entity::DatastoreEntity) -> Result<Self, Self::Error> {
                let key = entity.key();
                let mut properties = datastore_entity::DatastoreProperties::try_from(entity)?;

                fn optional_ok<T>(val: T) -> Result<Option<T>, datastore_entity::DatastoreParseError> {
                    Ok(Some(val))
                }
                fn optional_err<T>(err: datastore_entity::DatastoreParseError) -> Result<Option<T>, datastore_entity::DatastoreParseError> {
                    if err == datastore_entity::DatastoreParseError::NoSuchValue {
                        // Vale not set => the optional representation is None
                        Ok(None)
                    } else {
                        Err(err) // Forward the error
                    }
                }
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

                fn generate_empty_key() -> std::option::Option<google_datastore1::schemas::Key> {
                    Some(google_datastore1::schemas::Key {
                        partition_id: None,
                        path: Some(vec![google_datastore1::schemas::PathElement {
                            id: None,
                            kind: Some(#kind_str.to_string()),
                            name: None,
                        }]),
                    })
                }

                datastore_entity::DatastoreEntity::from(
                    #entity_key_field_expr.or_else(generate_empty_key),
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
