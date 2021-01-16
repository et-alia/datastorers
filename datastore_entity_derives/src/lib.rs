#![recursion_limit = "128"]
#![allow(clippy::single_match)]

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Expr, Ident, Lit, Meta, Type};

struct EntityGetter {
    // Property name in the datastore entity
    datastore_property: Expr,
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

fn build_field_meta(
    ident: Ident,
    datastore_property_name: &str,
    struct_property_name: &str,
    indexed: bool,
) -> FieldMeta {
    let into_property_expr_string = format!("properties.get(\"{}\")", datastore_property_name);
    let from_property_expr_string = format!(
        "properties.set(\"{}\", entity.{})",
        datastore_property_name, struct_property_name
    );
    let entity_getter = match indexed {
        true => Some(EntityGetter {
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
        entity_getter,
    }
}

#[proc_macro_derive(
    DatastoreManaged,
    attributes(kind, key, indexed, property, page_size, version)
)]
pub fn datastore_managed(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as DeriveInput);

    let mut kind: Option<String> = None;
    let mut version_field: Option<String> = None;
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
                            }
                            "page_size" => {
                                if let Lit::Int(lit_int) = name_value.lit.clone() {
                                    page_size = parse_expr(&format!("Some({})", lit_int));
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
                        Meta::Path(ref path) => {
                            match path.get_ident().unwrap().to_string().as_str() {
                                "key" => {
                                    key_field =
                                        Some(field.ident.as_ref().unwrap().clone().to_string());
                                }
                                "version" => {
                                    version_field =
                                        Some(field.ident.as_ref().unwrap().clone().to_string());
                                }
                                "indexed" => {
                                    indexed = true;
                                }
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
                    Type::Path(_) => {
                        let ident = field.ident.as_ref().unwrap().clone();
                        let struct_property_name = ident.to_string();
                        match &version_field {
                            Some(version_field) => {
                                // Ignore version field if set
                                if version_field == &struct_property_name {
                                    continue;
                                }
                            }
                            None => (),
                        }
                        match &key_field {
                            Some(key_field) => {
                                // Ignore key field if set
                                if key_field == &struct_property_name {
                                    continue;
                                }
                            }
                            None => (),
                        }
                        let datastore_property_name =
                            property_name.unwrap_or_else(|| struct_property_name.clone());

                        field_metas.push(build_field_meta(
                            ident,
                            &datastore_property_name,
                            &struct_property_name,
                            indexed,
                        ));
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
    let self_key_field_expr = parse_expr(&format!("self.{}.as_ref()", key_field_str));
    let entity_key_field_expr = parse_expr(&format!("entity.{}", key_field_str));

    let mut entity_version = parse_expr("None");
    let mut meta_field_assignements = vec![parse_expr(&format!("{}: key", &key_field_str))];
    if let Some(version) = version_field {
        meta_field_assignements.push(parse_expr(&format!("{}: version", &version)));
        entity_version = parse_expr(&format!("entity.{}", &version));
    }

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
        .map(|f| f.entity_getter.as_ref().unwrap().get_method_name.clone())
        .collect::<Vec<_>>();
    let ds_property_names = fields
        .iter()
        .filter(|f| f.entity_getter.is_some())
        .map(|f| f.entity_getter.as_ref().unwrap().datastore_property.clone())
        .collect::<Vec<_>>();

    let tokens = quote! {
        impl datastorers::Kind for #name {
            fn kind(&self) -> &'static str {
                #kind_str
            }

            fn kind_str() -> &'static str {
                #kind_str
            }
        }
        impl #name {
            pub fn id(&self) -> std::option::Option<&google_datastore1::schemas::Key> {
                #self_key_field_expr
            }

            pub async fn get_one_by_id(id: i64, connection: &impl datastorers::DatastoreConnection) -> Result<#name, datastorers::DatastorersError>
            {
                let datastore_entity = datastorers::get_one_by_id(id, #kind_str.to_string(), connection).await?;
                let result: #name = datastore_entity
                    .try_into()?;
                return Ok(result)
            }
            #(
                pub async fn #entity_getters(value: impl datastorers::serialize::Serialize, connection: &impl datastorers::DatastoreConnection) -> Result<#name, datastorers::DatastorersError>
                {
                    let datastore_entity = datastorers::get_one_by_property(#ds_property_names.to_string(), value, #kind_str.to_string(), connection).await?;
                    let result: #name = datastore_entity
                        .try_into()?;
                    return Ok(result)
                }
            )*

            #(
                pub async fn #entity_collection_getters(value: impl datastorers::serialize::Serialize, connection: &impl datastorers::DatastoreConnection) -> Result<datastorers::ResultCollection<#name>, datastorers::DatastorersError>
                {
                    let entities = datastorers::get_by_property(#ds_property_names.to_string(), value, #kind_str.to_string(), #page_size, connection).await?;
                    let result: datastorers::ResultCollection<#name> = entities
                        .try_into()?;
                    return Ok(result)
                }
            )*

            pub async fn commit(self, connection: &impl datastorers::DatastoreConnection) -> Result<#name, datastorers::DatastorersError>
            {
                let result_entity = datastorers::commit_one(
                    self.try_into()?,
                    connection
                ).await?;
                let result: #name = result_entity
                    .try_into()?;
                return Ok(result)
            }

            pub async fn delete(self, connection: &impl datastorers::DatastoreConnection) -> Result<(), datastorers::DatastorersError>
            {
                datastorers::delete_one(self.try_into()?, connection).await
            }
        }

        impl core::convert::TryFrom<datastorers::DatastoreEntity> for #name {
            type Error = datastorers::DatastorersError;

            fn try_from(mut entity: datastorers::DatastoreEntity) -> Result<Self, Self::Error> {
                let key = entity.key();
                let version = entity.version();
                let mut properties = datastorers::DatastoreProperties::try_from(entity)?;
                fn optional_ok<T>(val: T) -> Result<Option<T>, datastorers::DatastoreParseError> {
                    Ok(Some(val))
                }
                fn optional_err<T>(err: datastorers::DatastoreParseError) -> Result<Option<T>, datastorers::DatastoreParseError> {
                    if err == datastorers::DatastoreParseError::NoSuchValue {
                        // Vale not set => the optional representation is None
                        Ok(None)
                    } else {
                        Err(err) // Forward the error
                    }
                }
                Ok(
                    #name {
                        #(
                            #meta_field_assignements,
                        )*
                        #(
                            #idents: #into_properties?,
                        )*
                    }
                )
            }
        }

        impl core::convert::TryFrom<#name> for datastorers::DatastoreEntity {
            type Error = datastorers::DatastorersError;

            fn try_from(entity: #name) -> Result<Self, Self::Error> {
                let mut properties = datastorers::DatastoreProperties::new();
                #(
                    #from_properties?;
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

                Ok(
                    datastorers::DatastoreEntity::from(
                        #entity_key_field_expr.or_else(generate_empty_key),
                        properties,
                        #entity_version,
                    )
                )
            }
        }
    };

    TokenStream::from(tokens)
}

fn parse_expr(expr_string: &str) -> Expr {
    syn::parse_str::<Expr>(expr_string).expect("failed to parse expression")
}
