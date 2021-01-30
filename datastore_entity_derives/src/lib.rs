#![recursion_limit = "128"]
#![allow(clippy::single_match)]

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Expr, Ident, Lit, Meta, Type, TypePath};

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

struct KeyProperty {
    name: String,
    tp: TypePath,
}

#[proc_macro_derive(
    DatastoreManaged,
    attributes(kind, key, indexed, property, page_size, version)
)]
pub fn datastore_managed(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as DeriveInput);

    let mut kind: Option<String> = None;
    let mut version_field: Option<String> = None;
    let mut key_field: Option<KeyProperty> = None;
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
                                    if let Type::Path(tp) = &field.ty {
                                        key_field = Some(KeyProperty {
                                            name: field.ident.as_ref().unwrap().clone().to_string(),
                                            tp: tp.clone(),
                                        });
                                    }
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
                                if key_field.name == struct_property_name {
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
    let key_property = key_field.unwrap();
    let self_key_field_expr = parse_expr(&format!("&self.{}", key_property.name));
    let entity_key_field_expr = parse_expr(&format!("entity.{}", key_property.name));
    let key_field_type = key_property.tp;

    let mut entity_version = parse_expr("None");
    let mut meta_field_assignements = vec![parse_expr(&format!("{}: key", &key_property.name))];
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

        impl datastorers::Pagable for #name {
            fn page_size() -> Option<i32> {
                #page_size
            }
        }

        impl #name {
            pub fn id(&self) -> &#key_field_type {
                #self_key_field_expr
            }

            pub async fn get_one_by_id(connection: &impl datastorers::DatastoreConnection, key_path: &#key_field_type) -> Result<#name, datastorers::DatastorersError>
            {
                use std::convert::TryInto;
                let datastore_entity = datastorers::get_one_by_id(connection, key_path).await?;
                let result: #name = datastore_entity
                    .try_into()?;
                return Ok(result)
            }
            #(
                pub async fn #entity_getters(connection: &impl datastorers::DatastoreConnection, value: impl datastorers::serialize::Serialize) -> Result<#name, datastorers::DatastorersError>
                {
                    use std::convert::TryInto;
                    let datastore_entity = datastorers::get_one_by_property(connection, #ds_property_names.to_string(), value, #kind_str.to_string()).await?;
                    let result: #name = datastore_entity
                        .try_into()?;
                    return Ok(result)
                }
            )*

            #(
                pub async fn #entity_collection_getters(connection: &impl datastorers::DatastoreConnection, value: impl datastorers::serialize::Serialize) -> Result<datastorers::ResultCollection<#name>, datastorers::DatastorersError>
                {
                    use std::convert::TryInto;
                    let entities = datastorers::get_by_property(connection, #ds_property_names.to_string(), value, #kind_str.to_string(), #page_size).await?;
                    let result: datastorers::ResultCollection<#name> = entities
                        .try_into()?;
                    return Ok(result)
                }
            )*

            pub async fn commit(self, connection: &impl datastorers::DatastoreConnection) -> Result<#name, datastorers::DatastorersError>
            {
                use std::convert::TryInto;
                let result_entity = datastorers::commit_one(
                    connection,
                    self.try_into()?,
                ).await?;
                let result: #name = result_entity
                    .try_into()?;
                return Ok(result)
            }

            pub async fn delete(self, connection: &impl datastorers::DatastoreConnection) -> Result<(), datastorers::DatastorersError>
            {
                use std::convert::TryInto;
                datastorers::delete_one(connection, self.try_into()?).await
            }
        }

        impl core::convert::TryFrom<datastorers::DatastoreEntity> for #name {
            type Error = datastorers::DatastorersError;

            fn try_from(mut entity: datastorers::DatastoreEntity) -> Result<Self, Self::Error> {
                use std::convert::TryInto;
                let key = entity
                    .key()
                    .ok_or(datastorers::DatastoreKeyError::NoKey)?
                    .try_into()?;
                let version = entity.version();
                let mut properties = datastorers::DatastoreProperties::try_from(entity)?;
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
                use datastorers::KeyPath;
                let mut properties = datastorers::DatastoreProperties::new();
                #(
                    #from_properties?;
                )*

                Ok(
                    datastorers::DatastoreEntity::from(
                        Some(#entity_key_field_expr.get_key()),
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
