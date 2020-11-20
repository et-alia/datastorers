// Based on this blog post
// https://cprimozic.net/blog/writing-a-hashmap-to-struct-procedural-macro-in-rust/
#![recursion_limit = "128"]

extern crate google_datastore1 as datastore1;
extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;
use syn::{DeriveInput, Expr};

#[proc_macro_derive(DatastoreEntity)]
pub fn datastore_entity(input: TokenStream) -> TokenStream {
    // Parse the string representation into a syntax tree
    let ast = syn::parse_macro_input!(input as DeriveInput);

    // create a vector containing the names of all fields on the struct
    let (idents, values) = match ast.data {
        syn::Data::Struct(vdata) => {
            let mut idents = Vec::new();
            let mut values = Vec::new();

            for ref field in vdata.fields.iter() {
                match &field.ty {
                    syn::Type::Path(p) => {
                        match p.path.segments.first().unwrap().ident.to_string().as_str() {
                            "String" => {
                                idents.push(field.ident.clone().unwrap());
                                let expr = match syn::parse_str::<Expr>("parse_string(occ_ent)") {
                                    Ok(v) => v,
                                    Err(_) => panic!("failed to parse expression"),
                                };
                                values.push(expr);
                            },
                            "i32" => {
                                idents.push(field.ident.clone().unwrap());
                                let expr = match syn::parse_str::<Expr>("parse_u32(occ_ent)") {
                                    Ok(v) => v,
                                    Err(_) => panic!("failed to parse expression"),
                                };
                                values.push(expr);
                            },
                            "bool" => {
                                idents.push(field.ident.clone().unwrap());
                                let expr = match syn::parse_str::<Expr>("parse_bool(occ_ent)") {
                                    Ok(v) => v,
                                    Err(_) => panic!("failed to parse expression"),
                                };
                                values.push(expr);
                            },
                            _ => (), // Ignore
                        }
                    },
                    _ => (), // Ignore
                }
            }
            (idents, values)
        },
        syn::Data::Enum(_) => panic!("You can only derive this on structs!"),
        syn::Data::Union(_) => panic!("You can only derive this on structs!"),
    };

    // contains quoted strings containing the struct fields in the same order as
    // the vector of idents.
    let mut keys = Vec::new();
    for ident in idents.iter() {
        keys.push(ident.to_string());
    }

    let name = &ast.ident;

    let tokens = quote! {
        fn parse_string(v: &datastore1::Value) -> String {
            // TODO - handle errors
            v.string_value.as_ref().unwrap().to_string()
        }

        fn parse_u32(v: &datastore1::Value) -> i32 {
            // TODO - handle errors
            v.integer_value.as_ref().unwrap().parse::<i32>().unwrap()
        }

        fn parse_bool(v: &datastore1::Value) -> bool {
            // TODO - handle errors
            v.boolean_value.unwrap()
        }

        impl DatastoreEntity<#name> for #name {
            fn from_result_map(hm: &std::collections::HashMap<String, datastore1::Value>) -> #name {
                // start with the default implementation
                let mut settings = #name::default();
                
                #(                    
                    match hm.get(#keys) {
                        Some(occ_ent) => {
                            // set the corresponding struct field to the value in
                            // the corresponding hashmap if it contains it
                            settings.#idents = #values
                        },
                        None => (),
                    }
                    
                )*

                // return the modified struct
                settings
            }
        }
    };

    TokenStream::from(tokens)
}