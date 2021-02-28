use crate::entity::Kind;
use crate::error::DatastoreKeyError;
use crate::{DatastoreNameRepresentationError, DatastorersError};
use google_datastore1::schemas;
use google_datastore1::schemas::Key;
use std::convert::TryFrom;
use std::marker::PhantomData;

/// A KeyPath knows how to create a [Key](schemas::Key) from itself.
/// There are only three valid identifiers that implement KeyPath so far
/// and together they model the Datastore API fully:
/// * [IdentifierId](IdentifierId) for an identifier of "key id" type (i64)
/// * [IdentifierName](IdentifierName) for an identifier of "key name" type (String)
/// * [IdentifierNone](IdentifierNone) for the tail end of a key path type
pub trait KeyPath {
    /// Start the process of converting an Identifier chain into a [Key](schemas::Key).
    fn get_key(&self) -> schemas::Key;
}

/// A helper trait for being able to create a [Key](schemas::Key) using the [KeyPath](KeyPath) trait,
pub trait KeyPathElement: Sized {
    /// The identifier (id, name, or none) of this KeyPathElement.
    fn identifier(&self) -> Identifier;

    /// The kind of this KeyPathElement
    fn kind(&self) -> &'static str;

    /// Append this KeyPathElement as a [PathElement](schemas::PathElement) in the given
    /// [Key](schemas::Key).
    fn fill_key(&self, key: &mut schemas::Key);

    /// Convert a slice of [PathElements](schemas::PathElement) to a KeyPathElement.
    /// Any implementation should read the [PathElement](schemas::PathElement) at the
    /// given index, unless it is one past the end of the slice. The last element is allowed
    /// to be "empty" (no id, no name), every other element must have either an id or a name
    /// and the identifier must match the type it is parsing into:
    /// * id for [IdentifierId](IdentifierId)
    /// * name for [IdentifierName](IdentifierName)
    fn from_path_elements(
        path_elements: &[schemas::PathElement],
        index: usize,
        path_length: usize,
    ) -> Result<Self, DatastorersError>;
}

impl<T> KeyPath for T
where
    T: KeyPathElement,
{
    fn get_key(&self) -> schemas::Key {
        let mut key = schemas::Key {
            partition_id: None,
            path: Some(Vec::new()),
        };
        self.fill_key(&mut key);
        key
    }
}

/// Models the valid Datastore identifiers that can be found in [PathElement](schemas::PathElement)
pub enum Identifier {
    Id(i64),
    Name(String),
    None,
}

/// This type is an id identifier in a key path
///
/// Example:
/// ```
/// # use datastorers::*;
/// #[derive(DatastoreManaged)]
/// #[kind="my_entity"]
/// struct MyEntity {
///     /// A key path with no ancestors and an identifier id of type i64
///     #[key]
///     key: IdentifierId<Self>,
/// }
///
/// #[derive(DatastoreManaged)]
/// #[kind="my_child_entity"]
/// struct MyChildEntity {
///     /// A key path with "MyEntity" as ancestors and an identifier id of type i64
///     #[key]
///     key: IdentifierId<MyEntity, IdentifierId<Self>>,
/// }
/// ```
#[derive(Clone, Debug)]
pub struct IdentifierId<T, Child = IdentifierNone>
where
    T: Kind,
    Child: KeyPathElement + PartialEq,
{
    pub id: Option<i64>,
    pub child: Box<Child>,
    phantom_kind: PhantomData<T>,
}

impl<T, Child> PartialEq for IdentifierId<T, Child>
where
    T: Kind,
    Child: KeyPathElement + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.child == other.child
    }
}

impl<T, Child> TryFrom<schemas::Key> for IdentifierId<T, Child>
where
    T: Kind,
    Child: KeyPathElement + PartialEq,
{
    type Error = DatastorersError;

    fn try_from(value: Key) -> Result<Self, Self::Error> {
        if let Some(path) = value.path {
            match path.len() {
                0 => Err(DatastoreKeyError::NoKeyPathElement.into()),
                len => Self::from_path_elements(&path, 0, len - 1),
            }
        } else {
            Err(DatastoreKeyError::NoKeyPath.into())
        }
    }
}

impl<T, Child> IdentifierId<T, Child>
where
    T: Kind,
    Child: KeyPathElement + PartialEq,
{
    pub fn id(id: Option<i64>, child: Child) -> Self {
        Self {
            id,
            child: Box::new(child),
            phantom_kind: Default::default(),
        }
    }
}

impl<T, Child> KeyPathElement for IdentifierId<T, Child>
where
    T: Kind,
    Child: KeyPathElement + PartialEq,
{
    fn identifier(&self) -> Identifier {
        match self.id {
            Some(id) => Identifier::Id(id),
            None => Identifier::None,
        }
    }

    fn kind(&self) -> &'static str {
        T::kind_str()
    }

    fn fill_key(&self, key: &mut schemas::Key) {
        match &mut key.path {
            Some(path) => match self.id {
                Some(id) => path.push(schemas::PathElement {
                    id: Some(id),
                    kind: Some(self.kind().to_string()),
                    name: None,
                }),
                None => path.push(schemas::PathElement {
                    id: None,
                    kind: Some(self.kind().to_string()),
                    name: None,
                }),
            },
            None => (),
        }
        self.child.as_ref().fill_key(key);
    }

    fn from_path_elements(
        path_elements: &[schemas::PathElement],
        index: usize,
        path_length: usize,
    ) -> Result<Self, DatastorersError> {
        match path_elements.get(index) {
            Some(path_element) => {
                if let Some(kind) = path_element.kind.as_ref() {
                    if kind == T::kind_str() {
                        if let Some(id) = path_element.id {
                            let child =
                                Child::from_path_elements(path_elements, index + 1, path_length)?;
                            Ok(IdentifierId::id(Some(id), child))
                        } else if index == path_length {
                            let child =
                                Child::from_path_elements(path_elements, index + 1, path_length)?;
                            Ok(IdentifierId::id(None, child))
                        } else {
                            Err(DatastoreKeyError::ExpectedId.into())
                        }
                    } else {
                        Err(DatastoreKeyError::WrongKind {
                            expected: T::kind_str(),
                            found: std::string::ToString::to_string(&kind),
                        }
                        .into())
                    }
                } else {
                    Err(DatastoreKeyError::NoKind.into())
                }
            }
            None => {
                if index == path_length {
                    let child = Child::from_path_elements(path_elements, index + 1, path_length)?;
                    Ok(IdentifierId::id(None, child))
                } else {
                    Err(DatastoreKeyError::NoKeyPathElement.into())
                }
            }
        }
    }
}

/// Must be implemented by types that are used as [IdentifierName](IdentifierName) representations.
pub trait SerializeIdentifierName {
    fn to_string(&self) -> String;
}

/// Must be implemented by types that are used as [IdentifierName](IdentifierName) representations.
pub trait DeserializeIdentifierName: Sized {
    fn from_str(from: &str) -> Result<Self, DatastoreNameRepresentationError>;
}

/// This type is a name identifier in a key path, where the identifier is represented
/// by a String. It is implemented as a type alias of [IdentifierName].
/// See the documentation of [IdentifierName] for information on how
/// to create your own Identifier representation.
///
/// Example:
/// ```
/// # use datastorers::*;
/// #[derive(DatastoreManaged)]
/// #[kind="my_entity"]
/// struct MyEntity {
///     /// A key path with no children and an identifier name of type String
///     #[key]
///     key: IdentifierString<Self>,
/// }
/// ```
pub type IdentifierString<T, Child = IdentifierNone> = IdentifierName<T, String, Child>;

impl SerializeIdentifierName for String {
    fn to_string(&self) -> String {
        self.clone()
    }
}

impl DeserializeIdentifierName for String {
    fn from_str(from: &str) -> Result<Self, DatastoreNameRepresentationError> {
        Ok(from.to_string())
    }
}

/// This is the type that acts as a Name Identifier, typically you
/// won't use this type, but rather a type alias.
///
/// The simplest type alias is [IdentifierString](IdentifierString).
/// You can easily create your own alias, e.g. `IdentifierUuid` if you want.
/// Just implement [SerializeIdentifierName](SerializeIdentifierName) and
/// [DeserializeIdentifierName](DeserializeIdentifierName) for your type,
/// and derive/implement [PartialEq](PartialEq).
/// Currently, `IdentifierUuid` isn't provided.
#[derive(Clone, Debug)]
pub struct IdentifierName<T, Representation, Child = IdentifierNone>
where
    T: Kind,
    Representation: SerializeIdentifierName + DeserializeIdentifierName + PartialEq,
    Child: KeyPathElement + PartialEq,
{
    pub name: Option<Representation>,
    pub child: Box<Child>,
    phantom_kind: PhantomData<T>,
}

impl<T, Representation, Child> PartialEq for IdentifierName<T, Representation, Child>
where
    T: Kind,
    Representation: SerializeIdentifierName + DeserializeIdentifierName + PartialEq,
    Child: KeyPathElement + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.child == other.child
    }
}

impl<T, Representation, Child> TryFrom<schemas::Key> for IdentifierName<T, Representation, Child>
where
    T: Kind,
    Representation: SerializeIdentifierName + DeserializeIdentifierName + PartialEq,
    Child: KeyPathElement + PartialEq,
{
    type Error = DatastorersError;

    fn try_from(value: Key) -> Result<Self, Self::Error> {
        if let Some(path) = value.path {
            match path.len() {
                0 => Err(DatastoreKeyError::NoKeyPathElement.into()),
                len => Self::from_path_elements(&path, 0, len - 1),
            }
        } else {
            Err(DatastoreKeyError::NoKeyPath.into())
        }
    }
}

impl<T, Representation, Child> IdentifierName<T, Representation, Child>
where
    T: Kind,
    Representation: SerializeIdentifierName + DeserializeIdentifierName + PartialEq,
    Child: KeyPathElement + PartialEq,
{
    pub fn name(name: Option<Representation>, child: Child) -> Self {
        Self {
            name,
            child: Box::new(child),
            phantom_kind: Default::default(),
        }
    }
}

impl<T, Representation, Child> KeyPathElement for IdentifierName<T, Representation, Child>
where
    T: Kind,
    Representation: SerializeIdentifierName + DeserializeIdentifierName + PartialEq,
    Child: KeyPathElement + PartialEq,
{
    fn identifier(&self) -> Identifier {
        match &self.name {
            Some(name) => Identifier::Name(name.to_string()),
            None => Identifier::None,
        }
    }

    fn kind(&self) -> &'static str {
        T::kind_str()
    }

    fn fill_key(&self, key: &mut schemas::Key) {
        match &mut key.path {
            Some(path) => match &self.name {
                Some(name) => path.push(schemas::PathElement {
                    id: None,
                    kind: Some(self.kind().to_string()),
                    name: Some(name.to_string()),
                }),
                None => path.push(schemas::PathElement {
                    id: None,
                    kind: Some(self.kind().to_string()),
                    name: None,
                }),
            },
            None => (),
        }
        self.child.as_ref().fill_key(key);
    }

    fn from_path_elements(
        path_elements: &[schemas::PathElement],
        index: usize,
        path_length: usize,
    ) -> Result<Self, DatastorersError> {
        match path_elements.get(index) {
            Some(path_element) => {
                if let Some(kind) = path_element.kind.as_ref() {
                    if kind == T::kind_str() {
                        if let Some(name) = path_element.name.as_ref() {
                            let child =
                                Child::from_path_elements(path_elements, index + 1, path_length)?;
                            let representation = Representation::from_str(&name)?;
                            Ok(IdentifierName::name(Some(representation), child))
                        } else if index == 0 {
                            let child =
                                Child::from_path_elements(path_elements, index + 1, path_length)?;
                            Ok(IdentifierName::name(None, child))
                        } else {
                            Err(DatastoreKeyError::ExpectedName.into())
                        }
                    } else {
                        Err(DatastoreKeyError::WrongKind {
                            expected: T::kind_str(),
                            found: std::string::ToString::to_string(&kind),
                        }
                        .into())
                    }
                } else {
                    Err(DatastoreKeyError::NoKind.into())
                }
            }
            None => {
                if index == 0 {
                    let child = Child::from_path_elements(path_elements, index + 1, path_length)?;
                    Ok(IdentifierName::name(None, child))
                } else {
                    Err(DatastoreKeyError::NoKeyPathElement.into())
                }
            }
        }
    }
}

/// This type is put as the last ancestor to finish of a full Key identifier path.
/// IdentifierNone is the implicit Ancestor argument for [IdentifierId](IdentifierId) and
/// [IdentifierName](IdentifierName).
///
/// Example:
/// ```
/// # use datastorers::*;
/// #[derive(DatastoreManaged)]
/// #[kind="my_entity"]
/// struct MyEntity {
///     // IdentifierNone is implicit, but spelled out here for clarity.
///     #[key]
///     key: IdentifierId<Self, IdentifierNone>,
/// }
/// ```
#[derive(Clone, Debug)]
pub struct IdentifierNone {}

impl IdentifierNone {
    pub fn none() -> Self {
        Self {}
    }
}

impl PartialEq for IdentifierNone {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl KeyPathElement for IdentifierNone {
    fn identifier(&self) -> Identifier {
        Identifier::None
    }

    fn kind(&self) -> &'static str {
        ""
    }

    fn fill_key(&self, _key: &mut schemas::Key) {}

    fn from_path_elements(
        _path_elements: &[schemas::PathElement],
        _index: usize,
        _path_length: usize,
    ) -> Result<Self, DatastorersError> {
        Ok(Self {})
    }
}

/// A macro for simplifying the creation of a chain of [IdentifierId](IdentifierId),
/// [IdentifierName](IdentifierName), or [IdentifierNone](IdentifierNone) structs.
///
/// For example, writing:
/// ```
/// # use datastorers::*;
/// # #[derive(DatastoreManaged)]
/// # #[kind="my_entity"]
/// # struct MyEntity {
/// #     /// A key path with no ancestors and an identifier id of type i64
/// #     #[key]
/// #     key: IdentifierId<Self>,
/// # }
/// let key: IdentifierId<MyEntity> = id![5];
/// ```
/// is much easier than writing:
/// ```
/// # use datastorers::*;
/// # #[derive(DatastoreManaged)]
/// # #[kind="my_entity"]
/// # struct MyEntity {
/// #     /// A key path with no ancestors and an identifier id of type i64
/// #     #[key]
/// #     key: IdentifierId<Self>,
/// # }
/// let key: IdentifierId<MyEntity> = IdentifierId::id(Some(5), IdentifierNone::none());
/// ```
///
/// See also [name!](name).
#[macro_export]
macro_rules! id {
    // something like id![None, name![....
    (None, $($tail:tt)*) => {
        datastorers::IdentifierId::id(None, $($tail)*)
    };
    // something like id![5, name![....
    ($lit: literal, $($tail:tt)*) => {
        datastorers::IdentifierId::id(Some($lit), $($tail)*)
    };
    // something like id![five, name![....
    ($e: expr, $($tail:tt)*) => {
        datastorers::IdentifierId::id(Some($e), $($tail)*)
    };
    // something like id![None]
    (None) => {
        datastorers::IdentifierId::id(None, datastorers::IdentifierNone::none())
    };
    // something like id![5]
    ($lit: literal) => {
        datastorers::IdentifierId::id(Some($lit), datastorers::IdentifierNone::none())
    };
    // something like id![five]
    ($e: expr) => {
        datastorers::IdentifierId::id(Some($e), datastorers::IdentifierNone::none())
    };
}

/// A macro for simplifying the creation of a chain of [IdentifierId](IdentifierId),
/// [IdentifierName](IdentifierName), or [IdentifierNone](IdentifierNone) structs.
///
/// Type aliased identifiers are allowed, e.g. [IdentifierString](IdentifierString).
///
/// For example, writing:
/// ```
/// # use datastorers::*;
/// # #[derive(DatastoreManaged)]
/// # #[kind="my_entity"]
/// # struct MyEntity {
/// #     #[key]
/// #     key: IdentifierString<Self>,
/// # }
/// let key: IdentifierString<MyEntity> = name!["name"];
/// ```
/// is much easier than writing:
/// ```
/// # use datastorers::*;
/// # #[derive(DatastoreManaged)]
/// # #[kind="my_entity"]
/// # struct MyEntity {
/// #     #[key]
/// #     key: IdentifierString<Self>,
/// # }
/// let key: IdentifierString<MyEntity> = IdentifierString::name(Some("name".to_string()), IdentifierNone::none());
/// ```
///
/// See also [id!](id).
#[macro_export]
macro_rules! name {
    // something like name![None, id![....
    (None, $($tail:tt)*) => {
        datastorers::IdentifierName::name(None, $($tail)*)
    };
    // something like name!["name", id![....
    // This case currently only supports Representation
    ($lit: literal, $($tail:tt)*) => {
        datastorers::IdentifierName::name(Some($lit.to_string()), $($tail)*)
    };
    // something like name![name, id![....
    ($e: expr, $($tail:tt)*) => {
        datastorers::IdentifierName::name(Some($e.clone()), $($tail)*)
    };
    // something like name![None]
    (None) => {
        datastorers::IdentifierName::name(None, datastorers::IdentifierNone::none())
    };
    // something like name!["name"]
    // This case currently only supports Representation
    ($lit: literal) => {
        datastorers::IdentifierName::name(Some($lit.to_string()), datastorers::IdentifierNone::none())
    };
    // something like name![name]
    ($e: expr) => {
        datastorers::IdentifierName::name(Some($e.clone()), datastorers::IdentifierNone::none())
    };
}
