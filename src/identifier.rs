use crate::error::DatastoreKeyError;
use crate::{DatastorersError, Kind};
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
    /// given index, unless it is one past the end of the slice. The 0th element is allowed
    /// to be "empty" (no id, no name), every other element must have either an id or a name
    /// and the identifier must match the type it is parsing into:
    /// * id for [IdentifierId](IdentifierId)
    /// * name for [IdentifierName](IdentifierName)
    fn from_path_elements(
        path_elements: &[schemas::PathElement],
        index: usize,
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
/// use datastorers::*;
/// #[derive(DatastoreManaged)]
/// #[kind="my_entity"]
/// struct MyEntity {
///     /// A key path with no ancestors and an identifier id of type i64
///     #[key]
///     key: IdentifierId<Self>,
/// }
/// ```
#[derive(Clone, Debug)]
pub struct IdentifierId<T, Ancestor = IdentifierNone>
where
    T: Kind,
    Ancestor: KeyPathElement + PartialEq,
{
    pub id: Option<i64>,
    pub ancestor: Box<Ancestor>,
    phantom_kind: PhantomData<T>,
}

impl<T, Ancestor> PartialEq for IdentifierId<T, Ancestor>
where
    T: Kind,
    Ancestor: KeyPathElement + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.ancestor == other.ancestor
    }
}

impl<T, Ancestor> TryFrom<schemas::Key> for IdentifierId<T, Ancestor>
where
    T: Kind,
    Ancestor: KeyPathElement + PartialEq,
{
    type Error = DatastorersError;

    fn try_from(value: Key) -> Result<Self, Self::Error> {
        if let Some(path) = value.path {
            Self::from_path_elements(&path, 0)
        } else {
            Err(DatastoreKeyError::NoKeyPath.into())
        }
    }
}

impl<T, Ancestor> IdentifierId<T, Ancestor>
where
    T: Kind,
    Ancestor: KeyPathElement + PartialEq,
{
    pub fn id(id: Option<i64>, ancestor: Ancestor) -> Self {
        Self {
            id,
            ancestor: Box::new(ancestor),
            phantom_kind: Default::default(),
        }
    }
}

impl<T, Ancestor> KeyPathElement for IdentifierId<T, Ancestor>
where
    T: Kind,
    Ancestor: KeyPathElement + PartialEq,
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
        self.ancestor.as_ref().fill_key(key);
    }

    fn from_path_elements(
        path_elements: &[schemas::PathElement],
        index: usize,
    ) -> Result<Self, DatastorersError> {
        match path_elements.get(index) {
            Some(path_element) => {
                if let Some(kind) = path_element.kind.as_ref() {
                    if kind == T::kind_str() {
                        if let Some(id) = path_element.id {
                            let ancestor = Ancestor::from_path_elements(path_elements, index + 1)?;
                            Ok(IdentifierId::id(Some(id), ancestor))
                        } else if index == 0 {
                            let ancestor = Ancestor::from_path_elements(path_elements, index + 1)?;
                            Ok(IdentifierId::id(None, ancestor))
                        } else {
                            Err(DatastoreKeyError::ExpectedId.into())
                        }
                    } else {
                        Err(DatastoreKeyError::WrongKind {
                            expected: T::kind_str(),
                            found: kind.to_string(),
                        }
                        .into())
                    }
                } else {
                    Err(DatastoreKeyError::NoKind.into())
                }
            }
            None => {
                if index == 0 {
                    let ancestor = Ancestor::from_path_elements(path_elements, index + 1)?;
                    Ok(IdentifierId::id(None, ancestor))
                } else {
                    Err(DatastoreKeyError::NoKeyPathElement.into())
                }
            }
        }
    }
}

/// This type is a name identifier in a key path
///
/// Example:
/// ```
/// use datastorers::*;
/// #[derive(DatastoreManaged)]
/// #[kind="my_entity"]
/// struct MyEntity {
///     /// A key path with no ancestors and an identifier name of type String
///     #[key]
///     key: IdentifierName<Self>,
/// }
/// ```
#[derive(Clone, Debug)]
pub struct IdentifierName<T, Ancestor = IdentifierNone>
where
    T: Kind,
    Ancestor: KeyPathElement + PartialEq,
{
    pub name: Option<String>,
    pub ancestor: Box<Ancestor>,
    phantom_kind: PhantomData<T>,
}

impl<T, Ancestor> PartialEq for IdentifierName<T, Ancestor>
where
    T: Kind,
    Ancestor: KeyPathElement + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.ancestor == other.ancestor
    }
}

impl<T, Ancestor> TryFrom<schemas::Key> for IdentifierName<T, Ancestor>
where
    T: Kind,
    Ancestor: KeyPathElement + PartialEq,
{
    type Error = DatastorersError;

    fn try_from(value: Key) -> Result<Self, Self::Error> {
        if let Some(path) = value.path {
            Self::from_path_elements(&path, 0)
        } else {
            Err(DatastoreKeyError::NoKeyPath.into())
        }
    }
}

impl<T, Ancestor> IdentifierName<T, Ancestor>
where
    T: Kind,
    Ancestor: KeyPathElement + PartialEq,
{
    pub fn name(name: Option<String>, ancestor: Ancestor) -> Self {
        Self {
            name,
            ancestor: Box::new(ancestor),
            phantom_kind: Default::default(),
        }
    }
}

impl<T, Ancestor> KeyPathElement for IdentifierName<T, Ancestor>
where
    T: Kind,
    Ancestor: KeyPathElement + PartialEq,
{
    fn identifier(&self) -> Identifier {
        match &self.name {
            Some(name) => Identifier::Name(name.clone()),
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
                    name: Some(name.clone()),
                }),
                None => path.push(schemas::PathElement {
                    id: None,
                    kind: Some(self.kind().to_string()),
                    name: None,
                }),
            },
            None => (),
        }
        self.ancestor.as_ref().fill_key(key);
    }

    fn from_path_elements(
        path_elements: &[schemas::PathElement],
        index: usize,
    ) -> Result<Self, DatastorersError> {
        match path_elements.get(index) {
            Some(path_element) => {
                if let Some(kind) = path_element.kind.as_ref() {
                    if kind == T::kind_str() {
                        if let Some(name) = path_element.name.as_ref() {
                            let ancestor = Ancestor::from_path_elements(path_elements, index + 1)?;
                            Ok(IdentifierName::name(Some(name.clone()), ancestor))
                        } else if index == 0 {
                            let ancestor = Ancestor::from_path_elements(path_elements, index + 1)?;
                            Ok(IdentifierName::name(None, ancestor))
                        } else {
                            Err(DatastoreKeyError::ExpectedName.into())
                        }
                    } else {
                        Err(DatastoreKeyError::WrongKind {
                            expected: T::kind_str(),
                            found: kind.to_string(),
                        }
                        .into())
                    }
                } else {
                    Err(DatastoreKeyError::NoKind.into())
                }
            }
            None => {
                if index == 0 {
                    let ancestor = Ancestor::from_path_elements(path_elements, index + 1)?;
                    Ok(IdentifierName::name(None, ancestor))
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
/// use datastorers::*;
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
    ) -> Result<Self, DatastorersError> {
        Ok(Self {})
    }
}
