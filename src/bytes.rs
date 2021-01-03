use radix64::STD as BASE64_CFG;
use std::fmt;

#[derive(Debug)]
pub struct Bytes(pub Vec<u8>);

impl ::std::convert::From<Vec<u8>> for Bytes {
    fn from(x: Vec<u8>) -> Bytes {
        Bytes(x)
    }
}

impl ::std::convert::From<Bytes> for Vec<u8> {
    fn from(x: Bytes) -> Vec<u8> {
        x.0
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        ::radix64::Display::new(BASE64_CFG, &self.0).fmt(f)
    }
}
