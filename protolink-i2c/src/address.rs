use core::fmt;

/// A valid 7-bit I2C target address.
///
/// Addresses `0x00..=0x07` and `0x78..=0x7F` are reserved by the I2C
/// specification (general call, CBUS, 10-bit prefixes, ...) and are rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Address(u8);

/// The address is outside the usable 7-bit range `0x08..=0x77`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidAddress(pub u8);

impl fmt::Display for InvalidAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#04x} is not a usable 7-bit I2C address", self.0)
    }
}

impl core::error::Error for InvalidAddress {}

impl Address {
    /// The lowest usable address.
    pub const MIN: u8 = 0x08;
    /// The highest usable address.
    pub const MAX: u8 = 0x77;

    /// Validates `address`.
    pub const fn new(address: u8) -> Result<Self, InvalidAddress> {
        if address >= Self::MIN && address <= Self::MAX {
            Ok(Self(address))
        } else {
            Err(InvalidAddress(address))
        }
    }

    /// The raw 7-bit address.
    pub const fn get(self) -> u8 {
        self.0
    }
}

impl TryFrom<u8> for Address {
    type Error = InvalidAddress;

    fn try_from(address: u8) -> Result<Self, InvalidAddress> {
        Self::new(address)
    }
}

impl From<Address> for u8 {
    fn from(address: Address) -> u8 {
        address.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_ranges_are_rejected() {
        assert_eq!(Address::new(0x00), Err(InvalidAddress(0x00)));
        assert_eq!(Address::new(0x07), Err(InvalidAddress(0x07)));
        assert_eq!(Address::new(0x78), Err(InvalidAddress(0x78)));
        assert_eq!(Address::new(0xFF), Err(InvalidAddress(0xFF)));
    }

    #[test]
    fn usable_range_is_accepted() {
        assert_eq!(Address::new(0x08).unwrap().get(), 0x08);
        assert_eq!(Address::new(0x42).unwrap().get(), 0x42);
        assert_eq!(u8::from(Address::new(0x77).unwrap()), 0x77);
    }
}
