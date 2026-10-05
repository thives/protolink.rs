/// A CRC-16 algorithm used to protect frames.
pub trait Crc16 {
    /// Computes the CRC-16 over the concatenation of `chunks`.
    fn checksum_concat<const N: usize>(&self, chunks: [&[u8]; N]) -> u16;
}

/// Any closure that computes a CRC-16 over the concatenation of slices can be
/// used as a [`Crc16`].
impl<F: Fn(&[&[u8]]) -> u16> Crc16 for F {
    fn checksum_concat<const N: usize>(&self, chunks: [&[u8]; N]) -> u16 {
        self(&chunks)
    }
}

impl Crc16 for crc::Crc<u16> {
    fn checksum_concat<const N: usize>(&self, chunks: [&[u8]; N]) -> u16 {
        let mut d = self.digest();
        for chunk in chunks {
            d.update(chunk);
        }
        d.finalize()
    }
}

impl Crc16 for crc::Crc<u16, crc::NoTable> {
    fn checksum_concat<const N: usize>(&self, chunks: [&[u8]; N]) -> u16 {
        let mut d = self.digest();
        for chunk in chunks {
            d.update(chunk);
        }
        d.finalize()
    }
}

impl Crc16 for crc::Crc<u16, crc::Table<16>> {
    fn checksum_concat<const N: usize>(&self, chunks: [&[u8]; N]) -> u16 {
        let mut d = self.digest();
        for chunk in chunks {
            d.update(chunk);
        }
        d.finalize()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn crc16_x25_check_value() {
        let crc = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);
        assert_eq!(crc.checksum(b"123456789"), 0x906E);
    }
}
