const NUM_COEFFS: usize = 24;
const NUM_SYNDROMES: usize = 22;
const FIELD_SIZE: usize = 63;

pub fn encode(word: u16) -> u64 {
    GEN.iter().fold(word as u64, |accum, &row| {
        let bit = ((word & row).count_ones() & 1) as u8;
        accum << 1 | bit as u64
    })
}

pub fn decode(bits: u64) -> Option<(u16, usize)> {
    let word = bits >> 1;
    Errors::new(syndromes(word)).map(|(nerr, errs)| {
        let fixed = errs.fold(word, |w, (loc, pat)| {
            assert!(pat.power().unwrap() == 0);
            w ^ 1 << loc
        });
        ((fixed >> 47) as u16, nerr)
    })
}

const GEN: &[u16] = &[
    0b1110110001000111,
    0b1001101001100100,
    0b0100110100110010,
    0b0010011010011001,
    0b1111111100001011,
    0b1001001111000010,
    0b0100100111100001,
    0b1100100010110111,
    0b1000100000011100,
    0b0100010000001110,
    0b0010001000000111,
    0b1111110101000100,
    0b0111111010100010,
    0b0011111101010001,
    0b1111001111101111,
    0b1001010110110000,
    0b0100101011011000,
    0b0010010101101100,
    0b0001001010110110,
    0b0000100101011011,
    0b1110100011101010,
    0b0111010001110101,
    0b1101011001111101,
    0b1000011101111001,
    0b1010111111111011,
    0b1011101110111010,
    0b0101110111011101,
    0b1100001010101001,
    0b1000110100010011,
    0b1010101011001110,
    0b0101010101100111,
    0b1100011011110100,
    0b0110001101111010,
    0b0011000110111101,
    0b1111010010011001,
    0b1001011000001011,
    0b1010011101000010,
    0b0101001110100001,
    0b1100010110010111,
    0b1000111010001100,
    0b0100011101000110,
    0b0010001110100011,
    0b1111110110010110,
    0b0111111011001011,
    0b1101001100100010,
    0b0110100110010001,
    0b1101100010001111,
    0b0000000000000011,
];

const CODEWORDS: [u8; FIELD_SIZE] = [
    0b000001, 0b000010, 0b000100, 0b001000, 0b010000, 0b100000, 0b000011, 0b000110, 0b001100,
    0b011000, 0b110000, 0b100011, 0b000101, 0b001010, 0b010100, 0b101000, 0b010011, 0b100110,
    0b001111, 0b011110, 0b111100, 0b111011, 0b110101, 0b101001, 0b010001, 0b100010, 0b000111,
    0b001110, 0b011100, 0b111000, 0b110011, 0b100101, 0b001001, 0b010010, 0b100100, 0b001011,
    0b010110, 0b101100, 0b011011, 0b110110, 0b101111, 0b011101, 0b111010, 0b110111, 0b101101,
    0b011001, 0b110010, 0b100111, 0b001101, 0b011010, 0b110100, 0b101011, 0b010101, 0b101010,
    0b010111, 0b101110, 0b011111, 0b111110, 0b111111, 0b111101, 0b111001, 0b110001, 0b100001,
];

const POWERS: [usize; FIELD_SIZE] = [
    0, 1, 6, 2, 12, 7, 26, 3, 32, 13, 35, 8, 48, 27, 18, 4, 24, 33, 16, 14, 52, 36, 54, 9, 45, 49,
    38, 28, 41, 19, 56, 5, 62, 25, 11, 34, 31, 17, 47, 15, 23, 53, 51, 37, 44, 55, 40, 10, 61, 46,
    30, 50, 22, 39, 43, 29, 60, 42, 21, 20, 59, 57, 58,
];

#[derive(Copy, Clone)]
struct Codeword {
    bits: u8,
}

impl Codeword {
    fn new(bits: u8) -> Codeword {
        Codeword { bits }
    }

    fn for_power(power: usize) -> Codeword {
        Codeword::new(CODEWORDS[power % FIELD_SIZE])
    }

    fn zero(&self) -> bool {
        self.bits == 0
    }

    fn power(self) -> Option<usize> {
        if self.zero() {
            None
        } else {
            Some(POWERS[self.bits as usize - 1])
        }
    }

    fn invert(self) -> Codeword {
        match self.power() {
            Some(p) => Codeword::for_power(FIELD_SIZE - p),
            None => panic!("invert zero"),
        }
    }
}

#[derive(Default, Copy, Clone)]
struct BchCoefs([Codeword; NUM_COEFFS]);

#[allow(clippy::suspicious_arithmetic_impl)]
impl core::ops::Add for Codeword {
    type Output = Codeword;

    fn add(self, rhs: Codeword) -> Self::Output {
        Codeword::new(self.bits ^ rhs.bits)
    }
}

#[allow(clippy::suspicious_arithmetic_impl)]
impl core::ops::Mul for Codeword {
    type Output = Codeword;

    fn mul(self, rhs: Codeword) -> Self::Output {
        match (self.power(), rhs.power()) {
            (Some(p), Some(q)) => Codeword::for_power(p + q),
            _ => Codeword::default(),
        }
    }
}

impl core::ops::Div for Codeword {
    type Output = Codeword;

    fn div(self, rhs: Codeword) -> Self::Output {
        match (self.power(), rhs.power()) {
            (Some(p), Some(q)) => Codeword::for_power(FIELD_SIZE + p - q),
            (None, Some(_)) => Codeword::default(),
            (_, None) => panic!("divide by zero"),
        }
    }
}

impl Default for Codeword {
    fn default() -> Self {
        Codeword::new(0)
    }
}

impl core::ops::Deref for BchCoefs {
    type Target = [Codeword];
    fn deref(&self) -> &Self::Target {
        &self.0[..]
    }
}

impl core::ops::DerefMut for BchCoefs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0[..]
    }
}

fn syndromes(word: u64) -> Polynomial {
    Polynomial::new((1..=NUM_SYNDROMES).map(|p| {
        (0..63).fold(Codeword::default(), |s, b| {
            if word >> b & 1 == 0 {
                s
            } else {
                s + Codeword::for_power(b * p)
            }
        })
    }))
}

struct Errors {
    roots: Polynomial,
    descs: ErrorDescriptions,
    pos: core::ops::Range<usize>,
}

impl Errors {
    fn new(syn: Polynomial) -> Option<(usize, Self)> {
        let loc = ErrorLocator::new(syn).build();
        let errors = loc.degree().expect("invalid error polynomial");
        let mut roots = Polynomial::default();
        let nroots = PolynomialRoots::new(loc).collect_slice(&mut roots[..]);
        if nroots != errors {
            return None;
        }
        Some((
            errors,
            Errors {
                roots,
                descs: ErrorDescriptions::new(syn, loc),
                pos: 0..errors,
            },
        ))
    }
}

impl Iterator for Errors {
    type Item = (usize, Codeword);

    fn next(&mut self) -> Option<Self::Item> {
        self.pos.next().map(|i| self.descs.for_root(self.roots[i]))
    }
}

struct ErrorDescriptions {
    deriv: Polynomial,
    vals: Polynomial,
}

impl ErrorDescriptions {
    fn new(syn: Polynomial, loc: Polynomial) -> Self {
        ErrorDescriptions {
            deriv: loc.deriv(),
            vals: (loc * syn).truncate(NUM_SYNDROMES - 1),
        }
    }

    fn for_root(&self, root: Codeword) -> (usize, Codeword) {
        (
            root.invert().power().unwrap(),
            self.vals.eval(root) / self.deriv.eval(root),
        )
    }
}

#[derive(Copy, Clone)]
struct Polynomial {
    coefs: BchCoefs,
    start: usize,
}

impl Polynomial {
    fn new<T: Iterator<Item = Codeword>>(mut init: T) -> Self {
        let mut coefs = BchCoefs::default();
        init.collect_slice(&mut coefs[..]);
        Polynomial { coefs, start: 0 }
    }

    fn get(&self, idx: usize) -> Codeword {
        match self.coefs.get(idx) {
            Some(&c) => c,
            None => Codeword::default(),
        }
    }

    fn shift(mut self) -> Polynomial {
        assert!(self.constant().zero());
        self.coefs[self.start] = Codeword::default();
        self.start += 1;
        self
    }

    fn constant(&self) -> Codeword {
        self.coefs[self.start]
    }

    fn unit_power(n: usize) -> Self {
        let mut coefs = BchCoefs::default();
        coefs[n] = Codeword::for_power(0);
        Polynomial { coefs, start: 0 }
    }

    fn degree(&self) -> Option<usize> {
        for (deg, coef) in self.coefs.iter().enumerate().rev() {
            if !coef.zero() {
                return Some(deg - self.start);
            }
        }
        None
    }

    fn coef(&self, i: usize) -> Codeword {
        self.get(self.start + i)
    }

    fn truncate(mut self, deg: usize) -> Polynomial {
        for i in (self.start + deg + 1)..self.coefs.len() {
            self.coefs[i] = Codeword::default();
        }
        self
    }

    fn deriv(mut self) -> Polynomial {
        for i in self.start..self.coefs.len() {
            self.coefs[i] = if (i - self.start).is_multiple_of(2) {
                self.get(i + 1)
            } else {
                Codeword::default()
            };
        }
        self
    }

    fn eval(&self, x: Codeword) -> Codeword {
        self.iter()
            .rev()
            .fold(Codeword::default(), |s, &coef| s * x + coef)
    }
}

impl Default for Polynomial {
    fn default() -> Self {
        Polynomial::new(core::iter::empty())
    }
}

impl core::ops::Deref for Polynomial {
    type Target = [Codeword];
    fn deref(&self) -> &Self::Target {
        &self.coefs[self.start..]
    }
}

impl core::ops::DerefMut for Polynomial {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.coefs[self.start..]
    }
}

struct PolynomialRoots {
    loc: Polynomial,
    pow: core::ops::Range<usize>,
}

impl PolynomialRoots {
    fn new(loc: Polynomial) -> Self {
        PolynomialRoots {
            loc,
            pow: 0..FIELD_SIZE,
        }
    }
}

impl Iterator for PolynomialRoots {
    type Item = Codeword;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let pow = self.pow.next()?;
            let eval = self.loc.iter().fold(Codeword::default(), |sum, &x| sum + x);
            for (pow, term) in self.loc.iter_mut().enumerate() {
                *term = *term * Codeword::for_power(pow);
            }
            if eval.zero() {
                return Some(Codeword::for_power(pow));
            }
        }
    }
}

struct ErrorLocator {
    p_saved: Polynomial,
    p_cur: Polynomial,
    q_saved: Polynomial,
    q_cur: Polynomial,
    deg_saved: usize,
    deg_cur: usize,
}

impl ErrorLocator {
    fn new(syn: Polynomial) -> ErrorLocator {
        ErrorLocator {
            q_saved: Polynomial::new(
                core::iter::once(Codeword::for_power(0))
                    .chain(syn.iter().take(NUM_SYNDROMES).cloned()),
            ),
            q_cur: syn,
            p_saved: Polynomial::unit_power(NUM_SYNDROMES + 1),
            p_cur: Polynomial::unit_power(NUM_SYNDROMES),
            deg_saved: 0,
            deg_cur: 1,
        }
    }

    fn build(mut self) -> Polynomial {
        for _ in 0..NUM_SYNDROMES {
            self.step();
        }
        self.p_cur
    }

    fn step(&mut self) {
        let (save, q, p, d) = if self.q_cur.constant().zero() {
            self.reduce()
        } else {
            self.transform()
        };
        if save {
            self.q_saved = self.q_cur;
            self.p_saved = self.p_cur;
            self.deg_saved = self.deg_cur;
        }
        self.q_cur = q;
        self.p_cur = p;
        self.deg_cur = d;
    }

    fn reduce(&mut self) -> (bool, Polynomial, Polynomial, usize) {
        (
            false,
            self.q_cur.shift(),
            self.p_cur.shift(),
            2 + self.deg_cur,
        )
    }

    fn transform(&mut self) -> (bool, Polynomial, Polynomial, usize) {
        let mult = self.q_cur.constant() / self.q_saved.constant();
        (
            self.deg_cur >= self.deg_saved,
            (self.q_cur + self.q_saved * mult).shift(),
            (self.p_cur + self.p_saved * mult).shift(),
            2 + core::cmp::min(self.deg_cur, self.deg_saved),
        )
    }
}

impl core::ops::Mul<Polynomial> for Polynomial {
    type Output = Polynomial;

    fn mul(self, rhs: Polynomial) -> Self::Output {
        let mut out = Polynomial::default();
        for (i, &coef) in self.iter().enumerate() {
            for (j, &mult) in rhs.iter().enumerate() {
                if let Some(c) = out.coefs.get_mut(i + j) {
                    *c = *c + coef * mult
                }
            }
        }
        out
    }
}

impl core::ops::Add for Polynomial {
    type Output = Polynomial;

    fn add(mut self, rhs: Polynomial) -> Self::Output {
        for i in 0..self.coefs.len() {
            self.coefs[i] = self.coef(i) + rhs.coef(i);
        }
        self.start = 0;
        self
    }
}

impl core::ops::Mul<Codeword> for Polynomial {
    type Output = Polynomial;

    fn mul(mut self, rhs: Codeword) -> Self::Output {
        for coef in self.coefs.iter_mut() {
            *coef = *coef * rhs;
        }
        self
    }
}

trait CollectSlice: Iterator {
    fn collect_slice(&mut self, slice: &mut [Self::Item]) -> usize;
}

impl<I: ?Sized> CollectSlice for I
where
    I: Iterator,
{
    fn collect_slice(&mut self, slice: &mut [Self::Item]) -> usize {
        slice.iter_mut().zip(self).fold(0, |count, (dest, item)| {
            *dest = item;
            count + 1
        })
    }
}

#[cfg(test)]
mod test {
    use super::syndromes;
    use super::*;

    #[test]
    fn test_encode() {
        assert_eq!(
            encode(0b1111111100000000),
            0b1111111100000000100100110001000011000010001100000110100001101000
        );
        assert_eq!(encode(0b0011) & 1, 0);
        assert_eq!(encode(0b0101) & 1, 1);
        assert_eq!(encode(0b1010) & 1, 1);
        assert_eq!(encode(0b1100) & 1, 0);
        assert_eq!(encode(0b1111) & 1, 0);
    }

    #[test]
    fn test_syndromes() {
        let w = encode(0b1111111100000000) >> 1;
        assert_eq!(syndromes(w).degree(), None);
        assert_eq!(syndromes(w ^ 1 << 60).degree().unwrap(), 21);
    }

    #[test]
    fn test_decode() {
        assert!(decode(encode(0b0000111100001111) ^ 1 << 63).unwrap() == (0b0000111100001111, 1));
        assert!(decode(encode(0b1100011111111111) ^ 1).unwrap() == (0b1100011111111111, 0));
        assert!(
            decode(encode(0b1111111100000000) ^ 0b11010011 << 30).unwrap()
                == (0b1111111100000000, 5)
        );
        assert!(
            decode(encode(0b1101101101010001) ^ (1 << 63 | 1)).unwrap() == (0b1101101101010001, 1)
        );
        assert!(
            decode(encode(0b1111111111111111) ^ 0b11111111111).unwrap() == (0b1111111111111111, 10)
        );
        assert!(
            decode(encode(0b0000000000000000) ^ 0b11111111111).unwrap() == (0b0000000000000000, 10)
        );
        assert!(
            decode(encode(0b0000111110000000) ^ 0b111111111110).unwrap()
                == (0b0000111110000000, 11)
        );
        assert!(
            decode(encode(0b0000111110000000) ^ 0b111111111110).unwrap()
                == (0b0000111110000000, 11)
        );
        assert!(decode(encode(0b0000111110001010) ^ 0b1111111111110).is_none());
        assert!(decode(encode(0b0000001111111111) ^ 0b11111111111111111111110).is_none());
        assert!(
            decode(encode(0b0000001111111111) ^ 0b00100101010101000010001100100010011111111110)
                .is_none()
        );
        for i in 0..1u32 << 17 {
            assert_eq!(decode(encode(i as u16)).unwrap().0, i as u16);
        }
    }
}
