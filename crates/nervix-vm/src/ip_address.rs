//! IP addresses as fixed-width `BYTES` values, and the CIDR networks they are tested against.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The `BYTES` representation of an IP address, reading and writing its text,
//!   masking it to a prefix, IPv4-mapped IPv6 conversion, and CIDR networks parsed once and tested
//!   with fixed-width integer operations.
//! - **Depends on.** Arrow binary, string and primitive columns, the standard library's address
//!   parsers and formatters, and the VM's row errors.
//! - **Must not know.** NSPL syntax, routes, schemas or connectors. It never resolves a name or
//!   reaches the network.

use std::{
    fmt::Write as _,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    num::NonZeroUsize,
    ops::BitAnd,
    str::FromStr,
};

use arrow_array::{
    Array, BinaryArray, BooleanArray, Int64Array, StringArray,
    builder::{BinaryBuilder, BooleanBuilder, Int64Builder, StringBuilder},
};
use meticulous::ResultExt as _;
use thiserror::Error;

use crate::{
    RowErrors, SideError, SideErrorReason,
    count::{CountOperand, SignedCount},
    error::{BytesOperation, IpOperation, TextOperation},
    operand::Operand,
    program::Span,
    text_column::{BinaryColumnBuilder, TextColumnBuilder},
};

/// The unsigned integer that holds the bits of one family's addresses, most significant bit first,
/// which is the order their octets have in network byte order.
trait AddressBits: Copy + Eq + BitAnd<Output = Self> {
    /// How many bits an address has, which is also its longest prefix.
    const WIDTH: u8;

    /// The mask that keeps the first `prefix` bits, or `None` when an address is shorter than
    /// `prefix` bits.
    fn prefix_mask(prefix: u8) -> Option<Self>;
}

macro_rules! address_bits {
    ($($native:ty => $width:literal),+ $(,)?) => {
        $(
            impl AddressBits for $native {
                const WIDTH: u8 = $width;

                fn prefix_mask(prefix: u8) -> Option<Self> {
                    let host_bits = Self::WIDTH.checked_sub(prefix)?;
                    // Shifting by the whole width is out of range, and a prefix of zero keeps no
                    // bit.
                    match Self::MAX.checked_shl(u32::from(host_bits)) {
                        Some(mask) => Some(mask),
                        None => Some(0),
                    }
                }
            }
        )+
    };
}

address_bits!(u32 => 32, u128 => 128);

/// The family of an IP address, which the length of its `BYTES` value encodes: four octets for
/// IPv4 and sixteen for IPv6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display)]
pub enum IpFamily {
    #[strum(to_string = "IPv4")]
    V4,
    #[strum(to_string = "IPv6")]
    V6,
}

impl IpFamily {
    /// How many bits an address of this family has, which is also its longest prefix.
    pub const fn bits(self) -> u8 {
        match self {
            Self::V4 => u32::WIDTH,
            Self::V6 => u128::WIDTH,
        }
    }

    /// The number `ip_family` answers for an address of this family.
    const fn number(self) -> i64 {
        match self {
            Self::V4 => 4,
            Self::V6 => 6,
        }
    }
}

/// One IP address as the bits of its family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IpBits {
    V4(u32),
    V6(u128),
}

impl IpBits {
    /// The address a `BYTES` value holds, or `None` when the value is neither four nor sixteen
    /// octets long.
    fn from_octets(octets: &[u8]) -> Option<Self> {
        if let Ok(octets) = <[u8; 4]>::try_from(octets) {
            return Some(Self::V4(u32::from_be_bytes(octets)));
        }
        if let Ok(octets) = <[u8; 16]>::try_from(octets) {
            return Some(Self::V6(u128::from_be_bytes(octets)));
        }
        None
    }

    /// The address `text` writes, read exactly as the standard library reads an IPv4 or IPv6
    /// address, or `None` when `text` writes no address.
    fn parse(text: &str) -> Option<Self> {
        let Ok(address) = IpAddr::from_str(text) else {
            return None;
        };
        let bits = match address {
            IpAddr::V4(address) => Self::V4(address.to_bits()),
            IpAddr::V6(address) => Self::V6(address.to_bits()),
        };
        Some(bits)
    }

    fn family(self) -> IpFamily {
        match self {
            Self::V4(_) => IpFamily::V4,
            Self::V6(_) => IpFamily::V6,
        }
    }

    /// The octets of the address in network byte order.
    fn octets(self) -> Octets {
        match self {
            Self::V4(bits) => Octets::V4(bits.to_be_bytes()),
            Self::V6(bits) => Octets::V6(bits.to_be_bytes()),
        }
    }

    /// Writes the address as text: IPv4 in dotted decimal, and IPv6 in the canonical form of
    /// RFC 5952, which writes the IPv4 address of an IPv4-mapped address in dotted decimal.
    fn write_text(self, text: &mut String) {
        let written = match self {
            Self::V4(bits) => write!(text, "{}", Ipv4Addr::from_bits(bits)),
            Self::V6(bits) => write!(text, "{}", Ipv6Addr::from_bits(bits)),
        };
        written.assured("formatting an address into a String never fails");
    }

    /// The address with every bit past its first `prefix` bits cleared, or `None` when the
    /// address is shorter than `prefix` bits.
    fn truncated(self, prefix: u8) -> Option<Self> {
        match self {
            Self::V4(bits) => {
                let mask = u32::prefix_mask(prefix)?;
                Some(Self::V4(bits & mask))
            }
            Self::V6(bits) => {
                let mask = u128::prefix_mask(prefix)?;
                Some(Self::V6(bits & mask))
            }
        }
    }

    /// The IPv4 address an IPv4-mapped IPv6 address carries, and every other address as it is.
    fn unmapped(self) -> Self {
        let Self::V6(bits) = self else {
            return self;
        };
        match Ipv6Addr::from_bits(bits).to_ipv4_mapped() {
            Some(mapped) => Self::V4(mapped.to_bits()),
            None => self,
        }
    }
}

/// The octets of one address, which a column appends as one `BYTES` value.
enum Octets {
    V4([u8; 4]),
    V6([u8; 16]),
}

impl AsRef<[u8]> for Octets {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::V4(octets) => octets,
            Self::V6(octets) => octets,
        }
    }
}

/// The addresses of one family whose first bits equal the network's, held as the network's
/// address and the mask of its prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FamilyNetwork<B> {
    network: B,
    mask: B,
}

impl<B: AddressBits> FamilyNetwork<B> {
    /// The network of `prefix` bits starting at `network`, whose address may set no bit past the
    /// prefix.
    fn new(network: B, prefix: u8, family: IpFamily) -> Result<Self, NetworkDefect> {
        let Some(mask) = B::prefix_mask(prefix) else {
            return Err(NetworkDefect::PrefixOutOfRange { family });
        };
        if network & mask != network {
            return Err(NetworkDefect::HostBitsSet);
        }
        Ok(Self { network, mask })
    }

    fn contains(self, address: B) -> bool {
        address & self.mask == self.network
    }
}

/// A CIDR network: the addresses of one family whose first bits equal the network's.
///
/// A network is parsed from its text once, and testing an address against it is one masked
/// comparison at the family's width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpNetwork(NetworkOfFamily);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NetworkOfFamily {
    V4(FamilyNetwork<u32>),
    V6(FamilyNetwork<u128>),
}

impl IpNetwork {
    /// Whether `address` lies in the network. An address of the other family never does, so an
    /// IPv4-mapped IPv6 address lies in no IPv4 network.
    fn contains(self, address: IpBits) -> bool {
        match (self.0, address) {
            (NetworkOfFamily::V4(network), IpBits::V4(address)) => network.contains(address),
            (NetworkOfFamily::V6(network), IpBits::V6(address)) => network.contains(address),
            (NetworkOfFamily::V4(_), IpBits::V6(_)) | (NetworkOfFamily::V6(_), IpBits::V4(_)) => {
                false
            }
        }
    }
}

impl FromStr for IpNetwork {
    type Err = NetworkDefect;

    /// Reads a network written as an address, a `/`, and a prefix length in decimal without
    /// leading zeros, whose address sets no bit past the prefix.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let Some((address, prefix)) = text.rsplit_once('/') else {
            return Err(NetworkDefect::NotAddressAndPrefix);
        };
        let Some(network) = IpBits::parse(address) else {
            return Err(NetworkDefect::UnreadableAddress);
        };
        let family = network.family();
        let digits_only = prefix.bytes().all(|byte| byte.is_ascii_digit());
        let leading_zero = prefix.len() > 1 && prefix.starts_with('0');
        if prefix.is_empty() || !digits_only || leading_zero {
            return Err(NetworkDefect::UnreadablePrefix);
        }
        // Every digit was checked above, so text that does not fit a `u8` is a number too long
        // for any family.
        let Ok(prefix) = prefix.parse::<u8>() else {
            return Err(NetworkDefect::PrefixOutOfRange { family });
        };
        let network = match network {
            IpBits::V4(bits) => NetworkOfFamily::V4(FamilyNetwork::new(bits, prefix, family)?),
            IpBits::V6(bits) => NetworkOfFamily::V6(FamilyNetwork::new(bits, prefix, family)?),
        };
        Ok(Self(network))
    }
}

/// Why text is not a network in CIDR notation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum NetworkDefect {
    #[error("is not written as address/prefix")]
    NotAddressAndPrefix,
    #[error("address is not an IPv4 or IPv6 address")]
    UnreadableAddress,
    #[error("prefix length is not a decimal number without leading zeros")]
    UnreadablePrefix,
    #[error(
        "prefix length must be 0 to {longest} for an {family} network",
        longest = family.bits()
    )]
    PrefixOutOfRange { family: IpFamily },
    #[error("has host bits set past its prefix length")]
    HostBitsSet,
}

/// Where `ip_in_network` reads its network from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkSource {
    /// A network known when the program was compiled, parsed once and shared by every batch.
    Constant(IpNetwork),
    /// A network read from the call's second argument, parsed for each row.
    Argument,
}

/// Records that `row` of an IP builtin read a `BYTES` value that holds no address.
fn not_an_address(errors: &mut RowErrors, row: usize, operation: IpOperation, span: Span) {
    errors.push(
        row,
        SideError {
            reason: SideErrorReason::NotAnIpAddress(operation),
            span,
        },
    );
}

/// `ip_from_string`: the address each text writes, as four or sixteen octets.
pub(crate) fn from_string(
    input: &StringArray,
    rows_per_value: NonZeroUsize,
    errors: &mut RowErrors,
    span: Span,
) -> BinaryArray {
    let mut output = BinaryColumnBuilder::new(BinaryBuilder::new(), rows_per_value);
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        let Some(address) = IpBits::parse(input.value(row)) else {
            output.append_null();
            errors.push(
                row,
                SideError {
                    reason: SideErrorReason::UnreadableIpAddress,
                    span,
                },
            );
            continue;
        };
        if !output.append_value(address.octets()) {
            output.append_null();
            errors.push(
                row,
                SideError {
                    reason: SideErrorReason::BytesTooLong(BytesOperation::IpFromString),
                    span,
                },
            );
        }
    }
    output.finish()
}

/// Whether `text` writes an address `ip_from_string` reads. Folding `is_ip_address` over a literal
/// and executing it over a column both answer through this test.
pub(crate) fn is_address_text(text: &str) -> bool {
    IpBits::parse(text).is_some()
}

/// `is_ip_address`: whether each text writes an address `ip_from_string` reads.
pub(crate) fn is_address(input: &StringArray) -> BooleanArray {
    let mut output = BooleanBuilder::with_capacity(input.len());
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
        } else {
            output.append_value(is_address_text(input.value(row)));
        }
    }
    output.finish()
}

/// `ip_to_string`: each address written as text.
pub(crate) fn to_string(
    input: &BinaryArray,
    rows_per_value: NonZeroUsize,
    errors: &mut RowErrors,
    span: Span,
) -> StringArray {
    let mut output = TextColumnBuilder::new(StringBuilder::new(), rows_per_value);
    let mut text = String::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        let Some(address) = IpBits::from_octets(input.value(row)) else {
            output.append_null();
            not_an_address(errors, row, IpOperation::IpToString, span);
            continue;
        };
        text.clear();
        address.write_text(&mut text);
        if !output.append_value(&text) {
            output.append_null();
            errors.push(
                row,
                SideError {
                    reason: SideErrorReason::TextTooLong(TextOperation::IpToString),
                    span,
                },
            );
        }
    }
    output.finish()
}

/// `ip_family`: `4` for each IPv4 address and `6` for each IPv6 address.
pub(crate) fn family(input: &BinaryArray, errors: &mut RowErrors, span: Span) -> Int64Array {
    let mut output = Int64Builder::with_capacity(input.len());
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        match IpBits::from_octets(input.value(row)) {
            Some(address) => output.append_value(address.family().number()),
            None => {
                output.append_null();
                not_an_address(errors, row, IpOperation::IpFamily, span);
            }
        }
    }
    output.finish()
}

/// `ip_trunc`: each address with every bit past its row's prefix length cleared.
pub(crate) fn truncate(
    input: &BinaryArray,
    prefixes: CountOperand<'_>,
    rows_per_value: NonZeroUsize,
    errors: &mut RowErrors,
    span: Span,
) -> BinaryArray {
    let mut output = BinaryColumnBuilder::new(BinaryBuilder::new(), rows_per_value);
    for row in 0..input.len() {
        let Some(count) = prefixes.value(row) else {
            output.append_null();
            continue;
        };
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        let Some(address) = IpBits::from_octets(input.value(row)) else {
            output.append_null();
            not_an_address(errors, row, IpOperation::IpTrunc, span);
            continue;
        };
        let prefix = match count {
            SignedCount::NonNegative(prefix) => u8::try_from(prefix).ok(),
            SignedCount::Negative(_) => None,
        };
        let truncated = match prefix {
            Some(prefix) => address.truncated(prefix),
            None => None,
        };
        let Some(truncated) = truncated else {
            output.append_null();
            let family = address.family();
            errors.push(
                row,
                SideError {
                    reason: SideErrorReason::IpPrefixOutOfRange { family },
                    span,
                },
            );
            continue;
        };
        if !output.append_value(truncated.octets()) {
            output.append_null();
            errors.push(
                row,
                SideError {
                    reason: SideErrorReason::BytesTooLong(BytesOperation::IpTrunc),
                    span,
                },
            );
        }
    }
    output.finish()
}

/// `ip_unmap`: the IPv4 address of each IPv4-mapped IPv6 address, and every other address as it
/// is.
pub(crate) fn unmap(input: &BinaryArray, errors: &mut RowErrors, span: Span) -> BinaryArray {
    // An unmapped address is never longer than the address it came from, so the column holds no
    // more octets than its input does.
    let mut output = BinaryBuilder::with_capacity(input.len(), input.value_data().len());
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        match IpBits::from_octets(input.value(row)) {
            Some(address) => output.append_value(address.unmapped().octets()),
            None => {
                output.append_null();
                not_an_address(errors, row, IpOperation::IpUnmap, span);
            }
        }
    }
    output.finish()
}

/// `ip_in_network` against a network compiled with its program.
pub(crate) fn in_constant_network(
    input: &BinaryArray,
    network: IpNetwork,
    errors: &mut RowErrors,
    span: Span,
) -> BooleanArray {
    let mut output = BooleanBuilder::with_capacity(input.len());
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        match IpBits::from_octets(input.value(row)) {
            Some(address) => output.append_value(network.contains(address)),
            None => {
                output.append_null();
                not_an_address(errors, row, IpOperation::IpInNetwork, span);
            }
        }
    }
    output.finish()
}

/// `ip_in_network` against the network each row names.
///
/// Rows usually repeat the network of the row before them, so a network whose text repeats the
/// previous row's is not parsed again.
pub(crate) fn in_argument_networks(
    input: &BinaryArray,
    networks: Operand<'_, StringArray>,
    errors: &mut RowErrors,
    span: Span,
) -> BooleanArray {
    /// The network text the previous row named, and what parsing it produced.
    struct Parsed<'t> {
        text: &'t str,
        network: Result<IpNetwork, NetworkDefect>,
    }

    let mut output = BooleanBuilder::with_capacity(input.len());
    let mut previous: Option<Parsed<'_>> = None;
    for row in 0..input.len() {
        let network_index = networks.index(row);
        if input.is_null(row) || networks.array().is_null(network_index) {
            output.append_null();
            continue;
        }
        let Some(address) = IpBits::from_octets(input.value(row)) else {
            output.append_null();
            not_an_address(errors, row, IpOperation::IpInNetwork, span);
            continue;
        };
        let text = networks.array().value(network_index);
        let network = match &previous {
            Some(parsed) if parsed.text == text => parsed.network,
            Some(_) | None => {
                let network = text.parse::<IpNetwork>();
                previous = Some(Parsed { text, network });
                network
            }
        };
        match network {
            Ok(network) => output.append_value(network.contains(address)),
            Err(defect) => {
                output.append_null();
                errors.push(
                    row,
                    SideError {
                        reason: SideErrorReason::InvalidIpNetwork(defect),
                        span,
                    },
                );
            }
        }
    }
    output.finish()
}

#[cfg(test)]
#[path = "ip_address_tests.rs"]
mod tests;
