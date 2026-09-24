use std::num::NonZeroUsize;

use arrow_array::{Array, BinaryArray, Int8Array, Int64Array, StringArray, UInt64Array};
use rstest::rstest;

use super::{
    IpBits, IpFamily, IpNetwork, NetworkDefect, family, from_string, in_argument_networks,
    in_constant_network, is_address, to_string, truncate, unmap,
};
use crate::{
    RowErrors, SideErrorReason,
    count::CountOperand,
    error::{BytesOperation, IpOperation, TextOperation},
    operand::Operand,
    program::Span,
};

const SPAN: Span = Span { start: 0, end: 1 };

/// The octets `ip_from_string` reads from one text, or the reason it reads none.
fn octets(text: &str) -> Result<Vec<u8>, SideErrorReason> {
    let input = StringArray::from(vec![text]);
    let mut errors = RowErrors::new(1);
    let output = from_string(&input, NonZeroUsize::MIN, &mut errors, SPAN);
    match errors.row(0).first() {
        Some(error) => Err(error.reason.clone()),
        None => Ok(output.value(0).to_vec()),
    }
}

/// The text `ip_to_string` writes for the address one text reads as.
fn round_trip(text: &str) -> String {
    let address = BinaryArray::from(vec![
        octets(text).expect("the address must read").as_slice(),
    ]);
    let mut errors = RowErrors::new(1);
    let written = to_string(&address, NonZeroUsize::MIN, &mut errors, SPAN);
    assert!(errors.is_error_free());
    written.value(0).to_string()
}

#[rstest]
#[case("0.0.0.0", &[0, 0, 0, 0])]
#[case("255.255.255.255", &[255, 255, 255, 255])]
#[case("192.0.2.1", &[192, 0, 2, 1])]
#[case("::", &[0; 16])]
#[case(
    "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
    &[255; 16]
)]
#[case("::1", &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])]
#[case(
    "::ffff:192.0.2.1",
    &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 192, 0, 2, 1]
)]
#[case(
    "2001:DB8::0:1",
    &[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
)]
fn text_reads_as_network_byte_order_octets_of_its_family(
    #[case] text: &str,
    #[case] expected: &[u8],
) {
    assert_eq!(octets(text), Ok(expected.to_vec()));
}

#[rstest]
#[case("010.0.0.1")]
#[case("256.0.0.1")]
#[case("1.2.3")]
#[case("1.2.3.4.5")]
#[case(" 1.2.3.4")]
#[case("1.2.3.4 ")]
#[case("1.2.3.4/32")]
#[case("[::1]")]
#[case("fe80::1%eth0")]
#[case("1:2:3:4:5:6:7:8:9")]
#[case("::ffff:1.2.3.256")]
#[case("12345::")]
#[case("")]
#[case("localhost")]
fn text_that_is_not_exactly_an_address_is_not_read(#[case] text: &str) {
    assert_eq!(octets(text), Err(SideErrorReason::UnreadableIpAddress));
}

#[rstest]
#[case("10.0.0.1", "10.0.0.1")]
#[case("255.255.255.255", "255.255.255.255")]
#[case("0:0:0:0:0:0:0:0", "::")]
#[case("2001:DB8:0:0:0:0:0:1", "2001:db8::1")]
#[case("2001:db8:0:0:1:0:0:1", "2001:db8::1:0:0:1")]
#[case("2001:db8:0:1:1:1:1:1", "2001:db8:0:1:1:1:1:1")]
#[case("0:0:1:0:0:0:0:0", "0:0:1::")]
#[case("::FFFF:10.9.8.7", "::ffff:10.9.8.7")]
#[case("::a09:807", "::a09:807")]
#[case(
    "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
    "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"
)]
fn addresses_write_canonical_text(#[case] text: &str, #[case] expected: &str) {
    assert_eq!(round_trip(text), expected);
}

#[test]
fn only_four_and_sixteen_octets_hold_an_address() {
    let input = BinaryArray::from(vec![
        Some(&[1, 2, 3, 4][..]),
        Some(&[0; 16][..]),
        Some(&[1, 2, 3][..]),
        Some(&[][..]),
        Some(&[0; 5][..]),
        None,
    ]);
    let mut errors = RowErrors::new(input.len());
    let families = family(&input, &mut errors, SPAN);
    assert_eq!(
        families,
        Int64Array::from(vec![Some(4), Some(6), None, None, None, None])
    );
    for row in 2..=4 {
        assert_eq!(
            errors.row(row)[0].reason,
            SideErrorReason::NotAnIpAddress(IpOperation::IpFamily)
        );
    }
    assert!(errors.row(5).is_empty());

    let mut errors = RowErrors::new(input.len());
    let written = to_string(&input, NonZeroUsize::MIN, &mut errors, SPAN);
    assert_eq!(written.value(0), "1.2.3.4");
    assert_eq!(written.value(1), "::");
    assert!(written.is_null(2));
    assert_eq!(
        errors.row(2)[0].reason,
        SideErrorReason::NotAnIpAddress(IpOperation::IpToString)
    );

    let mut errors = RowErrors::new(input.len());
    let unmapped = unmap(&input, &mut errors, SPAN);
    assert_eq!(unmapped.value(0), [1, 2, 3, 4]);
    assert!(unmapped.is_null(3));
    assert_eq!(
        errors.row(3)[0].reason,
        SideErrorReason::NotAnIpAddress(IpOperation::IpUnmap)
    );
}

#[test]
fn unmapping_converts_only_ipv4_mapped_addresses() {
    let texts = [
        "::ffff:10.9.8.7",
        "::ffff:0.0.0.0",
        "10.9.8.7",
        "::a09:807",
        "2001:db8::1",
        "::ffff:0:10.9.8.7",
    ];
    let addresses = texts
        .iter()
        .map(|text| octets(text).expect("the address must read"))
        .collect::<Vec<_>>();
    let input = BinaryArray::from_iter_values(addresses.iter());
    let mut errors = RowErrors::new(input.len());
    let unmapped = unmap(&input, &mut errors, SPAN);
    assert!(errors.is_error_free());
    assert_eq!(unmapped.value(0), [10, 9, 8, 7]);
    assert_eq!(unmapped.value(1), [0, 0, 0, 0]);
    assert_eq!(unmapped.value(2), [10, 9, 8, 7]);
    // An IPv4-compatible address and an IPv4-translated address are not IPv4-mapped.
    assert_eq!(unmapped.value(3), addresses[3].as_slice());
    assert_eq!(unmapped.value(4), addresses[4].as_slice());
    assert_eq!(unmapped.value(5), addresses[5].as_slice());
}

#[test]
fn truncation_clears_every_bit_past_the_prefix_at_the_family_width() {
    let addresses = [
        "255.255.255.255",
        "10.1.2.3",
        "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
    ]
    .map(|text| octets(text).expect("the address must read"));
    let input = BinaryArray::from_iter_values(addresses.iter());
    let cases: [(i64, [&[u8]; 3]); 5] = [
        (0, [&[0, 0, 0, 0], &[0, 0, 0, 0], &[0; 16]]),
        (
            1,
            [
                &[128, 0, 0, 0],
                &[0, 0, 0, 0],
                &[128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ],
        ),
        (
            23,
            [
                &[255, 255, 254, 0],
                &[10, 1, 2, 0],
                &[255, 255, 254, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ],
        ),
        (
            32,
            [
                &[255, 255, 255, 255],
                &[10, 1, 2, 3],
                &[255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ],
        ),
        (
            127,
            [
                &[],
                &[],
                &[
                    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 254,
                ],
            ],
        ),
    ];
    for (prefix, expected) in cases {
        let prefixes = Int64Array::from(vec![prefix]);
        let prefixes: &dyn Array = &prefixes;
        let operand = CountOperand::of(Operand::Scalar(prefixes)).expect("an I64 prefix");
        let mut errors = RowErrors::new(input.len());
        let truncated = truncate(&input, operand, NonZeroUsize::MIN, &mut errors, SPAN);
        for (row, expected) in expected.into_iter().enumerate() {
            if expected.is_empty() {
                assert!(truncated.is_null(row), "prefix {prefix} row {row}");
                assert_eq!(
                    errors.row(row)[0].reason,
                    SideErrorReason::IpPrefixOutOfRange {
                        family: IpFamily::V4
                    }
                );
            } else {
                assert_eq!(truncated.value(row), expected, "prefix {prefix} row {row}");
                assert!(errors.row(row).is_empty(), "prefix {prefix} row {row}");
            }
        }
    }
}

#[test]
fn prefix_lengths_outside_the_family_fail_their_row() {
    let input = BinaryArray::from(vec![
        Some(&[10, 1, 2, 3][..]),
        Some(&[0; 16][..]),
        Some(&[10, 1, 2, 3][..]),
        None,
        Some(&[1, 2][..]),
    ]);
    let prefixes = Int8Array::from(vec![Some(-1), Some(-128), None, Some(8), Some(8)]);
    let prefixes: &dyn Array = &prefixes;
    let operand = CountOperand::of(Operand::Column(prefixes)).expect("an I8 prefix column");
    let mut errors = RowErrors::new(input.len());
    let truncated = truncate(&input, operand, NonZeroUsize::MIN, &mut errors, SPAN);
    assert_eq!(truncated.null_count(), 5);
    assert_eq!(
        errors.row(0)[0].reason,
        SideErrorReason::IpPrefixOutOfRange {
            family: IpFamily::V4
        }
    );
    assert_eq!(
        errors.row(1)[0].reason,
        SideErrorReason::IpPrefixOutOfRange {
            family: IpFamily::V6
        }
    );
    assert!(errors.row(2).is_empty());
    assert!(errors.row(3).is_empty());
    assert_eq!(
        errors.row(4)[0].reason,
        SideErrorReason::NotAnIpAddress(IpOperation::IpTrunc)
    );

    let huge = UInt64Array::from(vec![u64::MAX, 129]);
    let huge: &dyn Array = &huge;
    let operand = CountOperand::of(Operand::Column(huge)).expect("a U64 prefix column");
    let input = BinaryArray::from(vec![&[0_u8; 16][..], &[0; 16][..]]);
    let mut errors = RowErrors::new(input.len());
    let truncated = truncate(&input, operand, NonZeroUsize::MIN, &mut errors, SPAN);
    assert_eq!(truncated.null_count(), 2);
    assert_eq!(
        errors.row(0)[0].reason.to_string(),
        "ip_trunc prefix length must be 0 to 128 for an IPv6 address"
    );
}

#[rstest]
#[case("10.0.0.0/8", "10.255.255.255", true)]
#[case("10.0.0.0/8", "11.0.0.0", false)]
#[case("10.0.0.0/8", "9.255.255.255", false)]
#[case("0.0.0.0/0", "255.255.255.255", true)]
#[case("192.0.2.1/32", "192.0.2.1", true)]
#[case("192.0.2.1/32", "192.0.2.2", false)]
#[case("::/0", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", true)]
#[case("::/0", "0.0.0.0", false)]
#[case("0.0.0.0/0", "::", false)]
#[case("2001:db8::/32", "2001:db8:ffff::1", true)]
#[case("2001:db8::/32", "2001:db9::", false)]
#[case("::1/128", "::1", true)]
#[case("fd00::/8", "fdff::", true)]
#[case("fd00::/8", "fe00::", false)]
#[case("10.0.0.0/8", "::ffff:10.0.0.1", false)]
#[case("::ffff:10.0.0.0/104", "::ffff:10.0.0.1", true)]
fn networks_hold_the_addresses_of_their_family_under_their_prefix(
    #[case] network: &str,
    #[case] address: &str,
    #[case] expected: bool,
) {
    let network = network.parse::<IpNetwork>().expect("the network must read");
    let input = BinaryArray::from(vec![
        octets(address).expect("the address must read").as_slice(),
    ]);
    let mut errors = RowErrors::new(1);
    let contained = in_constant_network(&input, network, &mut errors, SPAN);
    assert!(errors.is_error_free());
    assert_eq!(contained.value(0), expected);
}

#[rstest]
#[case("10.0.0.0", NetworkDefect::NotAddressAndPrefix)]
#[case("10.0.0/8", NetworkDefect::UnreadableAddress)]
#[case("10.0.0.0//8", NetworkDefect::UnreadableAddress)]
#[case(" 10.0.0.0/8", NetworkDefect::UnreadableAddress)]
#[case("10.0.0.0/", NetworkDefect::UnreadablePrefix)]
#[case("10.0.0.0/08", NetworkDefect::UnreadablePrefix)]
#[case("10.0.0.0/+8", NetworkDefect::UnreadablePrefix)]
#[case("10.0.0.0/8 ", NetworkDefect::UnreadablePrefix)]
#[case("10.0.0.0/33", NetworkDefect::PrefixOutOfRange { family: IpFamily::V4 })]
#[case("10.0.0.0/256", NetworkDefect::PrefixOutOfRange { family: IpFamily::V4 })]
#[case("::/129", NetworkDefect::PrefixOutOfRange { family: IpFamily::V6 })]
#[case("::/99999999999999999999", NetworkDefect::PrefixOutOfRange { family: IpFamily::V6 })]
#[case("10.0.0.1/8", NetworkDefect::HostBitsSet)]
#[case("2001:db8::1/64", NetworkDefect::HostBitsSet)]
fn network_text_names_what_it_lacks(#[case] text: &str, #[case] defect: NetworkDefect) {
    assert_eq!(text.parse::<IpNetwork>(), Err(defect));
}

#[test]
fn network_defects_render_their_published_messages() {
    let messages = [
        (
            NetworkDefect::NotAddressAndPrefix,
            "ip_in_network network is not written as address/prefix",
        ),
        (
            NetworkDefect::UnreadableAddress,
            "ip_in_network network address is not an IPv4 or IPv6 address",
        ),
        (
            NetworkDefect::UnreadablePrefix,
            "ip_in_network network prefix length is not a decimal number without leading zeros",
        ),
        (
            NetworkDefect::PrefixOutOfRange {
                family: IpFamily::V4,
            },
            "ip_in_network network prefix length must be 0 to 32 for an IPv4 network",
        ),
        (
            NetworkDefect::HostBitsSet,
            "ip_in_network network has host bits set past its prefix length",
        ),
    ];
    for (defect, message) in messages {
        assert_eq!(
            SideErrorReason::InvalidIpNetwork(defect).to_string(),
            message
        );
    }
}

#[test]
fn row_networks_parse_once_per_run_and_fail_only_their_rows() {
    let addresses = [
        "10.1.2.3",
        "10.1.2.3",
        "192.0.2.1",
        "10.1.2.3",
        "10.1.2.3",
        "::1",
    ]
    .map(|text| octets(text).expect("the address must read"));
    let mut input = addresses
        .iter()
        .map(|octets| Some(octets.as_slice()))
        .collect::<Vec<_>>();
    input.push(None);
    input.push(Some(&[1, 2, 3]));
    let input = BinaryArray::from(input);
    let networks = StringArray::from(vec![
        Some("10.0.0.0/8"),
        Some("10.0.0.0/8"),
        Some("10.0.0.0/8"),
        Some("10.0.0.1/8"),
        None,
        Some("::/0"),
        Some("::/0"),
        Some("::/0"),
    ]);
    let mut errors = RowErrors::new(input.len());
    let contained = in_argument_networks(&input, Operand::Column(&networks), &mut errors, SPAN);
    assert_eq!(
        contained.iter().collect::<Vec<_>>(),
        [
            Some(true),
            Some(true),
            Some(false),
            None,
            None,
            Some(true),
            None,
            None
        ]
    );
    assert_eq!(
        errors.row(3)[0].reason,
        SideErrorReason::InvalidIpNetwork(NetworkDefect::HostBitsSet)
    );
    assert!(errors.row(4).is_empty());
    assert!(errors.row(6).is_empty());
    assert_eq!(
        errors.row(7)[0].reason,
        SideErrorReason::NotAnIpAddress(IpOperation::IpInNetwork)
    );

    let shared = StringArray::from(vec!["fd00::/8"]);
    let input = BinaryArray::from(vec![
        octets("fd12::1").expect("the address must read").as_slice(),
        octets("fe12::1").expect("the address must read").as_slice(),
    ]);
    let mut errors = RowErrors::new(input.len());
    let contained = in_argument_networks(&input, Operand::Scalar(&shared), &mut errors, SPAN);
    assert_eq!(
        contained.iter().collect::<Vec<_>>(),
        [Some(true), Some(false)]
    );
}

#[test]
fn a_constant_network_rejects_bytes_that_hold_no_address() {
    let network = "10.0.0.0/8"
        .parse::<IpNetwork>()
        .expect("the network must read");
    let input = BinaryArray::from(vec![Some(&[10, 0, 0][..]), None]);
    let mut errors = RowErrors::new(input.len());
    let contained = in_constant_network(&input, network, &mut errors, SPAN);
    assert!(contained.is_null(0));
    assert!(contained.is_null(1));
    assert_eq!(
        errors.row(0)[0].reason,
        SideErrorReason::NotAnIpAddress(IpOperation::IpInNetwork)
    );
    assert!(errors.row(1).is_empty());
}

#[test]
fn address_validity_is_what_ip_from_string_reads() {
    let input = StringArray::from(vec![Some("192.0.2.1"), Some("::"), Some("010.0.0.1"), None]);
    assert_eq!(
        is_address(&input).iter().collect::<Vec<_>>(),
        [Some(true), Some(true), Some(false), None]
    );
}

#[test]
fn results_that_every_row_shares_are_charged_for_every_row() {
    let input = StringArray::from(vec!["::1"]);
    let mut errors = RowErrors::new(1);
    let parsed = from_string(&input, NonZeroUsize::MAX, &mut errors, SPAN);
    assert!(parsed.is_null(0));
    assert_eq!(
        errors.row(0)[0].reason,
        SideErrorReason::BytesTooLong(BytesOperation::IpFromString)
    );

    let address = BinaryArray::from(vec![&[1_u8, 2, 3, 4][..]]);
    let mut errors = RowErrors::new(1);
    let written = to_string(&address, NonZeroUsize::MAX, &mut errors, SPAN);
    assert!(written.is_null(0));
    assert_eq!(
        errors.row(0)[0].reason,
        SideErrorReason::TextTooLong(TextOperation::IpToString)
    );

    let prefixes = Int64Array::from(vec![8]);
    let prefixes: &dyn Array = &prefixes;
    let operand = CountOperand::of(Operand::Scalar(prefixes)).expect("an I64 prefix");
    let mut errors = RowErrors::new(1);
    let truncated = truncate(&address, operand, NonZeroUsize::MAX, &mut errors, SPAN);
    assert!(truncated.is_null(0));
    assert_eq!(
        errors.row(0)[0].reason,
        SideErrorReason::BytesTooLong(BytesOperation::IpTrunc)
    );
}

#[test]
fn nulls_pass_through_every_address_builtin_without_errors() {
    let texts = StringArray::from(vec![None::<&str>]);
    let addresses = BinaryArray::from(vec![None::<&[u8]>]);
    let mut errors = RowErrors::new(1);
    assert!(from_string(&texts, NonZeroUsize::MIN, &mut errors, SPAN).is_null(0));
    assert!(to_string(&addresses, NonZeroUsize::MIN, &mut errors, SPAN).is_null(0));
    assert!(family(&addresses, &mut errors, SPAN).is_null(0));
    assert!(unmap(&addresses, &mut errors, SPAN).is_null(0));
    assert!(errors.is_error_free());
}

#[test]
fn bits_report_their_family() {
    assert_eq!(IpBits::V4(0).family(), IpFamily::V4);
    assert_eq!(IpBits::V6(0).family(), IpFamily::V6);
    assert_eq!(IpFamily::V4.bits(), 32);
    assert_eq!(IpFamily::V6.bits(), 128);
    assert_eq!(IpFamily::V6.to_string(), "IPv6");
}
