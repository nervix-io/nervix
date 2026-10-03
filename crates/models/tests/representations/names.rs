//! Every name type keeps its text through every representation, and its parser and decoders accept
//! exactly the text the name rule allows.
//!
//! The valid domain is text of 1 through 128 bytes from `[a-z0-9_~.-]`, without a dot for a
//! domain name. Parsing also accepts upper-case letters and lower-cases them, which is a
//! canonicalizing contract tested on arbitrary text; decoders accept only text already in that
//! canonical form.

use nervix_arbitrary::{Arbitrary, Domain, Entropy};
use nervix_models::{
    BranchName, BuiltinFunctionName, ChannelName, ClientName, ClusterNodeName, CodecName,
    CollectionName, ConsumerGroupName, CorrelatorName, DeduplicatorName, DomainName, DotPolicy,
    EmitterName, EndpointName, FieldName, GeneratorName, InferencerName, IngestorName,
    JunctionName, LookupName, ModelName, NameError, PlacementName, PulsarSubscriptionName,
    QueueGroupName, QueueName, ReingestorName, RelayName, ReordererName, ResourceName, SchemaName,
    SignalingProtocolName, SubjectName, SubscriptionName, TableName, TopicName, UdfName, UserName,
    VhostName, WasmProcessorName, WindowProcessorName, WireSchemaName,
};

/// The longest name the vocabulary accepts, in bytes, as `NameError::TooLong` reports it.
const MAX_NAME_BYTES: usize = 128;

/// Every character a name may hold once parsed.
const NAME_ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789-_~.";

/// Characters a name may be written with: the name alphabet and the upper-case letters parsing
/// lower-cases.
const WRITTEN_ALPHABET: [char; 10] = ['a', 'z', '0', '9', '-', '_', '~', '.', 'A', 'Z'];

/// Characters no name may hold, including ones outside ASCII.
const FOREIGN_CHARACTERS: [char; 5] = ['/', ' ', 'é', '$', '🎉'];

/// Valid, already lower-cased name text: 1 through 128 bytes, a dot only where `dots` allows one.
fn valid_text(entropy: &mut Entropy<'_>, dots: DotPolicy) -> String {
    let length = entropy.boundary_biased(1..=128);
    let mut text = String::new();
    for _ in 0..length {
        let index = entropy.up_to(39);
        let index = usize::try_from(index).expect("an alphabet index fits in usize");
        let character = char::from(NAME_ALPHABET[index]);
        if character == '.' && dots == DotPolicy::Rejected {
            text.push('_');
        } else {
            text.push(character);
        }
    }
    text
}

/// Arbitrary text, from empty to past the bound. Most characters are ones a name may be written
/// with, so the text is valid often enough to test acceptance as well as each rejection.
fn arbitrary_text(entropy: &mut Entropy<'_>) -> String {
    let length = entropy.boundary_biased(0..=130);
    let mut text = String::new();
    for _ in 0..length {
        if entropy.byte().is_multiple_of(16) {
            text.push(entropy.pick(FOREIGN_CHARACTERS));
        } else {
            text.push(entropy.pick(WRITTEN_ALPHABET));
        }
    }
    text
}

/// What parsing `text` under `dots` must produce: the lower-cased name, or the first rule the text
/// breaks, in the order the rules are checked.
fn expected_parse(text: &str, dots: DotPolicy) -> Result<String, NameError> {
    if text.is_empty() {
        return Err(NameError::Empty);
    }
    if text.len() > MAX_NAME_BYTES {
        return Err(NameError::TooLong {
            max: MAX_NAME_BYTES,
            actual: text.len(),
        });
    }
    for ch in text.chars() {
        if ch == '.' && dots == DotPolicy::Rejected {
            return Err(NameError::DotNotAllowed);
        }
        if !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '~' | '.')) {
            return Err(NameError::InvalidChar { ch });
        }
    }
    Ok(text.to_ascii_lowercase())
}

/// Checks that a valid name of type `$Type` keeps `$text` through every representation.
macro_rules! assert_valid_name_round_trips {
    ($Type:ident, $text:expr) => {{
        let text: String = $text;
        let name = $Type::parse(&text)
            .unwrap_or_else(|error| panic!("{text:?} is a valid {}: {error:?}", stringify!($Type)));
        assert_eq!(
            name.as_str(),
            text,
            "parsing changed a canonical {}",
            stringify!($Type)
        );
        assert_eq!(name.to_string(), text);
        assert_eq!(text.parse::<$Type>().as_ref(), Ok(&name));
        assert_eq!($Type::try_from(text.as_str()).as_ref(), Ok(&name));
        assert_eq!($Type::try_from(text.clone()).as_ref(), Ok(&name));

        let json = serde_json::to_string(&name).expect("a name has a JSON string form");
        assert_eq!(
            json,
            serde_json::to_string(&text).expect("a string has a JSON form")
        );
        let decoded: $Type = serde_json::from_str(&json).expect("a canonical name decodes");
        assert_eq!(decoded, name);

        let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&name).expect("a name archives");
        let plain = rkyv::to_bytes::<rkyv::rancor::Error>(&text).expect("a string archives");
        assert_eq!(
            archived.as_slice(),
            plain.as_slice(),
            "a name archives as its text"
        );
        let restored = rkyv::from_bytes::<$Type, rkyv::rancor::Error>(&archived)
            .expect("a canonical archived name reads back");
        assert_eq!(restored, name);
    }};
}

/// Checks that the entity name `$Type` widens into a [`ModelName`] and narrows back unchanged.
macro_rules! assert_entity_name_widens {
    ($Type:ident, $text:expr) => {{
        let text: String = $text;
        let name = $Type::parse(&text).expect("the text is a valid name");
        let erased = ModelName::from(&name);
        assert_eq!(erased.as_str(), text);
        assert_eq!($Type::from(&erased), name);
    }};
}

/// Checks the parser and both decoders of `$Type` against the name rule on arbitrary `$text`.
macro_rules! assert_name_text_is_validated {
    ($Type:ident, $text:expr) => {{
        let text: &str = $text;
        let expected = expected_parse(text, $Type::DOTS);
        let parsed = $Type::parse(text);
        match (&expected, &parsed) {
            (Ok(canonical), Ok(name)) => assert_eq!(name.as_str(), canonical),
            (Err(rule), Err(report)) => assert_eq!(report.current_context(), rule),
            _ => panic!(
                "{} parsed {text:?} as {parsed:?}; the rule says {expected:?}",
                stringify!($Type)
            ),
        }
        // A stored or transmitted name was written by an encoder, which only ever writes parsed
        // text, so a decoder accepts that text alone.
        let decodable = expected.as_deref() == Ok(text);
        let json = serde_json::to_string(text).expect("a string has a JSON form");
        let decoded = serde_json::from_str::<$Type>(&json);
        assert_eq!(
            decoded.is_ok(),
            decodable,
            "{} JSON decoding of {text:?}",
            stringify!($Type)
        );
        let archived =
            rkyv::to_bytes::<rkyv::rancor::Error>(&text.to_owned()).expect("a string archives");
        let restored = rkyv::from_bytes::<$Type, rkyv::rancor::Error>(&archived);
        assert_eq!(
            restored.is_ok(),
            decodable,
            "{} archive decoding of {text:?}",
            stringify!($Type)
        );
        if let (Ok(decoded), Ok(restored)) = (decoded, restored) {
            assert_eq!(decoded.as_str(), text);
            assert_eq!(restored.as_str(), text);
        }
    }};
}

/// Expands `$check` once for every name type the vocabulary declares.
macro_rules! for_every_name_type {
    ($assertion:ident, $text:expr) => {
        $assertion!(DomainName, $text);
        $assertion!(SchemaName, $text);
        $assertion!(WireSchemaName, $text);
        $assertion!(CodecName, $text);
        $assertion!(ClientName, $text);
        $assertion!(VhostName, $text);
        $assertion!(BranchName, $text);
        $assertion!(EndpointName, $text);
        $assertion!(SignalingProtocolName, $text);
        $assertion!(GeneratorName, $text);
        $assertion!(InferencerName, $text);
        $assertion!(WasmProcessorName, $text);
        $assertion!(IngestorName, $text);
        $assertion!(ReingestorName, $text);
        $assertion!(RelayName, $text);
        $assertion!(LookupName, $text);
        $assertion!(JunctionName, $text);
        $assertion!(DeduplicatorName, $text);
        $assertion!(CorrelatorName, $text);
        $assertion!(ReordererName, $text);
        $assertion!(WindowProcessorName, $text);
        $assertion!(EmitterName, $text);
        $assertion!(PlacementName, $text);
        $assertion!(UdfName, $text);
        $assertion!(ModelName, $text);
        $assertion!(ResourceName, $text);
        $assertion!(UserName, $text);
        $assertion!(SubscriptionName, $text);
        $assertion!(FieldName, $text);
        $assertion!(BuiltinFunctionName, $text);
        $assertion!(ClusterNodeName, $text);
        $assertion!(TopicName, $text);
        $assertion!(QueueName, $text);
        $assertion!(QueueGroupName, $text);
        $assertion!(ChannelName, $text);
        $assertion!(SubjectName, $text);
        $assertion!(TableName, $text);
        $assertion!(CollectionName, $text);
        $assertion!(ConsumerGroupName, $text);
        $assertion!(PulsarSubscriptionName, $text);
    };
}

/// Expands `$check` once for every entity name that widens into a [`ModelName`].
macro_rules! for_every_entity_name_type {
    ($assertion:ident, $text:expr) => {
        $assertion!(SchemaName, $text);
        $assertion!(WireSchemaName, $text);
        $assertion!(CodecName, $text);
        $assertion!(ClientName, $text);
        $assertion!(VhostName, $text);
        $assertion!(BranchName, $text);
        $assertion!(EndpointName, $text);
        $assertion!(SignalingProtocolName, $text);
        $assertion!(GeneratorName, $text);
        $assertion!(InferencerName, $text);
        $assertion!(WasmProcessorName, $text);
        $assertion!(IngestorName, $text);
        $assertion!(ReingestorName, $text);
        $assertion!(RelayName, $text);
        $assertion!(LookupName, $text);
        $assertion!(JunctionName, $text);
        $assertion!(DeduplicatorName, $text);
        $assertion!(CorrelatorName, $text);
        $assertion!(ReordererName, $text);
        $assertion!(WindowProcessorName, $text);
        $assertion!(EmitterName, $text);
        $assertion!(PlacementName, $text);
        $assertion!(UdfName, $text);
        $assertion!(ResourceName, $text);
        $assertion!(SubscriptionName, $text);
        $assertion!(UserName, $text);
    };
}

#[test]
fn bolero_names_round_trip_through_every_representation() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(256)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let dotted = valid_text(arbitrary.entropy(), DotPolicy::Allowed);
            let undotted = valid_text(arbitrary.entropy(), DotPolicy::Rejected);
            macro_rules! valid_for {
                ($Type:ident, $unused:expr) => {
                    if $Type::DOTS == DotPolicy::Rejected {
                        assert_valid_name_round_trips!($Type, undotted.clone());
                    } else {
                        assert_valid_name_round_trips!($Type, dotted.clone());
                    }
                };
            }
            for_every_name_type!(valid_for, ());
            for_every_entity_name_type!(assert_entity_name_widens, dotted.clone());
        });
}

#[test]
fn bolero_name_text_is_validated_or_rejected() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(256)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let text = arbitrary_text(arbitrary.entropy());
            for_every_name_type!(assert_name_text_is_validated, &text);
        });
}
