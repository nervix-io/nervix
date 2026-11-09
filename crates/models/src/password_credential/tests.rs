//! Complete current credential round trips and malformed-cost refusals.

use super::*;

fn credential(input: &[u8; 128]) -> PasswordCredential {
    let generated = nervix_arbitrary::Arbitrary::new(input, nervix_arbitrary::Domain::Vocabulary)
        .password_credential();
    PasswordCredential::from_parts(
        u64::from(generated.cost().get().get()),
        *generated.salt(),
        *generated.digest(),
    )
    .assured("the shared generator supplies a complete bounded credential")
}

#[test]
fn bolero_password_credentials_preserve_the_complete_value() {
    bolero::check!().with_type::<[u8; 128]>().for_each(|input| {
        let original = credential(input);
        let encoded =
            rkyv::to_bytes::<rkyv::rancor::Error>(&original).assured("bounded credentials archive");
        let restored: PasswordCredential = rkyv::from_bytes::<_, rkyv::rancor::Error>(&encoded)
            .assured("the complete valid credential decodes");
        assert_eq!(restored, original);
        let json = serde_json::to_vec(&original)
            .assured("the current credential has a JSON representation");
        let restored: PasswordCredential =
            serde_json::from_slice(&json).assured("the complete valid credential decodes");
        assert_eq!(restored, original);
    });
}

#[test]
fn bolero_malformed_password_credentials_refuse_unbounded_work() {
    bolero::check!().with_type::<[u8; 128]>().for_each(|input| {
        let original = credential(input);
        for invalid in [
            0,
            PasswordIterations::MAXIMUM
                .checked_add(1)
                .assured("the declared bound is below u64::MAX"),
            u64::MAX,
        ] {
            let mut json =
                serde_json::to_value(&original).assured("the valid credential serializes");
            json["Pbkdf2HmacSha512"]["cost"] = serde_json::Value::from(invalid);
            assert!(serde_json::from_value::<PasswordCredential>(json).is_err());
            let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&original)
                .assured("the valid credential archives");
            let archived = rkyv::access::<ArchivedPasswordCredential, rkyv::rancor::Error>(&bytes)
                .assured("the current archive validates");
            let ArchivedPasswordCredential::Pbkdf2HmacSha512 { cost, .. } = archived;
            let offset = std::ptr::from_ref(&cost.0)
                .addr()
                .checked_sub(bytes.as_ptr().addr())
                .assured("the archived cost belongs to this buffer");
            let end = offset
                .checked_add(std::mem::size_of::<u64>())
                .assured("one fixed archived field fits its buffer");
            bytes[offset..end].copy_from_slice(&invalid.to_le_bytes());
            assert!(rkyv::from_bytes::<PasswordCredential, rkyv::rancor::Error>(&bytes).is_err());
        }
    });
}

#[test]
fn password_costs_validate_before_any_cryptographic_operation() {
    for count in [1, 210000, PasswordIterations::MAXIMUM] {
        let cost = PasswordCost::try_from(count).assured("the explicit boundary count is accepted");
        assert_eq!(u64::from(cost.get().get()), count);
    }
    assert!(PasswordCost::try_from(0).is_err());
    assert!(
        PasswordCost::try_from(
            PasswordIterations::MAXIMUM
                .checked_add(1)
                .assured("the bound leaves room")
        )
        .is_err()
    );
}
