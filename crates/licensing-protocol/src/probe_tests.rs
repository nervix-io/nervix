//! Focused qualification of maintained JWS owners before defining the licensing profile.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_crypto::{ecdsa::EcdsaP256KeyPair, jws};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    value: String,
}

#[test]
fn maintained_jws_preserves_exact_signed_payload_and_raw_header() {
    let private = EcdsaP256KeyPair::generate_pkcs8().assured("AWS-LC key generation succeeds");
    let signer = jws::SigningKey::from_pkcs8(private.as_ref()).assured("generated P-256 key");
    let point = signer.public_coordinates();
    let jwk = jws::Jwk {
        common: jws::CommonParameters::default(),
        algorithm: jws::AlgorithmParameters::EllipticCurve(jws::EllipticCurveKeyParameters {
            key_type: jws::EllipticCurveKeyType::EC,
            curve: jws::EllipticCurve::P256,
            x: URL_SAFE_NO_PAD.encode(point.x),
            y: URL_SAFE_NO_PAD.encode(point.y),
        }),
    };
    let kid = jws::thumbprint(&jwk).assured("valid public JWK");
    let original = Payload {
        value: "complete Unicode payload: λ\n".to_owned(),
    };
    let token = signer
        .sign("nervix.licensing.decision", &kid, &original)
        .assured("valid ES256 payload");
    let parsed: jose_jws::Flattened = token.parse().assured("maintained compact parser");
    let protected = parsed
        .signature
        .protected
        .assured("compact JWS carries a protected header");
    let header: serde_json::Value = serde_json::from_slice(protected.as_ref())
        .assured("maintained decoder retains the original protected JSON");
    assert_eq!(header.as_object().assured("header is an object").len(), 3);
    assert_eq!(header["kid"], kid);
    let received: Payload = jws::verify(&token, &jwk).assured("signature verifies");
    assert_eq!(received, original);
}
