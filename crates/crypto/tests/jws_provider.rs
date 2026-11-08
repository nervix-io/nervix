//! Process-isolated refusal when another caller preinstalls the native library's default.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_crypto::{
    ecdsa::EcdsaP256KeyPair,
    jws::{JwsError, SigningKey},
};

#[test]
fn an_external_default_cannot_select_the_boundary_provider() {
    jsonwebtoken::crypto::aws_lc::DEFAULT_PROVIDER
        .install_default()
        .assured("this integration test owns its process");
    let key = EcdsaP256KeyPair::generate_pkcs8().assured("generated P-256 fixture");
    let error = SigningKey::from_pkcs8(key.as_ref())
        .err()
        .assured("the boundary must install its own provider");
    assert!(matches!(
        error.current_context(),
        JwsError::ProviderUnavailable
    ));
}
