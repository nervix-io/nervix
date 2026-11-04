//! Outside the layer order: the Pulsar broker that authenticates its clients.
//!
//! - **Owns.** The second standalone Pulsar broker beside the shared one, which takes a token on
//!   its plaintext port and requires a certificate of the scenario certificate authority on its TLS
//!   port, the key it verifies tokens with, the tokens a run is given, and its endpoints.
//! - **Depends on.** The environment's container lifecycle, readiness probe and TLS material, and
//!   Testcontainers.
//! - **Must not know.** Nervix, its Pulsar client declarations, or the shared broker's clients.

use std::io;

use testcontainers::{
    CopyTargetOptions, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::HttpWaitStrategy},
};

use crate::{
    ContainerReadiness, DependencyEnvironment, PULSAR_AUTH_ADDR, PULSAR_AUTH_FORGED_TOKEN,
    PULSAR_AUTH_TLS_ADDR, PULSAR_AUTH_TOKEN, RunningContainer, STARTUP_TIMEOUT,
    http_readiness::HttpReadiness, mapped_port,
};

/// The key the authenticating Pulsar broker verifies tokens with, in base64. It protects nothing
/// outside that broker's own scenario container.
const PULSAR_AUTH_SIGNING_KEY: &str = "bmVydml4LXNjZW5hcmlvLXB1bHNhci10b2tlbi1rZXk=";
/// The role [`PULSAR_AUTH_ACCEPTED_TOKEN`] names, which is also the broker's own superuser.
const PULSAR_AUTH_ROLE: &str = "nervix";
/// An HS256 token for [`PULSAR_AUTH_ROLE`], signed with [`PULSAR_AUTH_SIGNING_KEY`].
const PULSAR_AUTH_ACCEPTED_TOKEN: &str =
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJuZXJ2aXgifQ.zWV7XjUC1T7Ax-6JB6dR4YB0uVZUiIfkGo48cpdyuo0";
/// The claims of [`PULSAR_AUTH_ACCEPTED_TOKEN`] signed with another key.
const PULSAR_AUTH_FORGED: &str =
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJuZXJ2aXgifQ.jsC6MyqBudjrNmsNb6km1OQKy7pWG5Uj4ObwGes15eA";
/// A few scenarios run on the authenticating broker, so it takes far less memory than the shared
/// one beside it.
const PULSAR_AUTH_MEMORY: &str = "-Xms512m -Xmx1g -XX:MaxDirectMemorySize=1g";

impl DependencyEnvironment {
    /// A second standalone broker that authenticates every client: its plaintext port takes a
    /// token, and its TLS port also requires a certificate of the scenario certificate authority.
    /// A scenario on this broker cannot disturb the shared one, which accepts every client.
    pub async fn start_pulsar_auth(&mut self) -> io::Result<()> {
        if !self.mark_starting("pulsar-auth").await? {
            return Ok(());
        }
        let tls = self.ensure_tls()?.clone();
        let start_script = format!(
            r#"#!/bin/sh
set -eu
config=/tmp/standalone-auth.conf
cp /pulsar/conf/standalone.conf "$config"
cat >>"$config" <<'EOF'
brokerServicePortTls=6651
webServicePortTls=8443
tlsEnabled=true
tlsCertificateFilePath=/pulsar/certs/node.pem
tlsKeyFilePath=/pulsar/certs/node-key.pem
tlsTrustCertsFilePath=/pulsar/certs/ca.pem
tlsAllowInsecureConnection=false
tlsRequireTrustedClientCertOnConnect=true
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken,org.apache.pulsar.broker.authentication.AuthenticationProviderTls
tokenSecretKey=data:;base64,{PULSAR_AUTH_SIGNING_KEY}
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
brokerClientAuthenticationParameters=token:{PULSAR_AUTH_ACCEPTED_TOKEN}
superUserRoles={PULSAR_AUTH_ROLE}
EOF
exec /pulsar/bin/pulsar standalone --no-functions-worker --no-stream-storage -c "$config"
"#
        );
        let namespace_path = "/admin/v2/namespaces/public/default";
        let readiness_client = HttpReadiness::client()?;
        let container = self
            .start_container_with_readiness(
                "pulsar-auth",
                ContainerReadiness::Http {
                    port: 8080.tcp(),
                    path: namespace_path,
                    bearer_token: Some(PULSAR_AUTH_ACCEPTED_TOKEN),
                },
                "authenticated Pulsar public/default namespace",
                || {
                    GenericImage::new("apachepulsar/pulsar", "4.1.0")
                        .with_exposed_port(6650.tcp())
                        .with_exposed_port(6651.tcp())
                        .with_exposed_port(8080.tcp())
                        .with_wait_for(WaitFor::http(
                            HttpWaitStrategy::new(namespace_path)
                                .with_port(8080.tcp())
                                .with_client(readiness_client.clone())
                                .with_bearer_auth(PULSAR_AUTH_ACCEPTED_TOKEN)
                                .with_expected_status_code(200_u16),
                        ))
                        .with_env_var("PULSAR_MEM", PULSAR_AUTH_MEMORY)
                        .with_copy_to(
                            CopyTargetOptions::new("/pulsar/bin/nervix-start-auth.sh")
                                .with_mode(0o755),
                            start_script.clone().into_bytes(),
                        )
                        .with_copy_to("/pulsar/certs/ca.pem", tls.ca_pem.clone())
                        .with_copy_to("/pulsar/certs/node.pem", tls.node_pem.clone())
                        .with_copy_to(
                            CopyTargetOptions::new("/pulsar/certs/node-key.pem").with_mode(0o644),
                            tls.node_key_pem.clone(),
                        )
                        .with_cmd(["sh", "/pulsar/bin/nervix-start-auth.sh"])
                        .with_startup_timeout(STARTUP_TIMEOUT)
                },
            )
            .await?;
        let plaintext_port = mapped_port(&container, 6650, "authenticated Pulsar").await?;
        let tls_port = mapped_port(&container, 6651, "authenticated Pulsar TLS").await?;
        self.endpoints.insert(
            PULSAR_AUTH_ADDR,
            format!("pulsar://127.0.0.1:{plaintext_port}"),
        );
        self.endpoints.insert(
            PULSAR_AUTH_TLS_ADDR,
            format!("pulsar+ssl://127.0.0.1:{tls_port}"),
        );
        self.endpoints
            .insert(PULSAR_AUTH_TOKEN, PULSAR_AUTH_ACCEPTED_TOKEN.to_string());
        self.endpoints
            .insert(PULSAR_AUTH_FORGED_TOKEN, PULSAR_AUTH_FORGED.to_string());
        self.containers.push(RunningContainer::Generic(container));
        Ok(())
    }
}
