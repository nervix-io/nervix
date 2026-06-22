#!/bin/sh
set -eu

config=/tmp/standalone-tls.conf
cp /pulsar/conf/standalone.conf "$config"

cat >>"$config" <<'EOF'

advertisedAddress=127.0.0.1
brokerServicePortTls=6651
webServicePortTls=8443
tlsEnabled=true
tlsCertificateFilePath=/pulsar/certs/node.pem
tlsKeyFilePath=/pulsar/certs/node-key.pem
tlsTrustCertsFilePath=/pulsar/certs/ca.pem
tlsAllowInsecureConnection=false
tlsRequireTrustedClientCertOnConnect=false
EOF

exec /pulsar/bin/pulsar standalone --no-functions-worker --no-stream-storage -c "$config"
