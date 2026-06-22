#!/usr/bin/env bash
set -euo pipefail

tls_dir="tls/dev"
ca_cert="${tls_dir}/ca.pem"
ca_key="${tls_dir}/ca-key.pem"
node_cert="${tls_dir}/node.pem"
node_key="${tls_dir}/node-key.pem"
node_bundle="${tls_dir}/node-bundle.pem"
kafka_keystore="${tls_dir}/kafka.keystore.p12"
kafka_keystore_password_file="${tls_dir}/kafka_keystore_creds"
kafka_key_password_file="${tls_dir}/kafka_key_creds"
kafka_keystore_password="nervix-dev-kafka"

tls_assets_are_valid() {
    [[ -f "${ca_cert}" && -f "${ca_key}" && -f "${node_cert}" && -f "${node_key}" && -f "${node_bundle}" && -f "${kafka_keystore}" && -f "${kafka_keystore_password_file}" && -f "${kafka_key_password_file}" ]] || return 1
    openssl x509 -in "${ca_cert}" -noout >/dev/null 2>&1 || return 1
    openssl x509 -in "${node_cert}" -noout >/dev/null 2>&1 || return 1
    openssl pkey -in "${node_key}" -noout >/dev/null 2>&1 || return 1
    openssl verify -CAfile "${ca_cert}" "${node_cert}" >/dev/null 2>&1 || return 1
    openssl pkcs12 -in "${kafka_keystore}" -passin "pass:${kafka_keystore_password}" -nokeys >/dev/null 2>&1 || return 1
    local cert_pubkey key_pubkey
    cert_pubkey="$(openssl x509 -in "${node_cert}" -pubkey -noout 2>/dev/null | openssl pkey -pubin -outform pem 2>/dev/null)" || return 1
    key_pubkey="$(openssl pkey -in "${node_key}" -pubout -outform pem 2>/dev/null)" || return 1
    [[ "${cert_pubkey}" == "${key_pubkey}" ]] || return 1
    [[ "$(cat "${kafka_keystore_password_file}")" == "${kafka_keystore_password}" ]] || return 1
    [[ "$(cat "${kafka_key_password_file}")" == "${kafka_keystore_password}" ]] || return 1
}

if tls_assets_are_valid; then
    exit 0
fi

mkdir -p "${tls_dir}"

openssl_cnf="$(mktemp)"
leaf_cnf="$(mktemp)"
node_csr="$(mktemp)"
trap 'rm -f "${openssl_cnf}" "${leaf_cnf}" "${node_csr}"' EXIT

cat > "${openssl_cnf}" <<'EOF'
[ req ]
default_bits       = 2048
distinguished_name = req_distinguished_name
prompt             = no
x509_extensions    = v3_ca

[ req_distinguished_name ]
CN = nervix-dev-ca

[ v3_ca ]
basicConstraints = critical, CA:TRUE
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
EOF

cat > "${leaf_cnf}" <<'EOF'
[ req ]
default_bits       = 2048
distinguished_name = req_leaf_distinguished_name
prompt             = no
req_extensions     = v3_req

[ req_leaf_distinguished_name ]
CN = localhost

[ v3_req ]
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

rm -f "${ca_cert}" "${ca_key}" "${node_cert}" "${node_key}" "${node_bundle}" \
    "${kafka_keystore}" "${kafka_keystore_password_file}" "${kafka_key_password_file}"

openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "${ca_key}" \
    -out "${ca_cert}" \
    -days 3650 \
    -config "${openssl_cnf}" >/dev/null 2>&1

openssl req -new -newkey rsa:2048 -nodes \
    -keyout "${node_key}" \
    -out "${node_csr}" \
    -config "${leaf_cnf}" >/dev/null 2>&1

openssl x509 -req \
    -in "${node_csr}" \
    -CA "${ca_cert}" \
    -CAkey "${ca_key}" \
    -CAcreateserial \
    -out "${node_cert}" \
    -days 3650 \
    -extensions v3_req \
    -extfile "${leaf_cnf}" >/dev/null 2>&1

cat "${node_key}" "${node_cert}" "${ca_cert}" > "${node_bundle}"
printf '%s' "${kafka_keystore_password}" > "${kafka_keystore_password_file}"
printf '%s' "${kafka_keystore_password}" > "${kafka_key_password_file}"
openssl pkcs12 -export \
    -in "${node_cert}" \
    -inkey "${node_key}" \
    -name kafka \
    -certfile "${ca_cert}" \
    -out "${kafka_keystore}" \
    -passout "pass:${kafka_keystore_password}" >/dev/null 2>&1

chmod 600 "${ca_key}"
chmod 644 "${ca_cert}" "${node_cert}" "${node_key}" "${node_bundle}" \
    "${kafka_keystore}" "${kafka_keystore_password_file}" "${kafka_key_password_file}"

rm -f "${tls_dir}/ca.srl"
