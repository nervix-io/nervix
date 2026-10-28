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
# A node certificate carries exactly one node identity, so the other nodes of the local cluster
# that `just cluster-dashboard` starts each have their own, issued by the same CA as node-1's.
peer_node_ids=(node-2 node-3)

# Whether `certificate` and `key` are a matching pair the CA issued for `node_id`.
node_identity_is_valid() {
    local node_id="$1"
    local certificate="$2"
    local key="$3"
    [[ -f "${certificate}" && -f "${key}" ]] || return 1
    openssl x509 -in "${certificate}" -noout >/dev/null 2>&1 || return 1
    openssl x509 -in "${certificate}" -noout -ext subjectAltName 2>/dev/null \
        | grep -Fq "URI:nervix://cluster/default/node/${node_id}" || return 1
    openssl x509 -in "${certificate}" -noout -ext subjectAltName 2>/dev/null \
        | grep -Fq 'DNS:native-session.nervix.test' || return 1
    openssl pkey -in "${key}" -noout >/dev/null 2>&1 || return 1
    openssl verify -CAfile "${ca_cert}" "${certificate}" >/dev/null 2>&1 || return 1
    local cert_pubkey key_pubkey
    cert_pubkey="$(openssl x509 -in "${certificate}" -pubkey -noout 2>/dev/null | openssl pkey -pubin -outform pem 2>/dev/null)" || return 1
    key_pubkey="$(openssl pkey -in "${key}" -pubout -outform pem 2>/dev/null)" || return 1
    [[ "${cert_pubkey}" == "${key_pubkey}" ]] || return 1
}

tls_assets_are_valid() {
    [[ -f "${ca_cert}" && -f "${ca_key}" && -f "${node_bundle}" && -f "${kafka_keystore}" && -f "${kafka_keystore_password_file}" && -f "${kafka_key_password_file}" ]] || return 1
    openssl x509 -in "${ca_cert}" -noout >/dev/null 2>&1 || return 1
    node_identity_is_valid node-1 "${node_cert}" "${node_key}" || return 1
    openssl pkcs12 -in "${kafka_keystore}" -passin "pass:${kafka_keystore_password}" -nokeys >/dev/null 2>&1 || return 1
    [[ "$(cat "${kafka_keystore_password_file}")" == "${kafka_keystore_password}" ]] || return 1
    [[ "$(cat "${kafka_key_password_file}")" == "${kafka_keystore_password}" ]] || return 1
}

work_dir="$(mktemp -d)"
trap 'rm -rf "${work_dir}"' EXIT

# Issues `node_id` a new key and a certificate the CA signs for it, written to `key` and
# `certificate`. Each certificate takes a random serial number, so issuing one shares no serial
# file with another run.
issue_node_identity() {
    local node_id="$1"
    local certificate="$2"
    local key="$3"
    local leaf_cnf="${work_dir}/${node_id}.cnf"
    local node_csr="${work_dir}/${node_id}.csr"

    cat > "${leaf_cnf}" <<EOF
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
DNS.2 = native-session.nervix.test
IP.1 = 127.0.0.1
URI.1 = nervix://cluster/default/node/${node_id}
EOF

    openssl req -new -newkey rsa:2048 -nodes \
        -keyout "${key}" \
        -out "${node_csr}" \
        -config "${leaf_cnf}" >/dev/null 2>&1

    openssl x509 -req \
        -in "${node_csr}" \
        -CA "${ca_cert}" \
        -CAkey "${ca_key}" \
        -set_serial "0x$(openssl rand -hex 16)" \
        -out "${certificate}" \
        -days 3650 \
        -extensions v3_req \
        -extfile "${leaf_cnf}" >/dev/null 2>&1

    chmod 644 "${certificate}" "${key}"
}

mkdir -p "${tls_dir}"

if ! tls_assets_are_valid; then
    ca_cnf="${work_dir}/ca.cnf"
    cat > "${ca_cnf}" <<'EOF'
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

    rm -f "${ca_cert}" "${ca_key}" "${node_cert}" "${node_key}" "${node_bundle}" \
        "${kafka_keystore}" "${kafka_keystore_password_file}" "${kafka_key_password_file}"

    openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout "${ca_key}" \
        -out "${ca_cert}" \
        -days 3650 \
        -config "${ca_cnf}" >/dev/null 2>&1

    issue_node_identity node-1 "${node_cert}" "${node_key}"

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
    chmod 644 "${ca_cert}" "${node_bundle}" \
        "${kafka_keystore}" "${kafka_keystore_password_file}" "${kafka_key_password_file}"
fi

# A peer is issued a new identity when its own is missing or invalid, including one an earlier CA
# issued before the CA was regenerated above.
for peer_node_id in "${peer_node_ids[@]}"; do
    peer_cert="${tls_dir}/${peer_node_id}.pem"
    peer_key="${tls_dir}/${peer_node_id}-key.pem"
    if ! node_identity_is_valid "${peer_node_id}" "${peer_cert}" "${peer_key}"; then
        issue_node_identity "${peer_node_id}" "${peer_cert}" "${peer_key}"
    fi
done
