#!/usr/bin/env bash
set -euo pipefail

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../binding-image.sh
source "${chaos_dir}/binding-image.sh"
temp="$(mktemp -d)"
trap 'rm -rf "${temp}"' EXIT

revision="$(printf 'a%.0s' {1..40})"
labels="$(jq -nc --arg revision "${revision}" '{"org.opencontainers.image.revision": $revision, "io.nervix.chaos.client": "python"}')"
binding_image_labels_match "${labels}" "${revision}"
if binding_image_labels_match "${labels}" "another-revision"; then
    echo 'binding image self-test: accepted another source revision' >&2
    exit 1
fi
if binding_image_labels_match "${labels}" ''; then
    echo 'binding image self-test: accepted missing product identity' >&2
    exit 1
fi
if binding_image_labels_match '{}' "${revision}"; then
    echo 'binding image self-test: accepted an image without client/source labels' >&2
    exit 1
fi
jq -nc --arg revision "${revision}" --arg digest "$(printf 'b%.0s' {1..64})" \
    '{revision: $revision, wheel: "nervix_client-0.0.0-cp312-abi3-linux_x86_64.whl", wheel_sha256: $digest}' \
    >"${temp}/provenance.json"
binding_image_provenance_matches "${temp}/provenance.json" "${revision}"
for field in revision wheel wheel_sha256; do
    jq --arg field "${field}" 'del(.[$field])' "${temp}/provenance.json" >"${temp}/incomplete.json"
    if binding_image_provenance_matches "${temp}/incomplete.json" "${revision}"; then
        echo "binding image self-test: accepted provenance missing ${field}" >&2
        exit 1
    fi
done
if "${chaos_dir}/run-baseline.sh" --image fixture --binding-image fixture >"${temp}/rejection.txt" 2>&1; then
    echo 'binding image self-test: accepted the binding workload outside binding-endpoints' >&2
    exit 1
fi
grep -Fq -- '--binding-image applies only to binding-endpoints' "${temp}/rejection.txt"
expect_refusal() {
    local reason="$1"
    shift
    if "${chaos_dir}/run-baseline.sh" --scenario binding-endpoints --image fixture "$@" \
        >"${temp}/rejection.txt" 2>&1; then
        echo 'binding image self-test: accepted an invalid binding experiment' >&2
        exit 1
    fi
    grep -Fq -- "${reason}" "${temp}/rejection.txt"
}
expect_refusal 'binding-endpoints requires --binding-image'
expect_refusal 'owner-stop requires --nodes 3' --binding-image fixture --nodes 1 --fault owner-stop
expect_refusal '--fault for binding-endpoints must be' --binding-image fixture --fault owner-partition
printf 'binding image self-test passed\n'
