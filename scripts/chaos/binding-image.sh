#!/usr/bin/env bash
# Source-derived binding tool images have one immutable identity for a suite. Their source and
# installed-wheel provenance must agree with the product revision before the client starts.

binding_image_labels_match() {
    local labels="$1"
    local revision="$2"
    [[ -n "${revision}" ]] || return 1
    jq -e --arg revision "${revision}" '
        .["org.opencontainers.image.revision"] == $revision
        and .["io.nervix.chaos.client"] == "python"
    ' <<<"${labels}" >/dev/null
}

binding_image_provenance_matches() {
    local file="$1"
    local revision="$2"
    jq -e --arg revision "${revision}" '
        .revision == $revision and (.wheel | type == "string" and startswith("nervix_client-") and endswith(".whl"))
        and (.wheel_sha256 | type == "string" and test("^[a-f0-9]{64}$"))
    ' "${file}" >/dev/null
}
