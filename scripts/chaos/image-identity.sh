#!/usr/bin/env bash
# Sourced by the chaos runner and its self-test. An image ID is the image store's own name for an
# image: the config digest in Docker's classic store, and the manifest or index digest in the
# containerd store. A repository digest names the same content in every store, so a replay accepts a
# recorded image through it when the run was recorded on a worker whose store is of the other kind,
# such as a CI worker.

# True when the local image IMAGE carries the repository digest that REFERENCE pins.
chaos_image_carries_digest() {
    local image="$1"
    local reference="$2"
    local digest="${reference##*@}"
    [[ "${reference}" == *@* && "${digest}" =~ ^sha256:[a-f0-9]{64}$ ]] || return 1
    local repo_digests
    repo_digests="$(timeout --foreground --kill-after=5s 30s docker image inspect \
        --format '{{range .RepoDigests}}{{println .}}{{end}}' "${image}" 2>/dev/null)" || return 1
    grep -Fq -- "@${digest}" <<<"${repo_digests}"
}
