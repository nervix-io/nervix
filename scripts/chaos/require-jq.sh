#!/usr/bin/env bash
# Sourced by the chaos runner, its self-test and the suite runner. Their verifiers and scenario
# scripts are written for jq 1.8, whose grammar admits expressions that jq 1.7 refuses to compile,
# so a controller whose jq is older stops before it starts anything instead of failing a verifier.

# True when the jq on PATH is 1.8 or later.
chaos_jq_supported() {
    local version major minor
    version="$(jq --version 2>/dev/null)" || return 1
    version="${version#jq-}"
    major="${version%%.*}"
    minor="${version#*.}"
    minor="${minor%%[!0-9]*}"
    [[ "${major}" =~ ^[0-9]+$ && "${minor}" =~ ^[0-9]+$ ]] || return 1
    ((major > 1 || (major == 1 && minor >= 8)))
}

# Describes the jq a controller found, for the error that refuses it.
chaos_jq_found() {
    jq --version 2>/dev/null || printf 'no jq on PATH'
}
