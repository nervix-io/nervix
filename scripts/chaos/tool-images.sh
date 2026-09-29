#!/usr/bin/env bash
# Sourced by the chaos runner and its self-test. Pumba 1.2.1 and the nettools helper published with
# it are the qualified fault injectors; both are pinned by multi-architecture digest.

# shellcheck disable=SC2034
chaos_pumba_image="ghcr.io/alexei-led/pumba@sha256:780505fe261932765921c94a23e24bda107c72927654c26a5893a238d7708bf0"
# shellcheck disable=SC2034
chaos_nettools_image="ghcr.io/alexei-led/pumba-alpine-nettools@sha256:bdcfacdd0c42f64cd280319d6dd5d3911f33425ce4a5168bb1b7de9570b72cd1"
