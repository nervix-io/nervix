#!/usr/bin/env bash
# Sourced by the chaos runner and its self-tests. Every tool image a run starts is pinned here by a
# registry-qualified digest: the Apache Kafka 3.9.1 broker and topic administration image, the kcat
# 1.7.1 producer and consumer, the Alpine 3.22.6 probe, observer and event-marker image, and the
# qualified fault injectors, Pumba 1.2.1 and the nettools helper published with it. Kafka, Alpine,
# Pumba and nettools are pinned by multi-architecture index digest; kcat 1.7.1 is published for
# linux/amd64 only, so its digest names that single manifest.

# shellcheck disable=SC2034
chaos_kafka_image="docker.io/apache/kafka@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed"
# shellcheck disable=SC2034
chaos_kcat_image="docker.io/edenhill/kcat@sha256:8f16a5fed099931ce1122420b7473efe467ff9841d53680b99db25dd1723d711"
# shellcheck disable=SC2034
chaos_probe_image="docker.io/library/alpine@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8"
# shellcheck disable=SC2034
chaos_pumba_image="ghcr.io/alexei-led/pumba@sha256:780505fe261932765921c94a23e24bda107c72927654c26a5893a238d7708bf0"
# shellcheck disable=SC2034
chaos_nettools_image="ghcr.io/alexei-led/pumba-alpine-nettools@sha256:bdcfacdd0c42f64cd280319d6dd5d3911f33425ce4a5168bb1b7de9570b72cd1"
