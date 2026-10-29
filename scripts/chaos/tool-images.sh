#!/usr/bin/env bash
# Sourced by the chaos runner and its self-tests. Every tool image a run starts is pinned here by a
# registry-qualified digest: the Apache Kafka 3.9.1 broker and topic administration image, the kcat
# 1.7.1 producer and consumer, the Alpine 3.22.6 probe, observer and event-marker image, the Python
# 3.13.18 Alpine image that runs the endpoint client, the PostgreSQL 17.11 Alpine server that the
# lifetimes scenario's pooled emitters write to, and the qualified fault injectors, Pumba 1.2.1 and
# the nettools helper published with it. Kafka, Alpine, Python, PostgreSQL, Pumba and nettools are
# pinned by multi-architecture index digest; kcat 1.7.1 is published for linux/amd64 only, so its
# digest names that single manifest.

# shellcheck disable=SC2034
chaos_kafka_image="docker.io/apache/kafka@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed"
# shellcheck disable=SC2034
chaos_kcat_image="docker.io/edenhill/kcat@sha256:8f16a5fed099931ce1122420b7473efe467ff9841d53680b99db25dd1723d711"
# shellcheck disable=SC2034
chaos_probe_image="docker.io/library/alpine@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8"
# shellcheck disable=SC2034
chaos_postgres_image="docker.io/library/postgres@sha256:266d3c257594e195b309ae9d1805e54c788f1d7b375f56bf6c367aac6c917e7b"
# shellcheck disable=SC2034
chaos_python_image="docker.io/library/python@sha256:bbc0290146ebe0a8b9a8ed07d3f2c7bdeb4d917fc93300a4e47490c8b5a11b48"
# shellcheck disable=SC2034
chaos_pumba_image="ghcr.io/alexei-led/pumba@sha256:780505fe261932765921c94a23e24bda107c72927654c26a5893a238d7708bf0"
# shellcheck disable=SC2034
chaos_nettools_image="ghcr.io/alexei-led/pumba-alpine-nettools@sha256:bdcfacdd0c42f64cd280319d6dd5d3911f33425ce4a5168bb1b7de9570b72cd1"
