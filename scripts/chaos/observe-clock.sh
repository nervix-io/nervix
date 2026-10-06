#!/bin/sh
set -u

# Follows one domain's clock through one node's public session route with the packaged CLI and
# stamps every line it prints with this container's clock, which is the host's, in Unix
# nanoseconds. A follow that ends is started again, so an outage of the node leaves the observer
# attaching again instead of exiting; each start begins with its own attach line.
while :; do
    nervix-cli --server "http://${CHAOS_CLOCK_HOST}:47391" --domain "${CHAOS_CLOCK_DOMAIN}" \
        --password "${NERVIX_PASSWORD}" domain-clock 2>&1 \
        | while IFS= read -r line; do
            printf '%s %s\n' "$(date +%s%N)" "${line}"
        done
    printf '%s observer: the domain-clock follow ended\n' "$(date +%s%N)"
    sleep 1
done
