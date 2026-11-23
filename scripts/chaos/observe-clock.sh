#!/bin/sh
set -u

# Follows one domain's clock through one node's public session route with the packaged CLI and
# stamps every line it prints with this container's clock, which is the host's, in Unix
# nanoseconds. A follow that ends is started again, so an outage of the node leaves the observer
# attaching again instead of exiting; each start begins with its own attach line.
#
# The verdict attributes every tick to the node this observer follows. By default the client gives
# up a session whose node stays silent past its keepalive interval and timeout (30 s and 20 s) and
# attaches the clock again through another node it knows, so a long pause would move the observer
# off its node. An interval of one day, the longest client deadline, keeps the client waiting for
# its node through every pause a scenario holds.
while :; do
    nervix-cli --server "http://${CHAOS_CLOCK_HOST}:47391" --domain "${CHAOS_CLOCK_DOMAIN}" \
        --password "${NERVIX_PASSWORD}" --keepalive-interval 1d domain-clock 2>&1 \
        | while IFS= read -r line; do
            printf '%s %s\n' "$(date +%s%N)" "${line}"
        done
    printf '%s observer: the domain-clock follow ended\n' "$(date +%s%N)"
    sleep 1
done
