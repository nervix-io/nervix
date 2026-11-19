#!/bin/sh
set -u

# Follows the server notices one node's sessions receive, with the packaged CLI, and stamps every
# line it prints with this container's clock, which is the host's, in Unix nanoseconds. A notice of
# kind runtime reports a failure of any node of the cluster, which every session receives.
#
# The CLI's subscribe mode prints server notices while it follows a subscription. The observer
# subscribes to a relay no record ever reaches, so it prints nothing else. A follow that ends is
# started again, so the observer attaches again instead of exiting; each start begins with its own
# subscription line, and each end is stamped.
while :; do
    nervix-cli --server "http://${CHAOS_EVENTS_HOST}:47391" --domain "${CHAOS_EVENTS_DOMAIN}" \
        --password "${NERVIX_PASSWORD}" \
        subscribe "events_${CHAOS_EVENTS_HOST#nervix-}" "${CHAOS_EVENTS_RELAY}" --dropping 2>&1 \
        | while IFS= read -r line; do
            printf '%s %s\n' "$(date +%s%N)" "${line}"
        done
    printf '%s observer: the notice follow ended\n' "$(date +%s%N)"
    sleep 1
done
