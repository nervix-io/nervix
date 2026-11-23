#!/bin/sh
set -eu

# Starts a node of the node-settings scenarios in place of the image's own command, inside the node's
# container: node-launch.sh SETTINGS_FILE SERVER. Every line of SETTINGS_FILE is one
# NERVIX_NAME=VALUE assignment that replaces the deployment's value of that environment setting for
# this process only, and an empty or absent file starts the server with the deployment's settings.
# The controller rewrites the file only while the node is stopped, so each start of the same
# container runs the settings chosen for it. Every applied assignment is logged before the server
# starts, so the node's log names the settings each of its processes ran with. The server replaces
# this shell, so it receives the container's signals and its exit status is the container's.

[ "$#" -eq 2 ] || {
    printf 'usage: node-launch.sh SETTINGS_FILE SERVER\n' >&2
    exit 2
}
settings="$1"
server="$2"

if [ -f "${settings}" ]; then
    while IFS= read -r assignment || [ -n "${assignment}" ]; do
        name="${assignment%%=*}"
        case "${assignment}" in
            NERVIX_?*=*) ;;
            *)
                printf 'node launch: refusing a line that assigns no NERVIX_ setting: %s\n' "${assignment}" >&2
                exit 2
                ;;
        esac
        # The class lists its letters, so no locale's collation can widen it.
        case "${name}" in
            *[!ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_]*)
                printf 'node launch: refusing a setting name of other than upper-case letters, digits and underscores: %s\n' "${name}" >&2
                exit 2
                ;;
        esac
        export "${assignment}"
        printf 'node launch: %s\n' "${assignment}" >&2
    done <"${settings}"
fi
exec "${server}"
