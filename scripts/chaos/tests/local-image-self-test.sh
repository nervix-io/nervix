#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repository="$(cd "${script_dir}/../../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
mkdir -p "${tmp_dir}/bin"

# Execute the production recipe with inert build tools. The Docker boundary must receive the
# identity of the checkout supplying the binaries, independently of the base image's identity.
cat >"${tmp_dir}/bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == build ]]
for profile in debug release; do
    mkdir -p "${CARGO_TARGET_DIR}/${profile}"
    for binary in nervix-server nervix-cli nervix-deadlock-report; do
        printf '#!/bin/sh\nexit 0\n' >"${CARGO_TARGET_DIR}/${profile}/${binary}"
    done
done
EOF
cat >"${tmp_dir}/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$*" in
    'rev-parse HEAD') printf '%s\n' "${CHAOS_IMAGE_TEST_HEAD}" ;;
    'status --porcelain --untracked-files=no')
        if [[ "${CHAOS_IMAGE_TEST_STATUS_FAILURE:-false}" == true ]]; then
            printf 'checkout status refused\n' >&2
            exit 75
        fi
        if [[ "${CHAOS_IMAGE_TEST_DIRTY}" == true ]]; then
            printf ' M justfile\n'
        fi
        ;;
    *) exit 2 ;;
esac
EOF
cat >"${tmp_dir}/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == build ]]
: >"${CHAOS_IMAGE_TEST_LABELS}"
while (($#)); do
    if [[ "$1" == --label ]]; then
        printf '%s\n' "$2" >>"${CHAOS_IMAGE_TEST_LABELS}"
        shift 2
    else
        shift
    fi
done
EOF
printf '#!/bin/sh\nexit 0\n' >"${tmp_dir}/bin/llvm-strip-23"
chmod +x "${tmp_dir}/bin/"*

head=0123456789abcdef0123456789abcdef01234567
status=0
for recipe in build-chaos-local-image build-chaos-diagnostic-image; do
    for dirty in false true; do
        expected="${head}"
        if [[ "${dirty}" == true ]]; then expected="${expected}-dirty"; fi
        PATH="${tmp_dir}/bin:${PATH}" CARGO_TARGET_DIR="${tmp_dir}/target" \
            CHAOS_IMAGE_TEST_HEAD="${head}" CHAOS_IMAGE_TEST_DIRTY="${dirty}" \
            CHAOS_IMAGE_TEST_LABELS="${tmp_dir}/labels" \
            just --no-deps --justfile "${repository}/justfile" \
            "${recipe}" nervix:image-identity-test nervix:base \
            >"${tmp_dir}/command.log" 2>&1 || {
                cat "${tmp_dir}/command.log" >&2
                exit 1
            }
        if ! grep -Fxq "org.opencontainers.image.revision=${expected}" "${tmp_dir}/labels"; then
            printf 'local image self-test failed: %s dirty=%s must label revision %s\n' \
                "${recipe}" "${dirty}" "${expected}" >&2
            status=1
        fi
    done
    rm -f "${tmp_dir}/labels"
    if PATH="${tmp_dir}/bin:${PATH}" CARGO_TARGET_DIR="${tmp_dir}/target" \
        CHAOS_IMAGE_TEST_HEAD="${head}" CHAOS_IMAGE_TEST_DIRTY=false \
        CHAOS_IMAGE_TEST_STATUS_FAILURE=true CHAOS_IMAGE_TEST_LABELS="${tmp_dir}/labels" \
        just --no-deps --justfile "${repository}/justfile" \
        "${recipe}" nervix:image-identity-test nervix:base >"${tmp_dir}/command.log" 2>&1; then
        printf 'local image self-test failed: %s accepted a refused checkout status\n' "${recipe}" >&2
        status=1
    fi
    if [[ -e "${tmp_dir}/labels" ]] || ! grep -Fq 'checkout status refused' "${tmp_dir}/command.log"; then
        printf 'local image self-test failed: %s must stop before Docker and retain the Git refusal\n' "${recipe}" >&2
        status=1
    fi
done
if ((status)); then exit "${status}"; fi
printf 'local image checkout identity controls passed\n'
