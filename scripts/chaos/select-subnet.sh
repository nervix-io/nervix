#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage: select-subnet.sh RUN_ID < USED_CIDRS

Print the first 10.213.N.0/24 run network that overlaps none of the IPv4 networks or addresses
read from standard input, one per line. N starts at a value derived from RUN_ID so concurrent runs
begin their search at different networks. Lines that are not IPv4 CIDRs or addresses are ignored.
EOF
}

if [[ "$#" -ne 1 || -z "$1" ]]; then
    usage
    exit 2
fi

run_id="$1"
checksum="$(printf '%s' "${run_id}" | cksum)"
checksum="${checksum%% *}"
first_block=$((checksum % 256))

awk -v first_block="${first_block}" '
    BEGIN {
        used_count = 0
    }
    function address_value(address,    octets) {
        split(address, octets, ".")
        return ((octets[1] * 256 + octets[2]) * 256 + octets[3]) * 256 + octets[4]
    }
    function valid_address(address,    octets, count, index_value) {
        count = split(address, octets, ".")
        if (count != 4) {
            return 0
        }
        for (index_value = 1; index_value <= 4; index_value++) {
            if (octets[index_value] !~ /^[0-9]+$/ || octets[index_value] + 0 > 255) {
                return 0
            }
        }
        return 1
    }
    {
        entry = $1
        prefix_length = 32
        address = entry
        if (index(entry, "/") > 0) {
            address = substr(entry, 1, index(entry, "/") - 1)
            prefix_length = substr(entry, index(entry, "/") + 1)
        }
        if (!valid_address(address) || prefix_length !~ /^[0-9]+$/ || prefix_length + 0 > 32) {
            next
        }
        block_size = 2 ^ (32 - prefix_length)
        start = int(address_value(address) / block_size) * block_size
        used_start[used_count] = start
        used_end[used_count] = start + block_size - 1
        used_count++
    }
    END {
        for (offset = 0; offset < 256; offset++) {
            block = (first_block + offset) % 256
            candidate_start = address_value("10.213." block ".0")
            candidate_end = candidate_start + 255
            overlaps = 0
            for (used = 0; used < used_count; used++) {
                if (candidate_start <= used_end[used] && used_start[used] <= candidate_end) {
                    overlaps = 1
                    break
                }
            }
            if (!overlaps) {
                printf "10.213.%d.0/24\n", block
                exit 0
            }
        }
        exit 1
    }
'
