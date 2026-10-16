#!/usr/bin/env bash
# Campaign only: print a WASM processor guest in WebAssembly text that counts its branch's input
# rows, carries every input row of the chaos backup graph's record into one chaos_padded_out row
# with the count after it, and saves exactly the given number of bytes per branch, the first eight
# of which hold the count. The output envelope is the branch counter's, encoded for this record by
# the guest ABI of the qualified revision.
set -euo pipefail
bytes="$1"
[[ "${bytes}" =~ ^[0-9]+$ ]] && ((bytes >= 8 && bytes <= 64 * 1024 * 1024)) \
    || { echo "padded state size must be 8 bytes to 64 MiB" >&2; exit 2; }
# The save sits at 1 MiB, above the input buffer the host allocates at 64 KiB for one batch.
base=$((1024 * 1024))
pages=$(((base + bytes + 65535) / 65536))
cat <<WAT
(module
  (memory (export "memory") 2)
  (global \$count (mut i64) (i64.const 0))
  (global \$pending (mut i32) (i32.const 0))
  (global \$next (mut i32) (i32.const 0))
  (global \$base (mut i64) (i64.const 0))
  (global \$read_ptr (mut i32) (i32.const 1024))
  (global \$read_len (mut i32) (i32.const 0))
  (data (i32.const 1024) "\ec\02\00\00\14\00\00\00\4e\56\57\59\00\00\00\00\08\00\0c\00\07\00\08\00\08\00\00\00\00\00\00\03\0c\00\00\00\08\00\0c\00\04\00\08\00\08\00\00\00\f4\00\00\00\04\00\00\00\01\00\00\00\10\00\00\00\00\00\0a\00\10\00\04\00\08\00\0c\00\0a\00\00\00\bc\00\00\00\68\00\00\00\10\00\00\00\0c\00\14\00\04\00\08\00\0c\00\10\00\0c\00\00\00\1c\00\00\00\14\00\00\00\0c\00\00\00\04\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\01\00\00\00\0c\00\00\00\08\00\14\00\04\00\08\00\08\00\00\00\10\00\00\00\71\80\5f\6e\1d\2c\3b\4a\00\00\00\00\01\00\00\00\71\80\5f\6e\1d\2c\3b\4a\00\00\00\00\05\00\00\00\48\00\00\00\38\00\00\00\24\00\00\00\18\00\00\00\0c\00\00\00\00\00\06\00\08\00\07\00\06\00\00\00\00\00\00\01\f0\ff\ff\ff\03\00\00\00\f8\ff\ff\ff\02\00\00\00\08\00\08\00\00\00\04\00\08\00\00\00\01\00\00\00\04\00\04\00\04\00\00\00\10\00\00\00\63\68\61\6f\73\5f\70\61\64\64\65\64\5f\6f\75\74\00\00\00\00\c8\01\00\00\ff\ff\ff\ff\78\00\00\00\10\00\00\00\00\00\0a\00\0c\00\0a\00\09\00\04\00\0a\00\00\00\10\00\00\00\00\01\04\00\08\00\08\00\00\00\04\00\08\00\00\00\04\00\00\00\01\00\00\00\14\00\00\00\10\00\14\00\10\00\00\00\0f\00\04\00\00\00\08\00\10\00\00\00\18\00\00\00\20\00\00\00\00\00\00\02\1c\00\00\00\08\00\0c\00\04\00\0b\00\08\00\00\00\40\00\00\00\00\00\00\01\00\00\00\00\00\00\00\00\00\00\00\00\ff\ff\ff\ff\b8\00\00\00\10\00\00\00\0c\00\1a\00\18\00\17\00\04\00\08\00\0c\00\00\00\20\00\00\00\80\00\00\00\00\00\00\00\00\00\00\00\00\00\00\03\04\00\0a\00\18\00\0c\00\08\00\04\00\0a\00\00\00\2c\00\00\00\10\00\00\00\01\00\00\00\00\00\00\00\00\00\00\00\01\00\00\00\01\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\02\00\00\00\00\00\00\00\00\00\00\00\01\00\00\00\00\00\00\00\40\00\00\00\00\00\00\00\08\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\ff\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\4d\3c\2b\1a\8f\7e\6d\5c\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\ff\ff\ff\ff\00\00\00\00")
  (func \$field (param \$table i32) (param \$slot i32) (result i32)
    local.get \$table
    local.get \$table local.get \$table i32.load i32.sub
    i32.const 4 i32.add local.get \$slot i32.const 2 i32.mul i32.add
    i32.load16_u i32.add)
  (func \$follow (param \$address i32) (result i32)
    local.get \$address local.get \$address i32.load i32.add)
  (func \$input_rows (param \$ptr i32) (result i32)
    local.get \$ptr i32.const 4 i32.add call \$follow
    i32.const 1 call \$field call \$follow
    i32.const 1 call \$field call \$follow
    i32.const 0 call \$field call \$follow)
  (func \$grow (param \$pages i32)
    local.get \$pages memory.size i32.gt_u
    if
      local.get \$pages memory.size i32.sub memory.grow
      i32.const -1 i32.eq
      if unreachable end
    end)
  (func (export "nervix_buffer_ptr") (result i32) global.get \$read_ptr)
  (func (export "nervix_buffer_len") (result i32) global.get \$read_len)
  (func (export "nervix_buffer_capacity") (result i32)
    i32.const 983040)
  (func (export "nervix_alloc") (param \$len i32) (result i32)
    local.get \$len i32.const 65536 i32.add i32.const 65535 i32.add
    i32.const 16 i32.shr_u call \$grow
    i32.const 65536)
  (func (export "nervix_init") (param i32 i32) (result i32) (i32.const 0))
  (func (export "nervix_current_domain_time_nanos") (result i64) (i64.const 0))
  (func (export "nervix_process_batch") (param \$ptr i32) (param i32) (result i32)
    (local \$rows i32) (local \$index i32) (local \$tokens i32)
    local.get \$ptr call \$input_rows local.set \$rows
    local.get \$rows i32.load global.set \$pending
    global.get \$pending i32.const 7168 i32.gt_u
    if unreachable end
    i32.const 0 local.set \$index
    block \$copied
      loop \$copy
        local.get \$index global.get \$pending i32.ge_u br_if \$copied
        local.get \$rows i32.const 4 i32.add local.get \$index i32.const 4 i32.mul i32.add
        call \$follow
        i32.const 0 call \$field call \$follow local.set \$tokens
        i32.const 8192 local.get \$index i32.const 8 i32.mul i32.add
        local.get \$tokens i32.const 4 i32.add i64.load
        i64.store
        local.get \$index i32.const 1 i32.add local.set \$index
        br \$copy
      end
    end
    i32.const 0 global.set \$next
    global.get \$count global.set \$base
    global.get \$count global.get \$pending i64.extend_i32_u i64.add global.set \$count
    i32.const 0)
  (func (export "nervix_on_timeout") (param i64) (result i32) (i32.const 0))
  (func (export "nervix_flush") (result i32) (i32.const 0))
  (func (export "nervix_read_emit") (result i32)
    (local \$token i64)
    global.get \$next global.get \$pending i32.ge_u
    if (result i32)
      i32.const 0
    else
      i32.const 8192 global.get \$next i32.const 8 i32.mul i32.add i64.load
      local.set \$token
      i32.const 1184 local.get \$token i64.store
      i32.const 1200 local.get \$token i64.store
      i32.const 1704
      global.get \$base global.get \$next i64.extend_i32_u i64.add i64.const 1 i64.add
      i64.store
      global.get \$next i32.const 1 i32.add global.set \$next
      i32.const 1024 global.set \$read_ptr
      i32.const 752 global.set \$read_len
      i32.const 752
    end)
  (func (export "nervix_dump_state") (result i32)
    i32.const ${pages} call \$grow
    i32.const ${base} global.get \$count i64.store
    i32.const ${base} global.set \$read_ptr
    i32.const ${bytes} global.set \$read_len
    i32.const ${bytes})
  (func (export "nervix_load_state") (param \$ptr i32) (param \$len i32) (result i32)
    local.get \$len i32.const ${bytes} i32.ne
    if (result i32)
      i32.const -8
    else
      local.get \$ptr i64.load global.set \$count
      i32.const 0
    end)
  (func (export "nervix_reset_state") (result i32)
    i64.const 0 global.set \$count
    i32.const 0 global.set \$pending
    i32.const 0 global.set \$next
    i32.const 0)
)
WAT
