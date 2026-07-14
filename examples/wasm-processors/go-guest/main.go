//go:build tinygo

package main

import (
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/nervix-wasm-processor-go-guest/tinyipc"
)

const (
	success             int32  = 0
	errInvalidSize      int32  = -1
	errOutOfBounds      int32  = -2
	errNotInitialized   int32  = -3
	errArrowIPC         int32  = -4
	errEnvelope         int32  = -5
	errErrorState       int32  = -6
	defaultTimeoutNanos int64  = 1_000_000_000
	flushEveryBatches   uint64 = 2
	maxGuestBufferBytes        = 4 * 1024 * 1024
)

//go:wasmimport env nervix_domain_time_nanos
func hostDomainTimeNanos() int64

//go:wasmimport env nervix_timeout_after_nanos
func hostTimeoutAfterNanos(delayNanos int64) int64

var fixedBuffer [maxGuestBufferBytes]byte
var buffer []byte
var initMetadata []byte
var pendingBatch []byte
var pendingEmit [][]byte
var globalError []byte
var savedState []byte
var outputRelays []string
var pendingStartRow uint64
var initialized bool
var processedBatches uint64
var processedRows uint64
var lastDomainTimeNanos int64
var lastTimeoutHandle int64
var errorState string

type guestSnapshot struct {
	ProcessedBatches    uint64
	ProcessedRows       uint64
	PendingStartRow     uint64
	LastDomainTimeNanos int64
	LastTimeoutHandle   int64
	PendingBatch        []byte
	InitMetadata        []byte
	SavedState          []byte
	ErrorState          string
}

type envelope struct {
	Kind                   string
	ArrowIPCBatch          []byte
	Acks                   ackSidecar
	GeneratedArrowIPCBatch []byte
	Outputs                []routedOutput
}

type routedOutput struct {
	OutputRelay string
	Columns     []outputColumn
	Acks        ackSidecar
}

type outputColumn struct {
	Kind        string
	ColumnIndex uint32
}

type ackSidecar struct {
	Rows          []outputRow
	Acked         []ackTokenSet
	Nacked        []nackSet
	MessageErrors []messageErrorSet
}

type outputRow struct {
	Tokens      []uint64
	SourceToken *uint64
}

type ackTokenSet struct {
	Tokens []uint64
}

type nackSet struct {
	Tokens []uint64
	Reason string
}

type messageErrorSet struct {
	Tokens []uint64
	Reason string
}

func main() {}

//export nervix_buffer_ptr
func nervixBufferPtr() int32 {
	if len(buffer) == 0 {
		return 0
	}
	return int32(uintptr(unsafe.Pointer(&buffer[0])))
}

//export nervix_buffer_len
func nervixBufferLen() int32 {
	return int32(len(buffer))
}

//export nervix_buffer_capacity
func nervixBufferCapacity() int32 {
	return int32(cap(buffer))
}

//export nervix_global_error_ptr
func nervixGlobalErrorPtr() int32 {
	if len(globalError) == 0 {
		return 0
	}
	return int32(uintptr(unsafe.Pointer(&globalError[0])))
}

//export nervix_global_error_len
func nervixGlobalErrorLen() int32 {
	return int32(len(globalError))
}

//export nervix_clear_global_error
func nervixClearGlobalError() int32 {
	clearGlobalError()
	return success
}

//export nervix_alloc
func nervixAlloc(size int32) int32 {
	if size < 0 {
		return errInvalidSize
	}
	if int(size) > len(fixedBuffer) {
		return errInvalidSize
	}
	buffer = fixedBuffer[:int(size)]
	return nervixBufferPtr()
}

//export nervix_init
func nervixInit(ptr int32, size int32) int32 {
	return guardedExport(func() int32 {
		data, code := readBufferRange(ptr, size)
		if code != success {
			return code
		}
		relays, code := outputRelaysFromInitMetadata(data)
		if code != success {
			return code
		}
		initMetadata = append(initMetadata[:0], data...)
		outputRelays = append(outputRelays[:0], relays...)
		initialized = true
		return success
	})
}

//export nervix_current_domain_time_nanos
func nervixCurrentDomainTimeNanos() int64 {
	lastDomainTimeNanos = hostDomainTimeNanos()
	return lastDomainTimeNanos
}

//export nervix_process_batch
func nervixProcessBatch(size int32) int32 {
	return guardedExport(func() int32 {
		if size < 0 {
			return errInvalidSize
		}
		if !initialized {
			return errNotInitialized
		}
		if int(size) > len(buffer) {
			return errOutOfBounds
		}
		processedBatches++
		lastDomainTimeNanos = hostDomainTimeNanos()
		lastTimeoutHandle = hostTimeoutAfterNanos(defaultTimeoutNanos)
		input, code := decodeEnvelope(buffer[:int(size)])
		if code != success {
			return code
		}
		if input.Kind != "input" {
			return errEnvelope
		}
		firstValue, hasFirstValue, code := firstInt32Value(input.ArrowIPCBatch)
		if code != success {
			return code
		}
		if hasFirstValue && firstValue == -300 {
			setGlobalError("guest error state for value -300")
			return errErrorState
		}
		if hasFirstValue && firstValue == -200 {
			setGlobalError("guest global error for value -200")
			return success
		}
		if hasFirstValue && firstValue == -100 {
			errorOutput, code := messageErrorOutput(input, "guest message error for value -100")
			if code != success {
				return code
			}
			if len(outputRelays) > 0 {
				errorOutput.OutputRelay = outputRelays[0]
			}
			encoded, code := encodeEnvelope(envelope{
				Kind:                   "output",
				GeneratedArrowIPCBatch: make([]byte, 0),
				Outputs:                []routedOutput{errorOutput},
			})
			if code != success {
				return code
			}
			pendingEmit = pendingEmit[:0]
			pendingEmit = append(pendingEmit, encoded)
			return success
		}
		pendingEmit = pendingEmit[:0]
		if len(pendingBatch) > 0 {
			if code := flushPending(); code != success {
				return code
			}
		}
		rowCount, code := arrowIPCRowCount(input.ArrowIPCBatch)
		if code != success {
			return code
		}
		pendingStartRow = processedRows
		processedRows += rowCount
		pendingBatch = append(pendingBatch[:0], buffer[:int(size)]...)
		if processedBatches%flushEveryBatches == 0 {
			return flushPending()
		}
		return success
	})
}

//export nervix_on_timeout
func nervixOnTimeout(handle int64) int32 {
	return guardedExport(func() int32 {
		lastTimeoutHandle = handle
		if len(pendingBatch) == 0 {
			return success
		}
		pendingEmit = pendingEmit[:0]
		return flushPending()
	})
}

//export nervix_read_emit
func nervixReadEmit() int32 {
	return guardedExport(func() int32 {
		if len(pendingEmit) == 0 {
			return 0
		}
		buffer = append(buffer[:0], pendingEmit[0]...)
		pendingEmit = pendingEmit[1:]
		return int32(len(buffer))
	})
}

//export nervix_dump_state
func nervixDumpState() int32 {
	return guardedStateExport(false, func() int32 {
		encoded, ok := encodeSnapshot(guestSnapshot{
			ProcessedBatches:    processedBatches,
			ProcessedRows:       processedRows,
			PendingStartRow:     pendingStartRow,
			LastDomainTimeNanos: lastDomainTimeNanos,
			LastTimeoutHandle:   lastTimeoutHandle,
			PendingBatch:        pendingBatch,
			InitMetadata:        initMetadata,
			SavedState:          savedState,
			ErrorState:          errorState,
		})
		if !ok {
			return errInvalidSize
		}

		buffer = append(buffer[:0], encoded...)
		return int32(len(buffer))
	})
}

//export nervix_load_state
func nervixLoadState(ptr int32, size int32) int32 {
	return guardedStateExport(false, func() int32 {
		data, code := readBufferRange(ptr, size)
		if code != success {
			return code
		}
		return loadStateBytes(data)
	})
}

//export nervix_reset_state
func nervixResetState() int32 {
	initMetadata = initMetadata[:0]
	pendingBatch = pendingBatch[:0]
	pendingEmit = pendingEmit[:0]
	clearGlobalError()
	savedState = savedState[:0]
	outputRelays = outputRelays[:0]
	pendingStartRow = 0
	initialized = false
	processedBatches = 0
	processedRows = 0
	lastDomainTimeNanos = 0
	lastTimeoutHandle = 0
	errorState = ""
	return success
}

func guardedExport(fn func() int32) (result int32) {
	return guardedStateExport(true, fn)
}

func guardedStateExport(checkErrorState bool, fn func() int32) (result int32) {
	if checkErrorState && errorState != "" {
		if len(globalError) == 0 {
			setGlobalError(errorState)
		}
		return errErrorState
	}
	return fn()
}

func setGlobalError(reason string) {
	globalError = append(globalError[:0], []byte(reason)...)
}

func clearGlobalError() {
	globalError = globalError[:0]
}

func flushPending() int32 {
	if len(pendingBatch) == 0 {
		return success
	}
	input, code := decodeEnvelope(pendingBatch)
	if code != success {
		return code
	}
	filtered, code := filterEnvelopeByGlobalRow(input, pendingStartRow)
	if code != success {
		return code
	}
	if len(outputRelays) == 0 {
		return errNotInitialized
	}
	outputs := make([]routedOutput, 0, len(outputRelays))
	for i := 0; i < len(outputRelays); i++ {
		output := filtered
		output.OutputRelay = outputRelays[i]
		if i > 0 {
			output.Acks.Acked = make([]ackTokenSet, 0)
			output.Acks.Nacked = make([]nackSet, 0)
			output.Acks.MessageErrors = make([]messageErrorSet, 0)
		}
		outputs = append(outputs, output)
	}
	encoded, code := encodeEnvelope(envelope{
		Kind:                   "output",
		GeneratedArrowIPCBatch: make([]byte, 0),
		Outputs:                outputs,
	})
	if code != success {
		return code
	}
	pendingEmit = append(pendingEmit, encoded)
	pendingBatch = pendingBatch[:0]
	pendingStartRow = processedRows
	return success
}

func encodeEnvelope(value envelope) ([]byte, int32) {
	return encodeFlatBufferEnvelope(value)
}

func (acks *ackSidecar) normalize() {
	if acks.Rows == nil {
		acks.Rows = make([]outputRow, 0)
	}
	if acks.Acked == nil {
		acks.Acked = make([]ackTokenSet, 0)
	}
	if acks.Nacked == nil {
		acks.Nacked = make([]nackSet, 0)
	}
	if acks.MessageErrors == nil {
		acks.MessageErrors = make([]messageErrorSet, 0)
	}
	for i := 0; i < len(acks.Rows); i++ {
		if acks.Rows[i].Tokens == nil {
			acks.Rows[i].Tokens = make([]uint64, 0)
		}
	}
	for i := 0; i < len(acks.Acked); i++ {
		if acks.Acked[i].Tokens == nil {
			acks.Acked[i].Tokens = make([]uint64, 0)
		}
	}
	for i := 0; i < len(acks.Nacked); i++ {
		if acks.Nacked[i].Tokens == nil {
			acks.Nacked[i].Tokens = make([]uint64, 0)
		}
	}
	for i := 0; i < len(acks.MessageErrors); i++ {
		if acks.MessageErrors[i].Tokens == nil {
			acks.MessageErrors[i].Tokens = make([]uint64, 0)
		}
	}
}

func decodeEnvelope(data []byte) (envelope, int32) {
	return decodeFlatBufferEnvelope(data)
}

func readBufferRange(ptr int32, size int32) ([]byte, int32) {
	if ptr < 0 || size < 0 {
		return nil, errInvalidSize
	}
	base := nervixBufferPtr()
	if base == 0 && size == 0 {
		return nil, success
	}
	start := int(ptr - base)
	if start < 0 {
		return nil, errOutOfBounds
	}
	end := start + int(size)
	if end < start || end > len(buffer) {
		return nil, errOutOfBounds
	}
	return buffer[start:end], success
}

func outputRelaysFromInitMetadata(data []byte) ([]string, int32) {
	return decodeBranchInitOutputRelays(data)
}

func loadStateBytes(data []byte) int32 {
	snapshot, ok := decodeSnapshot(data)
	if !ok {
		return errInvalidSize
	}

	processedBatches = snapshot.ProcessedBatches
	processedRows = snapshot.ProcessedRows
	pendingStartRow = snapshot.PendingStartRow
	lastDomainTimeNanos = snapshot.LastDomainTimeNanos
	lastTimeoutHandle = snapshot.LastTimeoutHandle
	pendingBatch = append(pendingBatch[:0], snapshot.PendingBatch...)
	if len(pendingBatch) > 0 {
		pending, code := decodeEnvelope(pendingBatch)
		if code != success || pending.Kind != "input" {
			return errInvalidSize
		}
	}
	initMetadata = append(initMetadata[:0], snapshot.InitMetadata...)
	relays, code := outputRelaysFromInitMetadata(initMetadata)
	if code != success {
		return code
	}
	outputRelays = append(outputRelays[:0], relays...)
	savedState = append(savedState[:0], snapshot.SavedState...)
	errorState = snapshot.ErrorState
	initialized = true
	return success
}

func arrowIPCRowCount(data []byte) (uint64, int32) {
	stream, ok := tinyipc.ParseStream(data)
	if !ok {
		return 0, errArrowIPC
	}
	return stream.RowCount(), success
}

func filterEnvelopeByGlobalRow(input envelope, startRow uint64) (routedOutput, int32) {
	if input.Kind != "input" {
		return routedOutput{}, errEnvelope
	}
	stream, ok := tinyipc.ParseStream(input.ArrowIPCBatch)
	if !ok {
		return routedOutput{}, errArrowIPC
	}
	outputAcks := ackSidecar{
		Rows:          make([]outputRow, 0, len(input.Acks.Rows)),
		Acked:         append([]ackTokenSet(nil), input.Acks.Acked...),
		Nacked:        append([]nackSet(nil), input.Acks.Nacked...),
		MessageErrors: append([]messageErrorSet(nil), input.Acks.MessageErrors...),
	}
	nextRow := startRow
	inputRow := 0
	for batchIndex := 0; batchIndex < len(stream.Batches); batchIndex++ {
		inputBatch := stream.Batches[batchIndex]
		for row := 0; row < len(inputBatch.Rows); row++ {
			if inputRow >= len(input.Acks.Rows) {
				return routedOutput{}, errEnvelope
			}
			nextRow++
			if nextRow%2 == 0 && inputBatch.Rows[row].Valid {
				outputAcks.Rows = append(outputAcks.Rows, input.Acks.Rows[inputRow])
			} else {
				outputAcks.Acked = append(outputAcks.Acked, ackTokenSet{
					Tokens: append([]uint64(nil), input.Acks.Rows[inputRow].Tokens...),
				})
			}
			inputRow++
		}
	}
	return routedOutput{
		Columns: []outputColumn{{Kind: "input", ColumnIndex: 0}},
		Acks:    outputAcks,
	}, success
}

func firstInt32Value(data []byte) (int32, bool, int32) {
	stream, ok := tinyipc.ParseStream(data)
	if !ok {
		return 0, false, errArrowIPC
	}
	value, found := stream.FirstInt32Value()
	return value, found, success
}

func messageErrorOutput(input envelope, reason string) (routedOutput, int32) {
	tokens := []uint64(nil)
	if len(input.Acks.Rows) > 0 {
		tokens = append(tokens, input.Acks.Rows[0].Tokens...)
	}
	return routedOutput{
		Columns: []outputColumn{{Kind: "input", ColumnIndex: 0}},
		Acks: ackSidecar{
			Rows:          make([]outputRow, 0),
			Acked:         append([]ackTokenSet(nil), input.Acks.Acked...),
			Nacked:        append([]nackSet(nil), input.Acks.Nacked...),
			MessageErrors: []messageErrorSet{{Tokens: tokens, Reason: reason}},
		},
	}, success
}
