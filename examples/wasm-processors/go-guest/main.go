//go:build tinygo

package main

import (
	"errors"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/nervix-wasm-processor-go-guest/tinyipc"
	"github.com/fxamacker/cbor/v2"
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
	ProcessedBatches    uint64 `cbor:"processed_batches"`
	ProcessedRows       uint64 `cbor:"processed_rows"`
	PendingStartRow     uint64 `cbor:"pending_start_row"`
	LastDomainTimeNanos int64  `cbor:"last_domain_time_nanos"`
	LastTimeoutHandle   int64  `cbor:"last_timeout_handle"`
	PendingBatch        []byte `cbor:"pending_batch"`
	InitMetadata        []byte `cbor:"init_metadata"`
	SavedState          []byte `cbor:"saved_state"`
	ErrorState          string `cbor:"error_state"`
}

type envelope struct {
	Kind                   string         `cbor:"kind"`
	ArrowIPCBatch          []byte         `cbor:"arrow_ipc_batch,omitempty"`
	Acks                   ackSidecar     `cbor:"acks,omitempty"`
	GeneratedArrowIPCBatch []byte         `cbor:"generated_arrow_ipc_batch,omitempty"`
	Outputs                []routedOutput `cbor:"outputs,omitempty"`
}

type routedOutput struct {
	OutputRelay string         `cbor:"output_relay"`
	Columns     []outputColumn `cbor:"columns"`
	Acks        ackSidecar     `cbor:"acks"`
}

type outputColumn struct {
	Kind        string `cbor:"kind"`
	ColumnIndex uint32 `cbor:"column_index"`
}

func (column outputColumn) MarshalCBOR() ([]byte, error) {
	if column.Kind == "input" || column.Kind == "generated" {
		return cbor.Marshal(struct {
			Kind        string `cbor:"kind"`
			ColumnIndex uint32 `cbor:"column_index"`
		}{Kind: column.Kind, ColumnIndex: column.ColumnIndex})
	}
	return nil, errors.New("invalid output column kind")
}

type inputEnvelopeWire struct {
	Kind          string     `cbor:"kind"`
	ArrowIPCBatch []byte     `cbor:"arrow_ipc_batch"`
	Acks          ackSidecar `cbor:"acks"`
}

type outputEnvelopeWire struct {
	Kind                   string         `cbor:"kind"`
	GeneratedArrowIPCBatch []byte         `cbor:"generated_arrow_ipc_batch"`
	Outputs                []routedOutput `cbor:"outputs"`
}

type branchInitMetadata struct {
	OutputSchemas []processorSchema `cbor:"output_schemas"`
}

type processorSchema struct {
	Name string `cbor:"name"`
}

type ackSidecar struct {
	Rows          []outputRow       `cbor:"rows"`
	Acked         []ackTokenSet     `cbor:"acked"`
	Nacked        []nackSet         `cbor:"nacked"`
	MessageErrors []messageErrorSet `cbor:"message_errors"`
}

type outputRow struct {
	Tokens      []uint64 `cbor:"tokens"`
	SourceToken *uint64  `cbor:"source_token"`
}

type ackTokenSet struct {
	Tokens []uint64 `cbor:"tokens"`
}

type nackSet struct {
	Tokens []uint64 `cbor:"tokens"`
	Reason string   `cbor:"reason"`
}

type messageErrorSet struct {
	Tokens []uint64 `cbor:"tokens"`
	Reason string   `cbor:"reason"`
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
		encoded, err := cbor.Marshal(guestSnapshot{
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
		if err != nil {
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
	if value.Kind == "input" {
		value.Acks.normalize()
		encoded, err := cbor.Marshal(inputEnvelopeWire{
			Kind:          value.Kind,
			ArrowIPCBatch: value.ArrowIPCBatch,
			Acks:          value.Acks,
		})
		if err != nil {
			return nil, errEnvelope
		}
		return encoded, success
	}
	if value.Kind == "output" {
		if value.GeneratedArrowIPCBatch == nil {
			value.GeneratedArrowIPCBatch = make([]byte, 0)
		}
		if value.Outputs == nil {
			value.Outputs = make([]routedOutput, 0)
		}
		for i := 0; i < len(value.Outputs); i++ {
			if value.Outputs[i].Columns == nil {
				value.Outputs[i].Columns = make([]outputColumn, 0)
			}
			value.Outputs[i].Acks.normalize()
		}
		encoded, err := cbor.Marshal(outputEnvelopeWire{
			Kind:                   value.Kind,
			GeneratedArrowIPCBatch: value.GeneratedArrowIPCBatch,
			Outputs:                value.Outputs,
		})
		if err != nil {
			return nil, errEnvelope
		}
		return encoded, success
	}
	return nil, errEnvelope
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
	mode, err := cbor.DecOptions{
		IndefLength:       cbor.IndefLengthForbidden,
		ExtraReturnErrors: cbor.ExtraDecErrorUnknownField,
	}.DecMode()
	if err != nil {
		return envelope{}, errEnvelope
	}
	var raw map[string]cbor.RawMessage
	if err := mode.Unmarshal(data, &raw); err != nil {
		return envelope{}, errEnvelope
	}
	kindBytes, ok := raw["kind"]
	if !ok {
		return envelope{}, errEnvelope
	}
	var kind string
	if err := mode.Unmarshal(kindBytes, &kind); err != nil {
		return envelope{}, errEnvelope
	}
	if kind == "input" {
		if !hasExactFields(raw, "kind", "arrow_ipc_batch", "acks") {
			return envelope{}, errEnvelope
		}
		if !isCBORByteString(raw["arrow_ipc_batch"]) {
			return envelope{}, errEnvelope
		}
		if !validAckSidecar(mode, raw["acks"]) {
			return envelope{}, errEnvelope
		}
		var wire inputEnvelopeWire
		if err := mode.Unmarshal(data, &wire); err != nil {
			return envelope{}, errEnvelope
		}
		return envelope{
			Kind:          wire.Kind,
			ArrowIPCBatch: wire.ArrowIPCBatch,
			Acks:          wire.Acks,
		}, success
	}
	if kind == "output" {
		if !hasExactFields(raw, "kind", "generated_arrow_ipc_batch", "outputs") {
			return envelope{}, errEnvelope
		}
		if !isCBORByteString(raw["generated_arrow_ipc_batch"]) {
			return envelope{}, errEnvelope
		}
		var rawOutputs []cbor.RawMessage
		if err := mode.Unmarshal(raw["outputs"], &rawOutputs); err != nil {
			return envelope{}, errEnvelope
		}
		for i := 0; i < len(rawOutputs); i++ {
			if !validRoutedOutput(mode, rawOutputs[i]) {
				return envelope{}, errEnvelope
			}
		}
		var wire outputEnvelopeWire
		if err := mode.Unmarshal(data, &wire); err != nil {
			return envelope{}, errEnvelope
		}
		return envelope{
			Kind:                   wire.Kind,
			GeneratedArrowIPCBatch: wire.GeneratedArrowIPCBatch,
			Outputs:                wire.Outputs,
		}, success
	}
	return envelope{}, errEnvelope
}

func validRoutedOutput(mode cbor.DecMode, data []byte) bool {
	var raw map[string]cbor.RawMessage
	if err := mode.Unmarshal(data, &raw); err != nil {
		return false
	}
	if !hasExactFields(raw, "output_relay", "columns", "acks") || !validAckSidecar(mode, raw["acks"]) {
		return false
	}
	var rawColumns []cbor.RawMessage
	if err := mode.Unmarshal(raw["columns"], &rawColumns); err != nil {
		return false
	}
	for i := 0; i < len(rawColumns); i++ {
		if !validOutputColumn(mode, rawColumns[i]) {
			return false
		}
	}
	return true
}

func validAckSidecar(mode cbor.DecMode, data []byte) bool {
	var raw map[string]cbor.RawMessage
	if err := mode.Unmarshal(data, &raw); err != nil {
		return false
	}
	if !hasExactFields(raw, "rows", "acked", "nacked", "message_errors") {
		return false
	}
	return validObjectArray(mode, raw["rows"], "tokens", "source_token") &&
		validObjectArray(mode, raw["acked"], "tokens") &&
		validObjectArray(mode, raw["nacked"], "tokens", "reason") &&
		validObjectArray(mode, raw["message_errors"], "tokens", "reason")
}

func validObjectArray(mode cbor.DecMode, data []byte, expected ...string) bool {
	var entries []cbor.RawMessage
	if err := mode.Unmarshal(data, &entries); err != nil {
		return false
	}
	for i := 0; i < len(entries); i++ {
		var entry map[string]cbor.RawMessage
		if err := mode.Unmarshal(entries[i], &entry); err != nil || !hasExactFields(entry, expected...) {
			return false
		}
	}
	return true
}

func validOutputColumn(mode cbor.DecMode, data []byte) bool {
	var raw map[string]cbor.RawMessage
	if err := mode.Unmarshal(data, &raw); err != nil {
		return false
	}
	kindBytes, ok := raw["kind"]
	if !ok {
		return false
	}
	var kind string
	if err := mode.Unmarshal(kindBytes, &kind); err != nil {
		return false
	}
	if kind == "input" || kind == "generated" {
		return hasExactFields(raw, "kind", "column_index")
	}
	return false
}

func isCBORByteString(data []byte) bool {
	return len(data) > 0 && data[0]>>5 == 2 && data[0]&31 != 31
}

func hasExactFields(fields map[string]cbor.RawMessage, expected ...string) bool {
	if len(fields) != len(expected) {
		return false
	}
	for i := 0; i < len(expected); i++ {
		if _, ok := fields[expected[i]]; !ok {
			return false
		}
	}
	return true
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
	var metadata branchInitMetadata
	if err := cbor.Unmarshal(data, &metadata); err != nil {
		return nil, errInvalidSize
	}
	relays := make([]string, 0, len(metadata.OutputSchemas))
	for i := 0; i < len(metadata.OutputSchemas); i++ {
		relays = append(relays, metadata.OutputSchemas[i].Name)
	}
	return relays, success
}

func loadStateBytes(data []byte) int32 {
	var snapshot guestSnapshot
	if err := cbor.Unmarshal(data, &snapshot); err != nil {
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
