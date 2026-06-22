//go:build tinygo

package main

import (
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
	maxInt                     = int(^uint(0) >> 1)
)

//go:wasmimport env nervix_domain_time_nanos
func hostDomainTimeNanos() int64

//go:wasmimport env nervix_timeout_after_nanos
func hostTimeoutAfterNanos(delayNanos int64) int64

var fixedBuffer [maxGuestBufferBytes]byte
var buffer []byte
var initMetadata []byte
var pendingBatch []byte
var pendingEmit []byte
var globalError []byte
var savedState []byte
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

type batchEnvelope struct {
	ArrowIPCBatch []byte
	Acks          ackSidecar
}

type ackSidecar struct {
	Rows          []rowAckSet       `cbor:"rows"`
	Acked         []rowAckSet       `cbor:"acked"`
	Nacked        []nackSet         `cbor:"nacked"`
	MessageErrors []messageErrorSet `cbor:"message_errors"`
}

type rowAckSet struct {
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
		initMetadata = append(initMetadata[:0], data...)
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
		envelope, code := decodeBatchEnvelope(buffer[:int(size)])
		if code != success {
			return code
		}
		firstValue, hasFirstValue, code := firstInt32Value(envelope.ArrowIPCBatch)
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
			errorEnvelope, code := messageErrorEnvelope(envelope, "guest message error for value -100")
			if code != success {
				return code
			}
			encoded, code := encodeBatchEnvelope(errorEnvelope)
			if code != success {
				return code
			}
			pendingEmit = append(pendingEmit[:0], encoded...)
			return success
		}
		rowCount, code := arrowIPCRowCount(envelope.ArrowIPCBatch)
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
		return flushPending()
	})
}

//export nervix_read_emit
func nervixReadEmit() int32 {
	return guardedExport(func() int32 {
		if len(pendingEmit) == 0 {
			return 0
		}
		buffer = append(buffer[:0], pendingEmit...)
		pendingEmit = pendingEmit[:0]
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
	envelope, code := decodeBatchEnvelope(pendingBatch)
	if code != success {
		return code
	}
	filtered, code := filterEnvelopeByGlobalRow(envelope, pendingStartRow)
	if code != success {
		return code
	}
	encoded, code := encodeBatchEnvelope(filtered)
	if code != success {
		return code
	}
	pendingEmit = append(pendingEmit[:0], encoded...)
	pendingBatch = pendingBatch[:0]
	pendingStartRow = processedRows
	return success
}

func encodeBatchEnvelope(envelope batchEnvelope) ([]byte, int32) {
	envelope.Acks.normalize()
	ackBytes := encodeAckSidecar(envelope.Acks)
	if uint64(len(envelope.ArrowIPCBatch)) > uint64(^uint32(0)) ||
		uint64(len(ackBytes)) > uint64(^uint32(0)) {
		return nil, errEnvelope
	}
	output := make([]byte, 0, 8+len(envelope.ArrowIPCBatch)+len(ackBytes))
	output = appendUint32(output, uint32(len(envelope.ArrowIPCBatch)))
	output = append(output, envelope.ArrowIPCBatch...)
	output = appendUint32(output, uint32(len(ackBytes)))
	output = append(output, ackBytes...)
	return output, success
}

func (acks *ackSidecar) normalize() {
	if acks.Rows == nil {
		acks.Rows = make([]rowAckSet, 0)
	}
	if acks.Acked == nil {
		acks.Acked = make([]rowAckSet, 0)
	}
	if acks.Nacked == nil {
		acks.Nacked = make([]nackSet, 0)
	}
	if acks.MessageErrors == nil {
		acks.MessageErrors = make([]messageErrorSet, 0)
	}
	for i := 0; i < len(acks.Rows); i++ {
		acks.Rows[i].normalize()
	}
	for i := 0; i < len(acks.Acked); i++ {
		acks.Acked[i].normalize()
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

func (acks *rowAckSet) normalize() {
	if acks.Tokens == nil {
		acks.Tokens = make([]uint64, 0)
	}
}

func decodeBatchEnvelope(data []byte) (batchEnvelope, int32) {
	if len(data) < 8 {
		return batchEnvelope{}, errEnvelope
	}
	arrowLen64 := uint64(readUint32(data))
	if arrowLen64 > uint64(maxInt) {
		return batchEnvelope{}, errEnvelope
	}
	arrowLen := int(arrowLen64)
	arrowStart := 4
	arrowEnd := arrowStart + arrowLen
	if arrowEnd < arrowStart || arrowEnd > len(data) || len(data)-arrowEnd < 4 {
		return batchEnvelope{}, errEnvelope
	}
	ackLen64 := uint64(readUint32(data[arrowEnd:]))
	if ackLen64 > uint64(maxInt) {
		return batchEnvelope{}, errEnvelope
	}
	ackLen := int(ackLen64)
	ackStart := arrowEnd + 4
	if ackStart > len(data) || ackLen > len(data)-ackStart {
		return batchEnvelope{}, errEnvelope
	}
	ackEnd := ackStart + ackLen
	if ackEnd != len(data) {
		return batchEnvelope{}, errEnvelope
	}
	acks, code := decodeAckSidecar(data[ackStart:ackEnd])
	if code != success {
		return batchEnvelope{}, errEnvelope
	}
	return batchEnvelope{
		ArrowIPCBatch: append([]byte(nil), data[arrowStart:arrowEnd]...),
		Acks:          acks,
	}, success
}

type cborReader struct {
	data []byte
	pos  int
}

func decodeAckSidecar(data []byte) (ackSidecar, int32) {
	reader := cborReader{data: data}
	length, code := reader.readMapLen()
	if code != success {
		return ackSidecar{}, code
	}
	acks := ackSidecar{}
	for i := uint64(0); i < length; i++ {
		key, code := reader.readText()
		if code != success {
			return ackSidecar{}, code
		}
		switch key {
		case "rows":
			rows, code := reader.readRowAckSets()
			if code != success {
				return ackSidecar{}, code
			}
			acks.Rows = rows
		case "acked":
			acked, code := reader.readRowAckSets()
			if code != success {
				return ackSidecar{}, code
			}
			acks.Acked = acked
		case "nacked":
			nacked, code := reader.readNackSets()
			if code != success {
				return ackSidecar{}, code
			}
			acks.Nacked = nacked
		case "message_errors":
			messageErrors, code := reader.readMessageErrorSets()
			if code != success {
				return ackSidecar{}, code
			}
			acks.MessageErrors = messageErrors
		default:
			return ackSidecar{}, errEnvelope
		}
	}
	if reader.pos != len(reader.data) {
		return ackSidecar{}, errEnvelope
	}
	acks.normalize()
	return acks, success
}

func (reader *cborReader) readRowAckSets() ([]rowAckSet, int32) {
	length, code := reader.readArrayLen()
	if code != success {
		return nil, code
	}
	if length > uint64(maxInt) {
		return nil, errEnvelope
	}
	rows := make([]rowAckSet, 0, int(length))
	for i := uint64(0); i < length; i++ {
		mapLen, code := reader.readMapLen()
		if code != success {
			return nil, code
		}
		row := rowAckSet{}
		for field := uint64(0); field < mapLen; field++ {
			key, code := reader.readText()
			if code != success {
				return nil, code
			}
			if key != "tokens" {
				return nil, errEnvelope
			}
			tokens, code := reader.readUintArray()
			if code != success {
				return nil, code
			}
			row.Tokens = tokens
		}
		row.normalize()
		rows = append(rows, row)
	}
	return rows, success
}

func (reader *cborReader) readNackSets() ([]nackSet, int32) {
	length, code := reader.readArrayLen()
	if code != success {
		return nil, code
	}
	if length > uint64(maxInt) {
		return nil, errEnvelope
	}
	nacks := make([]nackSet, 0, int(length))
	for i := uint64(0); i < length; i++ {
		mapLen, code := reader.readMapLen()
		if code != success {
			return nil, code
		}
		nack := nackSet{}
		for field := uint64(0); field < mapLen; field++ {
			key, code := reader.readText()
			if code != success {
				return nil, code
			}
			switch key {
			case "tokens":
				tokens, code := reader.readUintArray()
				if code != success {
					return nil, code
				}
				nack.Tokens = tokens
			case "reason":
				reason, code := reader.readText()
				if code != success {
					return nil, code
				}
				nack.Reason = reason
			default:
				return nil, errEnvelope
			}
		}
		if nack.Tokens == nil {
			nack.Tokens = make([]uint64, 0)
		}
		nacks = append(nacks, nack)
	}
	return nacks, success
}

func (reader *cborReader) readMessageErrorSets() ([]messageErrorSet, int32) {
	length, code := reader.readArrayLen()
	if code != success {
		return nil, code
	}
	if length > uint64(maxInt) {
		return nil, errEnvelope
	}
	errors := make([]messageErrorSet, 0, int(length))
	for i := uint64(0); i < length; i++ {
		mapLen, code := reader.readMapLen()
		if code != success {
			return nil, code
		}
		messageError := messageErrorSet{}
		for field := uint64(0); field < mapLen; field++ {
			key, code := reader.readText()
			if code != success {
				return nil, code
			}
			switch key {
			case "tokens":
				tokens, code := reader.readUintArray()
				if code != success {
					return nil, code
				}
				messageError.Tokens = tokens
			case "reason":
				reason, code := reader.readText()
				if code != success {
					return nil, code
				}
				messageError.Reason = reason
			default:
				return nil, errEnvelope
			}
		}
		if messageError.Tokens == nil {
			messageError.Tokens = make([]uint64, 0)
		}
		errors = append(errors, messageError)
	}
	return errors, success
}

func (reader *cborReader) readUintArray() ([]uint64, int32) {
	length, code := reader.readArrayLen()
	if code != success {
		return nil, code
	}
	if length > uint64(maxInt) {
		return nil, errEnvelope
	}
	values := make([]uint64, 0, int(length))
	for i := uint64(0); i < length; i++ {
		value, code := reader.readUint()
		if code != success {
			return nil, code
		}
		values = append(values, value)
	}
	return values, success
}

func (reader *cborReader) readMapLen() (uint64, int32) {
	major, value, code := reader.readHead()
	if code != success || major != 5 {
		return 0, errEnvelope
	}
	return value, success
}

func (reader *cborReader) readArrayLen() (uint64, int32) {
	major, value, code := reader.readHead()
	if code != success || major != 4 {
		return 0, errEnvelope
	}
	return value, success
}

func (reader *cborReader) readText() (string, int32) {
	major, length, code := reader.readHead()
	if code != success || major != 3 || length > uint64(maxInt) {
		return "", errEnvelope
	}
	size := int(length)
	if size > len(reader.data)-reader.pos {
		return "", errEnvelope
	}
	text := string(reader.data[reader.pos : reader.pos+size])
	reader.pos += size
	return text, success
}

func (reader *cborReader) readUint() (uint64, int32) {
	major, value, code := reader.readHead()
	if code != success || major != 0 {
		return 0, errEnvelope
	}
	return value, success
}

func (reader *cborReader) readHead() (byte, uint64, int32) {
	if reader.pos >= len(reader.data) {
		return 0, 0, errEnvelope
	}
	head := reader.data[reader.pos]
	reader.pos++
	major := head >> 5
	additional := head & 0x1f
	if additional < 24 {
		return major, uint64(additional), success
	}
	var size int
	switch additional {
	case 24:
		size = 1
	case 25:
		size = 2
	case 26:
		size = 4
	case 27:
		size = 8
	default:
		return 0, 0, errEnvelope
	}
	if size > len(reader.data)-reader.pos {
		return 0, 0, errEnvelope
	}
	var value uint64
	for i := 0; i < size; i++ {
		value = (value << 8) | uint64(reader.data[reader.pos+i])
	}
	reader.pos += size
	return major, value, success
}

func encodeAckSidecar(acks ackSidecar) []byte {
	output := make([]byte, 0, 64)
	output = appendCborMapLen(output, 4)
	output = appendCborText(output, "rows")
	output = appendRowAckSets(output, acks.Rows)
	output = appendCborText(output, "acked")
	output = appendRowAckSets(output, acks.Acked)
	output = appendCborText(output, "nacked")
	output = appendNackSets(output, acks.Nacked)
	output = appendCborText(output, "message_errors")
	output = appendMessageErrorSets(output, acks.MessageErrors)
	return output
}

func appendRowAckSets(output []byte, rows []rowAckSet) []byte {
	output = appendCborArrayLen(output, uint64(len(rows)))
	for i := 0; i < len(rows); i++ {
		output = appendCborMapLen(output, 1)
		output = appendCborText(output, "tokens")
		output = appendUintArray(output, rows[i].Tokens)
	}
	return output
}

func appendNackSets(output []byte, nacks []nackSet) []byte {
	output = appendCborArrayLen(output, uint64(len(nacks)))
	for i := 0; i < len(nacks); i++ {
		output = appendCborMapLen(output, 2)
		output = appendCborText(output, "tokens")
		output = appendUintArray(output, nacks[i].Tokens)
		output = appendCborText(output, "reason")
		output = appendCborText(output, nacks[i].Reason)
	}
	return output
}

func appendMessageErrorSets(output []byte, errors []messageErrorSet) []byte {
	output = appendCborArrayLen(output, uint64(len(errors)))
	for i := 0; i < len(errors); i++ {
		output = appendCborMapLen(output, 2)
		output = appendCborText(output, "tokens")
		output = appendUintArray(output, errors[i].Tokens)
		output = appendCborText(output, "reason")
		output = appendCborText(output, errors[i].Reason)
	}
	return output
}

func appendUintArray(output []byte, values []uint64) []byte {
	output = appendCborArrayLen(output, uint64(len(values)))
	for i := 0; i < len(values); i++ {
		output = appendCborUint(output, values[i])
	}
	return output
}

func appendCborMapLen(output []byte, length uint64) []byte {
	return appendCborHead(output, 5, length)
}

func appendCborArrayLen(output []byte, length uint64) []byte {
	return appendCborHead(output, 4, length)
}

func appendCborText(output []byte, text string) []byte {
	output = appendCborHead(output, 3, uint64(len(text)))
	return append(output, text...)
}

func appendCborUint(output []byte, value uint64) []byte {
	return appendCborHead(output, 0, value)
}

func appendCborHead(output []byte, major byte, value uint64) []byte {
	prefix := major << 5
	if value < 24 {
		return append(output, prefix|byte(value))
	}
	if value <= 0xff {
		return append(output, prefix|24, byte(value))
	}
	if value <= 0xffff {
		return append(output, prefix|25, byte(value>>8), byte(value))
	}
	if value <= 0xffffffff {
		return append(output, prefix|26, byte(value>>24), byte(value>>16), byte(value>>8), byte(value))
	}
	return append(output, prefix|27, byte(value>>56), byte(value>>48), byte(value>>40), byte(value>>32), byte(value>>24), byte(value>>16), byte(value>>8), byte(value))
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
	initMetadata = append(initMetadata[:0], snapshot.InitMetadata...)
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

func filterEnvelopeByGlobalRow(envelope batchEnvelope, startRow uint64) (batchEnvelope, int32) {
	stream, ok := tinyipc.ParseStream(envelope.ArrowIPCBatch)
	if !ok {
		return batchEnvelope{}, errArrowIPC
	}
	outputAcks := ackSidecar{
		Rows:          make([]rowAckSet, 0, len(envelope.Acks.Rows)),
		Acked:         append([]rowAckSet(nil), envelope.Acks.Acked...),
		Nacked:        append([]nackSet(nil), envelope.Acks.Nacked...),
		MessageErrors: append([]messageErrorSet(nil), envelope.Acks.MessageErrors...),
	}
	nextRow := startRow
	inputRow := 0
	outputBatches := make([]tinyipc.Batch, 0, len(stream.Batches))
	for batchIndex := 0; batchIndex < len(stream.Batches); batchIndex++ {
		inputBatch := stream.Batches[batchIndex]
		outputBatch := inputBatch
		outputBatch.Rows = make([]tinyipc.RowValue, 0, len(inputBatch.Rows)/2)
		for row := 0; row < len(inputBatch.Rows); row++ {
			nextRow++
			if nextRow%2 == 0 && inputBatch.Rows[row].Valid {
				outputBatch.Rows = append(outputBatch.Rows, inputBatch.Rows[row])
				if inputRow < len(envelope.Acks.Rows) {
					outputAcks.Rows = append(outputAcks.Rows, envelope.Acks.Rows[inputRow])
				} else {
					outputAcks.Rows = append(outputAcks.Rows, rowAckSet{})
				}
			} else if inputRow < len(envelope.Acks.Rows) {
				outputAcks.Acked = append(outputAcks.Acked, envelope.Acks.Rows[inputRow])
			}
			inputRow++
		}
		outputBatches = append(outputBatches, outputBatch)
	}
	output, ok := stream.EncodeBatches(outputBatches)
	if !ok {
		return batchEnvelope{}, errArrowIPC
	}
	return batchEnvelope{
		ArrowIPCBatch: output,
		Acks:          outputAcks,
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

func messageErrorEnvelope(envelope batchEnvelope, reason string) (batchEnvelope, int32) {
	output, ok := tinyipc.EmptyLike(envelope.ArrowIPCBatch)
	if !ok {
		return batchEnvelope{}, errArrowIPC
	}
	tokens := []uint64(nil)
	if len(envelope.Acks.Rows) > 0 {
		tokens = append(tokens, envelope.Acks.Rows[0].Tokens...)
	}
	return batchEnvelope{
		ArrowIPCBatch: output,
		Acks: ackSidecar{
			Rows:          make([]rowAckSet, 0),
			Acked:         append([]rowAckSet(nil), envelope.Acks.Acked...),
			Nacked:        append([]nackSet(nil), envelope.Acks.Nacked...),
			MessageErrors: []messageErrorSet{{Tokens: tokens, Reason: reason}},
		},
	}, success
}

func readUint32(data []byte) uint32 {
	return uint32(data[0]) |
		uint32(data[1])<<8 |
		uint32(data[2])<<16 |
		uint32(data[3])<<24
}

func appendUint32(out []byte, value uint32) []byte {
	for shift := uint(0); shift < 32; shift += 8 {
		out = append(out, byte(value>>shift))
	}
	return out
}
