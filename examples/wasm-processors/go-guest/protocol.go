//go:build tinygo

package main

import (
	"bytes"

	nervixwasm "github.com/apache/arrow-go/v18/arrow/nervix-wasm-processor-go-guest/nervixwasm"
	flatbuffers "github.com/google/flatbuffers/go"
)

func validFlatBufferMessage(data []byte) (*nervixwasm.Message, bool) {
	if len(data) < 12 {
		return nil, false
	}
	declared := uint32(data[0]) |
		uint32(data[1])<<8 |
		uint32(data[2])<<16 |
		uint32(data[3])<<24
	if uint64(declared) != uint64(len(data)-4) {
		return nil, false
	}
	if !nervixwasm.SizePrefixedMessageBufferHasIdentifier(data) {
		return nil, false
	}
	return nervixwasm.GetSizePrefixedRootAsMessage(data, 0), true
}

func finishMessage(
	builder *flatbuffers.Builder,
	payloadType nervixwasm.MessagePayload,
	payload flatbuffers.UOffsetT,
) []byte {
	nervixwasm.MessageStart(builder)
	nervixwasm.MessageAddPayload(builder, payload)
	nervixwasm.MessageAddPayloadType(builder, payloadType)
	message := nervixwasm.MessageEnd(builder)
	nervixwasm.FinishSizePrefixedMessageBuffer(builder, message)
	return builder.FinishedBytes()
}

func encodeFlatBufferEnvelope(value envelope) ([]byte, int32) {
	builder := flatbuffers.NewBuilder(1024)
	if value.Kind == "input" {
		arrow := builder.CreateByteVector(value.ArrowIPCBatch)
		acks := buildAckSidecar(builder, value.Acks)
		nervixwasm.InputEnvelopeStart(builder)
		nervixwasm.InputEnvelopeAddArrowIpcBatch(builder, arrow)
		nervixwasm.InputEnvelopeAddAcks(builder, acks)
		input := nervixwasm.InputEnvelopeEnd(builder)
		return finishMessage(builder, nervixwasm.MessagePayloadInputEnvelope, input), success
	}
	if value.Kind != "output" {
		return nil, errEnvelope
	}
	generated := builder.CreateByteVector(value.GeneratedArrowIPCBatch)
	outputs := make([]flatbuffers.UOffsetT, len(value.Outputs))
	for i := 0; i < len(value.Outputs); i++ {
		output, ok := buildRoutedOutput(builder, value.Outputs[i])
		if !ok {
			return nil, errEnvelope
		}
		outputs[i] = output
	}
	outputsVector := buildOffsetVector(builder, outputs)
	nervixwasm.OutputEnvelopeStart(builder)
	nervixwasm.OutputEnvelopeAddGeneratedArrowIpcBatch(builder, generated)
	nervixwasm.OutputEnvelopeAddOutputs(builder, outputsVector)
	output := nervixwasm.OutputEnvelopeEnd(builder)
	return finishMessage(builder, nervixwasm.MessagePayloadOutputEnvelope, output), success
}

func decodeFlatBufferEnvelope(data []byte) (envelope, int32) {
	message, ok := validFlatBufferMessage(data)
	if !ok {
		return envelope{}, errEnvelope
	}
	var payload flatbuffers.Table
	if !message.Payload(&payload) {
		return envelope{}, errEnvelope
	}
	if message.PayloadType() == nervixwasm.MessagePayloadInputEnvelope {
		input := nervixwasm.InputEnvelope{}
		input.Init(payload.Bytes, payload.Pos)
		acks := input.Acks(nil)
		if acks == nil || input.ArrowIpcBatchBytes() == nil {
			return envelope{}, errEnvelope
		}
		return envelope{
			Kind:          "input",
			ArrowIPCBatch: input.ArrowIpcBatchBytes(),
			Acks:          decodeAckSidecar(acks),
		}, success
	}
	if message.PayloadType() == nervixwasm.MessagePayloadOutputEnvelope {
		output := nervixwasm.OutputEnvelope{}
		output.Init(payload.Bytes, payload.Pos)
		generated := output.GeneratedArrowIpcBatchBytes()
		if generated == nil {
			return envelope{}, errEnvelope
		}
		outputs := make([]routedOutput, 0, output.OutputsLength())
		for i := 0; i < output.OutputsLength(); i++ {
			var wireOutput nervixwasm.RoutedOutput
			if !output.Outputs(&wireOutput, i) {
				return envelope{}, errEnvelope
			}
			decoded, ok := decodeRoutedOutput(&wireOutput)
			if !ok {
				return envelope{}, errEnvelope
			}
			outputs = append(outputs, decoded)
		}
		return envelope{
			Kind:                   "output",
			GeneratedArrowIPCBatch: generated,
			Outputs:                outputs,
		}, success
	}
	return envelope{}, errEnvelope
}

func buildOffsetVector(
	builder *flatbuffers.Builder,
	offsets []flatbuffers.UOffsetT,
) flatbuffers.UOffsetT {
	builder.StartVector(4, len(offsets), 4)
	for i := len(offsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(offsets[i])
	}
	return builder.EndVector(len(offsets))
}

func buildUint64Vector(builder *flatbuffers.Builder, values []uint64) flatbuffers.UOffsetT {
	builder.StartVector(8, len(values), 8)
	for i := len(values) - 1; i >= 0; i-- {
		builder.PrependUint64(values[i])
	}
	return builder.EndVector(len(values))
}

func buildAckSidecar(builder *flatbuffers.Builder, sidecar ackSidecar) flatbuffers.UOffsetT {
	rows := make([]flatbuffers.UOffsetT, len(sidecar.Rows))
	for i := 0; i < len(sidecar.Rows); i++ {
		tokens := buildUint64Vector(builder, sidecar.Rows[i].Tokens)
		nervixwasm.OutputRowStart(builder)
		nervixwasm.OutputRowAddTokens(builder, tokens)
		if sidecar.Rows[i].SourceToken != nil {
			nervixwasm.OutputRowAddSourceToken(builder, *sidecar.Rows[i].SourceToken)
		}
		rows[i] = nervixwasm.OutputRowEnd(builder)
	}
	rowsVector := buildOffsetVector(builder, rows)

	acked := make([]flatbuffers.UOffsetT, len(sidecar.Acked))
	for i := 0; i < len(sidecar.Acked); i++ {
		tokens := buildUint64Vector(builder, sidecar.Acked[i].Tokens)
		nervixwasm.AckTokenSetStart(builder)
		nervixwasm.AckTokenSetAddTokens(builder, tokens)
		acked[i] = nervixwasm.AckTokenSetEnd(builder)
	}
	ackedVector := buildOffsetVector(builder, acked)

	nacked := make([]flatbuffers.UOffsetT, len(sidecar.Nacked))
	for i := 0; i < len(sidecar.Nacked); i++ {
		tokens := buildUint64Vector(builder, sidecar.Nacked[i].Tokens)
		reason := builder.CreateString(sidecar.Nacked[i].Reason)
		nervixwasm.NackSetStart(builder)
		nervixwasm.NackSetAddTokens(builder, tokens)
		nervixwasm.NackSetAddReason(builder, reason)
		nacked[i] = nervixwasm.NackSetEnd(builder)
	}
	nackedVector := buildOffsetVector(builder, nacked)

	messageErrors := make([]flatbuffers.UOffsetT, len(sidecar.MessageErrors))
	for i := 0; i < len(sidecar.MessageErrors); i++ {
		tokens := buildUint64Vector(builder, sidecar.MessageErrors[i].Tokens)
		reason := builder.CreateString(sidecar.MessageErrors[i].Reason)
		nervixwasm.MessageErrorSetStart(builder)
		nervixwasm.MessageErrorSetAddTokens(builder, tokens)
		nervixwasm.MessageErrorSetAddReason(builder, reason)
		messageErrors[i] = nervixwasm.MessageErrorSetEnd(builder)
	}
	messageErrorsVector := buildOffsetVector(builder, messageErrors)

	nervixwasm.AckSidecarStart(builder)
	nervixwasm.AckSidecarAddRows(builder, rowsVector)
	nervixwasm.AckSidecarAddAcked(builder, ackedVector)
	nervixwasm.AckSidecarAddNacked(builder, nackedVector)
	nervixwasm.AckSidecarAddMessageErrors(builder, messageErrorsVector)
	return nervixwasm.AckSidecarEnd(builder)
}

func buildRoutedOutput(
	builder *flatbuffers.Builder,
	output routedOutput,
) (flatbuffers.UOffsetT, bool) {
	relay := builder.CreateString(output.OutputRelay)
	columns := make([]flatbuffers.UOffsetT, len(output.Columns))
	for i := 0; i < len(output.Columns); i++ {
		var source nervixwasm.ColumnSource
		if output.Columns[i].Kind == "input" {
			source = nervixwasm.ColumnSourceInput
		} else if output.Columns[i].Kind == "generated" {
			source = nervixwasm.ColumnSourceGenerated
		} else if output.Columns[i].Kind == "uninitialized" && output.Columns[i].ColumnIndex == 0 {
			source = nervixwasm.ColumnSourceUninitialized
		} else {
			return 0, false
		}
		nervixwasm.OutputColumnRefStart(builder)
		nervixwasm.OutputColumnRefAddSource(builder, source)
		nervixwasm.OutputColumnRefAddColumnIndex(builder, output.Columns[i].ColumnIndex)
		columns[i] = nervixwasm.OutputColumnRefEnd(builder)
	}
	columnsVector := buildOffsetVector(builder, columns)
	acks := buildAckSidecar(builder, output.Acks)
	nervixwasm.RoutedOutputStart(builder)
	nervixwasm.RoutedOutputAddOutputRelay(builder, relay)
	nervixwasm.RoutedOutputAddColumns(builder, columnsVector)
	nervixwasm.RoutedOutputAddAcks(builder, acks)
	return nervixwasm.RoutedOutputEnd(builder), true
}

func decodeAckSidecar(sidecar *nervixwasm.AckSidecar) ackSidecar {
	decoded := ackSidecar{
		Rows:          make([]outputRow, 0, sidecar.RowsLength()),
		Acked:         make([]ackTokenSet, 0, sidecar.AckedLength()),
		Nacked:        make([]nackSet, 0, sidecar.NackedLength()),
		MessageErrors: make([]messageErrorSet, 0, sidecar.MessageErrorsLength()),
	}
	for i := 0; i < sidecar.RowsLength(); i++ {
		var row nervixwasm.OutputRow
		if sidecar.Rows(&row, i) {
			tokens := make([]uint64, row.TokensLength())
			for j := 0; j < row.TokensLength(); j++ {
				tokens[j] = row.Tokens(j)
			}
			var sourceToken *uint64
			if source := row.SourceToken(); source != nil {
				value := *source
				sourceToken = &value
			}
			decoded.Rows = append(decoded.Rows, outputRow{Tokens: tokens, SourceToken: sourceToken})
		}
	}
	for i := 0; i < sidecar.AckedLength(); i++ {
		var set nervixwasm.AckTokenSet
		if sidecar.Acked(&set, i) {
			decoded.Acked = append(decoded.Acked, ackTokenSet{Tokens: decodeAckTokens(set.TokensLength(), set.Tokens)})
		}
	}
	for i := 0; i < sidecar.NackedLength(); i++ {
		var set nervixwasm.NackSet
		if sidecar.Nacked(&set, i) {
			decoded.Nacked = append(decoded.Nacked, nackSet{
				Tokens: decodeAckTokens(set.TokensLength(), set.Tokens),
				Reason: string(set.Reason()),
			})
		}
	}
	for i := 0; i < sidecar.MessageErrorsLength(); i++ {
		var set nervixwasm.MessageErrorSet
		if sidecar.MessageErrors(&set, i) {
			decoded.MessageErrors = append(decoded.MessageErrors, messageErrorSet{
				Tokens: decodeAckTokens(set.TokensLength(), set.Tokens),
				Reason: string(set.Reason()),
			})
		}
	}
	return decoded
}

func decodeAckTokens(length int, token func(int) uint64) []uint64 {
	tokens := make([]uint64, length)
	for i := 0; i < length; i++ {
		tokens[i] = token(i)
	}
	return tokens
}

func decodeRoutedOutput(output *nervixwasm.RoutedOutput) (routedOutput, bool) {
	acks := output.Acks(nil)
	if output.OutputRelay() == nil || acks == nil {
		return routedOutput{}, false
	}
	columns := make([]outputColumn, 0, output.ColumnsLength())
	for i := 0; i < output.ColumnsLength(); i++ {
		var column nervixwasm.OutputColumnRef
		if !output.Columns(&column, i) {
			return routedOutput{}, false
		}
		kind := ""
		if column.Source() == nervixwasm.ColumnSourceInput {
			kind = "input"
		} else if column.Source() == nervixwasm.ColumnSourceGenerated {
			kind = "generated"
		} else if column.Source() == nervixwasm.ColumnSourceUninitialized && column.ColumnIndex() == 0 {
			kind = "uninitialized"
		} else {
			return routedOutput{}, false
		}
		columns = append(columns, outputColumn{Kind: kind, ColumnIndex: column.ColumnIndex()})
	}
	return routedOutput{
		OutputRelay: string(output.OutputRelay()),
		Columns:     columns,
		Acks:        decodeAckSidecar(acks),
	}, true
}

func decodeBranchInitOutputRelays(data []byte) ([]string, int32) {
	message, ok := validFlatBufferMessage(data)
	if !ok || message.PayloadType() != nervixwasm.MessagePayloadBranchInit {
		return nil, errInvalidSize
	}
	var payload flatbuffers.Table
	if !message.Payload(&payload) {
		return nil, errInvalidSize
	}
	init := nervixwasm.BranchInit{}
	init.Init(payload.Bytes, payload.Pos)
	relays := make([]string, 0, init.OutputSchemasLength())
	for i := 0; i < init.OutputSchemasLength(); i++ {
		var schema nervixwasm.ProcessorSchema
		if !init.OutputSchemas(&schema, i) || schema.Name() == nil {
			return nil, errInvalidSize
		}
		relays = append(relays, string(schema.Name()))
	}
	return relays, success
}

func encodeSnapshot(snapshot guestSnapshot) []byte {
	builder := flatbuffers.NewBuilder(1024)
	initMetadata := builder.CreateByteVector(snapshot.InitMetadata)
	applicationState := builder.CreateByteVector(snapshot.ApplicationState)
	nervixwasm.GuestSnapshotStart(builder)
	nervixwasm.GuestSnapshotAddInitMetadata(builder, initMetadata)
	nervixwasm.GuestSnapshotAddApplicationState(builder, applicationState)
	payload := nervixwasm.GuestSnapshotEnd(builder)
	return finishMessage(builder, nervixwasm.MessagePayloadGuestSnapshot, payload)
}

func decodeSnapshot(data []byte) (guestSnapshot, bool) {
	message, ok := validFlatBufferMessage(data)
	if !ok || message.PayloadType() != nervixwasm.MessagePayloadGuestSnapshot {
		return guestSnapshot{}, false
	}
	var payload flatbuffers.Table
	if !message.Payload(&payload) {
		return guestSnapshot{}, false
	}
	snapshot := nervixwasm.GuestSnapshot{}
	snapshot.Init(payload.Bytes, payload.Pos)
	if snapshot.InitMetadataBytes() == nil || snapshot.ApplicationStateBytes() == nil {
		return guestSnapshot{}, false
	}
	return guestSnapshot{
		InitMetadata:     append([]byte(nil), snapshot.InitMetadataBytes()...),
		ApplicationState: append([]byte(nil), snapshot.ApplicationStateBytes()...),
	}, true
}

func decodeBranchInit(data []byte) (*nervixwasm.BranchInit, bool) {
	message, ok := validFlatBufferMessage(data)
	if !ok || message.PayloadType() != nervixwasm.MessagePayloadBranchInit {
		return nil, false
	}
	var payload flatbuffers.Table
	if !message.Payload(&payload) {
		return nil, false
	}
	init := nervixwasm.BranchInit{}
	init.Init(payload.Bytes, payload.Pos)
	return &init, true
}

// sameBranchConfiguration reports whether saved init metadata describes the same branch
// configuration as the init metadata this instance was initialized with. It compares the decoded
// fields rather than the bytes, so two encodings of one configuration match. The second result is
// false when the saved init metadata cannot be decoded.
func sameBranchConfiguration(saved []byte, current []byte) (bool, bool) {
	savedInit, ok := decodeBranchInit(saved)
	if !ok {
		return false, false
	}
	currentInit, ok := decodeBranchInit(current)
	if !ok {
		return false, false
	}
	if !bytes.Equal(savedInit.DomainName(), currentInit.DomainName()) {
		return false, true
	}
	if !bytes.Equal(savedInit.DomainType(), currentInit.DomainType()) {
		return false, true
	}
	savedKey := savedInit.BranchKeyBytes()
	currentKey := currentInit.BranchKeyBytes()
	if (savedKey == nil) != (currentKey == nil) || !bytes.Equal(savedKey, currentKey) {
		return false, true
	}
	if !sameProcessorSchema(savedInit.InputSchema(nil), currentInit.InputSchema(nil)) {
		return false, true
	}
	if savedInit.OutputSchemasLength() != currentInit.OutputSchemasLength() {
		return false, true
	}
	for i := 0; i < savedInit.OutputSchemasLength(); i++ {
		var savedSchema nervixwasm.ProcessorSchema
		var currentSchema nervixwasm.ProcessorSchema
		if !savedInit.OutputSchemas(&savedSchema, i) || !currentInit.OutputSchemas(&currentSchema, i) {
			return false, true
		}
		if !sameProcessorSchema(&savedSchema, &currentSchema) {
			return false, true
		}
	}
	return true, true
}

func sameProcessorSchema(saved *nervixwasm.ProcessorSchema, current *nervixwasm.ProcessorSchema) bool {
	if saved == nil || current == nil {
		return saved == current
	}
	if !bytes.Equal(saved.Name(), current.Name()) || saved.FieldsLength() != current.FieldsLength() {
		return false
	}
	for i := 0; i < saved.FieldsLength(); i++ {
		var savedField nervixwasm.ProcessorField
		var currentField nervixwasm.ProcessorField
		if !saved.Fields(&savedField, i) || !current.Fields(&currentField, i) {
			return false
		}
		if !bytes.Equal(savedField.Name(), currentField.Name()) {
			return false
		}
		if savedField.Optional() != currentField.Optional() {
			return false
		}
		if !sameProcessorType(savedField.Type(nil), currentField.Type(nil)) {
			return false
		}
	}
	return true
}

func sameProcessorType(saved *nervixwasm.ProcessorType, current *nervixwasm.ProcessorType) bool {
	if saved == nil || current == nil {
		return saved == current
	}
	if saved.Kind() != current.Kind() || saved.ArrayLen() != current.ArrayLen() {
		return false
	}
	return sameProcessorType(saved.Element(nil), current.Element(nil))
}
