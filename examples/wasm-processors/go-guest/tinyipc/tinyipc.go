//go:build tinygo

package tinyipc

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/internal/flatbuf"
)

const (
	continuationToken uint32 = 0xffffffff
	maxInt                   = int(^uint(0) >> 1)
)

var continuationMarker = []byte{0xff, 0xff, 0xff, 0xff}
var lastError int

func LastError() int {
	return lastError
}

type messageMetadata struct {
	headerType flatbuf.MessageHeader
	headerPos  int
	version    flatbuf.MetadataVersion
	bodyLen    int64
}

type Stream struct {
	SchemaMetadata []byte
	Version        flatbuf.MetadataVersion
	ValueType      arrow.DataType
	ValueWidth     int
	Batches        []Batch
}

type Batch struct {
	Metadata             []byte
	Rows                 []RowValue
	BodyLengthOffset     int
	RecordLengthOffset   int
	NodeLengthOffset     int
	NodeNullCountOffset  int
	NullBufferOffset     int
	NullBufferLenOffset  int
	ValueBufferOffset    int
	ValueBufferLenOffset int
}

type RowValue struct {
	Data  []byte
	Valid bool
}

// ParseStream handles the narrow Arrow IPC stream shape used by this TinyGo
// guest. The upstream arrow/ipc package already supports in-memory message
// readers, but importing that package also compiles stream/file compression and
// goroutine paths that TinyGo rejects with scheduler=none. This local packet
// reader keeps the guest scheduler-free while still using Arrow's generated
// FlatBuffer metadata types.
func ParseStream(data []byte) (Stream, bool) {
	lastError = 0
	cursor := data
	stream := Stream{Version: flatbuf.MetadataVersionV5}
	for {
		if len(cursor) < 8 {
			lastError = 1
			return Stream{}, false
		}
		prefix := readUint32(cursor)
		cursor = cursor[4:]
		var metadataLen uint32
		if prefix == continuationToken {
			metadataLen = readUint32(cursor)
			cursor = cursor[4:]
		} else {
			metadataLen = prefix
		}
		if metadataLen == 0 {
			return stream, len(cursor) == 0
		}
		if uint64(metadataLen) > uint64(maxInt) {
			lastError = 2
			return Stream{}, false
		}
		metadataSize := int(metadataLen)
		paddedMetadataSize := paddedLen(metadataSize, 8)
		if paddedMetadataSize < metadataSize || paddedMetadataSize > len(cursor) {
			lastError = 3
			return Stream{}, false
		}
		metadata := append([]byte(nil), cursor[:metadataSize]...)
		cursor = cursor[paddedMetadataSize:]
		message, ok := parseMessageMetadata(metadata)
		if !ok {
			if lastError == 0 {
				lastError = 4
			}
			return Stream{}, false
		}
		bodyLen := message.bodyLen
		if bodyLen < 0 || uint64(bodyLen) > uint64(maxInt) {
			lastError = 5
			return Stream{}, false
		}
		bodySize := int(bodyLen)
		paddedBodySize := paddedLen(bodySize, 8)
		if paddedBodySize < bodySize || paddedBodySize > len(cursor) {
			lastError = 6
			return Stream{}, false
		}
		body := cursor[:bodySize]
		cursor = cursor[paddedBodySize:]

		switch message.headerType {
		case flatbuf.MessageHeaderSchema:
			if bodySize != 0 {
				lastError = 7
				return Stream{}, false
			}
			valueType, ok := schemaFirstIntegerType(metadata, message.headerPos)
			if !ok {
				lastError = 12
				return Stream{}, false
			}
			fixedWidth, ok := valueType.(arrow.FixedWidthDataType)
			if !ok {
				lastError = 14
				return Stream{}, false
			}
			stream.SchemaMetadata = metadata
			stream.Version = message.version
			stream.ValueType = valueType
			stream.ValueWidth = fixedWidth.Bytes()
		case flatbuf.MessageHeaderRecordBatch:
			if stream.ValueWidth <= 0 {
				lastError = 13
				return Stream{}, false
			}
			rows, ok := recordBatchRows(metadata, message.headerPos, body, stream.ValueWidth)
			if !ok {
				if lastError == 0 {
					lastError = 8
				}
				return Stream{}, false
			}
			batch, ok := batchTemplate(metadata, message.headerPos)
			if !ok {
				lastError = 11
				return Stream{}, false
			}
			batch.Rows = rows
			stream.Batches = append(stream.Batches, batch)
		case flatbuf.MessageHeaderDictionaryBatch:
			lastError = 9
			return Stream{}, false
		default:
			lastError = 10
			return Stream{}, false
		}
	}
}

func (stream Stream) RowCount() uint64 {
	var rows uint64
	for i := 0; i < len(stream.Batches); i++ {
		rows += uint64(len(stream.Batches[i].Rows))
	}
	return rows
}

func (stream Stream) FirstInt32Value() (int32, bool) {
	if stream.ValueWidth != 4 {
		return 0, false
	}
	for i := 0; i < len(stream.Batches); i++ {
		if len(stream.Batches[i].Rows) == 0 {
			continue
		}
		row := stream.Batches[i].Rows[0]
		if row.Valid {
			return int32(readUint32(row.Data)), true
		}
	}
	return 0, false
}

func (stream Stream) EncodeBatches(batches []Batch) ([]byte, bool) {
	if len(stream.SchemaMetadata) == 0 {
		return nil, false
	}
	output := make([]byte, 0, len(stream.SchemaMetadata)+8)
	output = appendArrowMessage(output, stream.SchemaMetadata, nil)
	version := stream.Version
	if version == 0 {
		version = flatbuf.MetadataVersionV5
	}
	for i := 0; i < len(batches); i++ {
		values := make([][]byte, 0, len(batches[i].Rows))
		for row := 0; row < len(batches[i].Rows); row++ {
			if batches[i].Rows[row].Valid {
				values = append(values, batches[i].Rows[row].Data)
			}
		}
		metadata, body := buildRecordBatchMessage(batches[i], values)
		if len(metadata) == 0 {
			return nil, false
		}
		output = appendArrowMessage(output, metadata, body)
	}
	output = append(output, continuationMarker...)
	output = append(output, 0, 0, 0, 0)
	return output, true
}

func EmptyLike(data []byte) ([]byte, bool) {
	stream, ok := ParseStream(data)
	if !ok {
		return nil, false
	}
	var template Batch
	if len(stream.Batches) > 0 {
		template = stream.Batches[0]
	}
	template.Rows = nil
	return stream.EncodeBatches([]Batch{template})
}

func parseMessageMetadata(data []byte) (messageMetadata, bool) {
	root, ok := flatbufferRoot(data)
	if !ok || !tableInBounds(data, root) {
		lastError = 41
		return messageMetadata{}, false
	}
	headerTypeOffset, ok := tableFieldOffset(data, root, 6)
	if !ok || headerTypeOffset >= len(data) {
		lastError = 42
		return messageMetadata{}, false
	}
	headerOffset, ok := tableFieldOffset(data, root, 8)
	if !ok || headerOffset+4 > len(data) {
		lastError = 43
		return messageMetadata{}, false
	}
	headerPos := headerOffset + int(readUint32(data[headerOffset:]))
	if !tableInBounds(data, headerPos) {
		lastError = 44
		return messageMetadata{}, false
	}
	version := flatbuf.MetadataVersionV5
	if versionOffset, ok := tableFieldOffset(data, root, 4); ok {
		if versionOffset+2 > len(data) {
			lastError = 45
			return messageMetadata{}, false
		}
		version = flatbuf.MetadataVersion(readInt16(data[versionOffset:]))
	}
	var bodyLen int64
	if bodyLenOffset, ok := tableFieldOffset(data, root, 10); ok {
		if bodyLenOffset+8 > len(data) {
			lastError = 46
			return messageMetadata{}, false
		}
		bodyLen = readInt64(data[bodyLenOffset:])
	}
	return messageMetadata{
		headerType: flatbuf.MessageHeader(data[headerTypeOffset]),
		headerPos:  headerPos,
		version:    version,
		bodyLen:    bodyLen,
	}, true
}

func schemaFirstIntegerType(data []byte, schemaPos int) (arrow.DataType, bool) {
	if !tableInBounds(data, schemaPos) {
		return nil, false
	}
	fieldsVector, ok := vectorStart(data, schemaPos, 6)
	if !ok || fieldsVector+4 > len(data) {
		return nil, false
	}
	if readUint32(data[fieldsVector:]) < 1 {
		return nil, false
	}
	fieldElement := fieldsVector + 4
	if fieldElement+4 > len(data) {
		return nil, false
	}
	fieldPos := fieldElement + int(readUint32(data[fieldElement:]))
	if !tableInBounds(data, fieldPos) {
		return nil, false
	}
	typeTypeOffset, ok := tableFieldOffset(data, fieldPos, 8)
	if !ok || typeTypeOffset >= len(data) || flatbuf.Type(data[typeTypeOffset]) != flatbuf.TypeInt {
		return nil, false
	}
	if _, ok := tableFieldOffset(data, fieldPos, 12); ok {
		return nil, false
	}
	if childrenVector, ok := vectorStart(data, fieldPos, 14); ok {
		if childrenVector+4 > len(data) || readUint32(data[childrenVector:]) != 0 {
			return nil, false
		}
	}
	typeOffset, ok := tableFieldOffset(data, fieldPos, 10)
	if !ok || typeOffset+4 > len(data) {
		return nil, false
	}
	intPos := typeOffset + int(readUint32(data[typeOffset:]))
	if !tableInBounds(data, intPos) {
		return nil, false
	}
	bitWidthOffset, ok := tableFieldOffset(data, intPos, 4)
	if !ok || bitWidthOffset+4 > len(data) {
		return nil, false
	}
	bitWidth := readInt32(data[bitWidthOffset:])
	signedOffset, ok := tableFieldOffset(data, intPos, 6)
	if !ok || signedOffset >= len(data) {
		return nil, false
	}
	signed := data[signedOffset] != 0
	switch bitWidth {
	case 8:
		if signed {
			return arrow.PrimitiveTypes.Int8, true
		}
		return arrow.PrimitiveTypes.Uint8, true
	case 16:
		if signed {
			return arrow.PrimitiveTypes.Int16, true
		}
		return arrow.PrimitiveTypes.Uint16, true
	case 32:
		if signed {
			return arrow.PrimitiveTypes.Int32, true
		}
		return arrow.PrimitiveTypes.Uint32, true
	case 64:
		if signed {
			return arrow.PrimitiveTypes.Int64, true
		}
		return arrow.PrimitiveTypes.Uint64, true
	default:
		return nil, false
	}
}

func recordBatchRows(data []byte, recordPos int, body []byte, valueWidth int) ([]RowValue, bool) {
	if !tableInBounds(data, recordPos) {
		lastError = 81
		return nil, false
	}
	if _, ok := tableFieldOffset(data, recordPos, 10); ok {
		lastError = 82
		return nil, false
	}
	lengthOffset, ok := tableFieldOffset(data, recordPos, 4)
	var recordLength int64
	if ok {
		if lengthOffset+8 > len(data) {
			lastError = 83
			return nil, false
		}
		recordLength = readInt64(data[lengthOffset:])
	}
	if !ok {
		recordLength = 0
	}
	if recordLength < 0 || uint64(recordLength) > uint64(maxInt) {
		lastError = 83
		return nil, false
	}
	nodesVector, ok := vectorStart(data, recordPos, 6)
	if !ok || nodesVector+4 > len(data) || readUint32(data[nodesVector:]) < 1 {
		lastError = 85
		return nil, false
	}
	nodeOffset := nodesVector + 4
	if nodeOffset+16 > len(data) {
		lastError = 86
		return nil, false
	}
	nodeLength := readInt64(data[nodeOffset:])
	nullCount := readInt64(data[nodeOffset+8:])
	rows := int(recordLength)
	if nodeLength != int64(rows) || nullCount < 0 {
		lastError = 87
		return nil, false
	}
	buffersVector, ok := vectorStart(data, recordPos, 8)
	if !ok || buffersVector+4 > len(data) || readUint32(data[buffersVector:]) < 2 {
		lastError = 88
		return nil, false
	}
	buffersOffset := buffersVector + 4
	if buffersOffset+32 > len(data) {
		lastError = 89
		return nil, false
	}
	validityOffset := readInt64(data[buffersOffset:])
	validityLength := readInt64(data[buffersOffset+8:])
	valuesOffset := readInt64(data[buffersOffset+16:])
	valuesLength := readInt64(data[buffersOffset+24:])
	offsetRows := 0
	valueStart64 := valuesOffset + int64(offsetRows*valueWidth)
	valueLen64 := int64(rows * valueWidth)
	if valuesLength < valueLen64 || valueStart64 < 0 || valueLen64 < 0 || valueStart64+valueLen64 > int64(len(body)) {
		lastError = 90
		return nil, false
	}
	valueStart := int(valueStart64)
	if validityOffset < 0 || validityLength < 0 || validityOffset+validityLength > int64(len(body)) {
		lastError = 91
		return nil, false
	}
	validityBytes := body[int(validityOffset):int(validityOffset+validityLength)]
	out := make([]RowValue, 0, rows)
	rawValues := body[valueStart : valueStart+int(valueLen64)]
	for row := 0; row < rows; row++ {
		valid := true
		if nullCount > 0 {
			valid = bitIsSet(validityBytes, offsetRows+row)
		}
		rowStart := row * valueWidth
		out = append(out, RowValue{
			Data:  append([]byte(nil), rawValues[rowStart:rowStart+valueWidth]...),
			Valid: valid,
		})
	}
	return out, true
}

func batchTemplate(metadata []byte, recordPos int) (Batch, bool) {
	bodyLengthOffset, ok := tableFieldOffset(metadata, int(readUint32(metadata)), 10)
	if !ok {
		bodyLengthOffset = -1
	} else if bodyLengthOffset+8 > len(metadata) {
		return Batch{}, false
	}
	recordLengthOffset, ok := tableFieldOffset(metadata, recordPos, 4)
	if !ok {
		recordLengthOffset = -1
	} else if recordLengthOffset+8 > len(metadata) {
		return Batch{}, false
	}
	nodesVector, ok := vectorStart(metadata, recordPos, 6)
	if !ok || nodesVector+20 > len(metadata) {
		return Batch{}, false
	}
	buffersVector, ok := vectorStart(metadata, recordPos, 8)
	if !ok || buffersVector+36 > len(metadata) {
		return Batch{}, false
	}
	return Batch{
		Metadata:             metadata,
		BodyLengthOffset:     bodyLengthOffset,
		RecordLengthOffset:   recordLengthOffset,
		NodeLengthOffset:     nodesVector + 4,
		NodeNullCountOffset:  nodesVector + 12,
		NullBufferOffset:     buffersVector + 4,
		NullBufferLenOffset:  buffersVector + 12,
		ValueBufferOffset:    buffersVector + 20,
		ValueBufferLenOffset: buffersVector + 28,
	}, true
}

func buildRecordBatchMessage(batch Batch, values [][]byte) ([]byte, []byte) {
	if len(batch.Metadata) == 0 {
		return nil, nil
	}
	if len(values) > 0 && (batch.BodyLengthOffset < 0 || batch.RecordLengthOffset < 0) {
		return nil, nil
	}
	body := make([]byte, 0)
	for i := 0; i < len(values); i++ {
		body = append(body, values[i]...)
	}
	metadata := append([]byte(nil), batch.Metadata...)
	bodyLength := int64(len(body))
	if batch.BodyLengthOffset >= 0 {
		writeInt64(metadata[batch.BodyLengthOffset:], bodyLength)
	}
	if batch.RecordLengthOffset >= 0 {
		writeInt64(metadata[batch.RecordLengthOffset:], int64(len(values)))
	}
	writeInt64(metadata[batch.NodeLengthOffset:], int64(len(values)))
	writeInt64(metadata[batch.NodeNullCountOffset:], 0)
	writeInt64(metadata[batch.NullBufferOffset:], 0)
	writeInt64(metadata[batch.NullBufferLenOffset:], 0)
	writeInt64(metadata[batch.ValueBufferOffset:], 0)
	writeInt64(metadata[batch.ValueBufferLenOffset:], bodyLength)
	return metadata, body
}

func appendArrowMessage(output []byte, metadata []byte, body []byte) []byte {
	output = append(output, continuationMarker...)
	output = appendUint32(output, uint32(len(metadata)))
	output = append(output, metadata...)
	output = appendZeroPadding(output, len(metadata), 8)
	output = append(output, body...)
	output = appendZeroPadding(output, len(body), 8)
	return output
}

func flatbufferRoot(data []byte) (int, bool) {
	if len(data) < 4 {
		return 0, false
	}
	root := int(readUint32(data))
	if root < 4 || root >= len(data) {
		return 0, false
	}
	return root, true
}

func tableInBounds(data []byte, pos int) bool {
	if pos < 4 || pos+4 > len(data) {
		return false
	}
	vtable := pos - int(readInt32(data[pos:]))
	if vtable < 0 || vtable+4 > len(data) {
		return false
	}
	vtableLen := int(readUint16(data[vtable:]))
	if vtableLen < 4 || vtable+vtableLen > len(data) {
		return false
	}
	return int(readUint16(data[vtable+2:])) >= 4
}

func tableFieldOffset(data []byte, pos int, vtableOffset int) (int, bool) {
	if !tableInBounds(data, pos) {
		return 0, false
	}
	vtable := pos - int(readInt32(data[pos:]))
	vtableLen := int(readUint16(data[vtable:]))
	if vtableOffset < 0 || vtableOffset+2 > vtableLen {
		return 0, false
	}
	fieldOffset := int(readUint16(data[vtable+vtableOffset:]))
	if fieldOffset == 0 {
		return 0, false
	}
	field := pos + fieldOffset
	if field < pos || field > len(data) {
		return 0, false
	}
	return field, true
}

func vectorStart(data []byte, tablePos int, vtableOffset int) (int, bool) {
	field, ok := tableFieldOffset(data, tablePos, vtableOffset)
	if !ok || field+4 > len(data) {
		return 0, false
	}
	vector := field + int(readUint32(data[field:]))
	if vector < field || vector+4 > len(data) {
		return 0, false
	}
	length := int(readUint32(data[vector:]))
	if length < 0 || vector+4+length*4 < vector+4 {
		return 0, false
	}
	return vector, true
}

func bitIsSet(data []byte, bit int) bool {
	byteIndex := bit / 8
	if byteIndex < 0 || byteIndex >= len(data) {
		return false
	}
	return data[byteIndex]&(1<<uint(bit%8)) != 0
}

func paddedLen(size int, alignment int) int {
	remainder := size % alignment
	if remainder == 0 {
		return size
	}
	return size + alignment - remainder
}

func appendZeroPadding(output []byte, currentLen int, alignment int) []byte {
	padding := paddedLen(currentLen, alignment) - currentLen
	for i := 0; i < padding; i++ {
		output = append(output, 0)
	}
	return output
}

func readUint32(data []byte) uint32 {
	return uint32(data[0]) |
		uint32(data[1])<<8 |
		uint32(data[2])<<16 |
		uint32(data[3])<<24
}

func readInt32(data []byte) int32 {
	return int32(readUint32(data))
}

func readInt64(data []byte) int64 {
	return int64(uint64(data[0]) |
		uint64(data[1])<<8 |
		uint64(data[2])<<16 |
		uint64(data[3])<<24 |
		uint64(data[4])<<32 |
		uint64(data[5])<<40 |
		uint64(data[6])<<48 |
		uint64(data[7])<<56)
}

func readInt16(data []byte) int16 {
	return int16(readUint16(data))
}

func readUint16(data []byte) uint16 {
	return uint16(data[0]) | uint16(data[1])<<8
}

func appendUint32(out []byte, value uint32) []byte {
	for shift := uint(0); shift < 32; shift += 8 {
		out = append(out, byte(value>>shift))
	}
	return out
}

func writeInt16(out []byte, value int16) {
	out[0] = byte(value)
	out[1] = byte(value >> 8)
}

func writeInt64(out []byte, value int64) {
	unsigned := uint64(value)
	for shift := uint(0); shift < 64; shift += 8 {
		out[shift/8] = byte(unsigned >> shift)
	}
}
