// The Go probe: an independent implementation of the session protocol over native gRPC.
//
// It shares no code with the Rust client. It writes every ClientMessage frame with the FlatBuffers
// code flatc generates for Go from the session schema, reads every ServerMessage frame the Rust
// server writes with the same generated code, correlates replies by request identity, follows
// leader redirects with the same execution reference, and prints the same conformance report as
// every other probe. The Go FlatBuffers runtime has no verifier, so the probe checks each frame's
// identifier and every union discriminant and required value it reads.
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	session "nervix.dev/conformance/nervix/session"
)

const exchangePath = "/nervix.session.Session/Exchange"

// frameCodec carries one finished frame per gRPC message, as the schema's framing requires.
type frameCodec struct{}

func (frameCodec) Marshal(value any) ([]byte, error) {
	frame, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("a frame is a byte slice, not %T", value)
	}
	return frame, nil
}

func (frameCodec) Unmarshal(data []byte, value any) error {
	frame, ok := value.(*[]byte)
	if !ok {
		return fmt.Errorf("a frame is read into a byte slice, not %T", value)
	}
	*frame = append([]byte(nil), data...)
	return nil
}

func (frameCodec) Name() string { return "proto" }

type target struct {
	grpcURI, username, password, domain, relay, subscription string
	rows                                                     int
}

// exchange is one session: a bidirectional gRPC stream of frames.
type exchange struct {
	connection *grpc.ClientConn
	stream     grpc.ClientStream
	nextID     uint64
	// Unsolicited frames that arrived while a reply was awaited, kept in arrival order.
	pending []*session.ServerMessage
}

func dial(ctx context.Context, uri string, probe target) (*exchange, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "http" {
		return nil, fmt.Errorf("the probe speaks plaintext gRPC only, not %q", parsed.Scheme)
	}
	connection, err := grpc.NewClient(parsed.Host,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(frameCodec{})))
	if err != nil {
		return nil, err
	}
	credentials := base64.StdEncoding.EncodeToString([]byte(probe.username + ":" + probe.password))
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Basic "+credentials)
	description := &grpc.StreamDesc{StreamName: "Exchange", ServerStreams: true, ClientStreams: true}
	stream, err := connection.NewStream(ctx, description, exchangePath)
	if err != nil {
		connection.Close()
		return nil, err
	}
	return &exchange{connection: connection, stream: stream}, nil
}

func (e *exchange) close() {
	e.stream.CloseSend()
	e.connection.Close()
}

// receive reads and checks one ServerMessage frame.
func (e *exchange) receive() (*session.ServerMessage, error) {
	var frame []byte
	if err := e.stream.RecvMsg(&frame); err != nil {
		return nil, err
	}
	if len(frame) < 8 || string(frame[4:8]) != "NXSM" {
		return nil, errors.New("a server frame lacks the NXSM identifier")
	}
	return session.GetRootAsServerMessage(frame, 0), nil
}

// request sends one ClientMessage and returns the reply that carries its request identity.
func (e *exchange) request(kind session.ClientRequest,
	build func(*flatbuffers.Builder) flatbuffers.UOffsetT) (*session.Reply, error) {
	e.nextID++
	id := e.nextID
	builder := flatbuffers.NewBuilder(256)
	body := build(builder)
	session.ClientMessageStart(builder)
	session.ClientMessageAddRequestId(builder, id)
	session.ClientMessageAddRequestType(builder, kind)
	session.ClientMessageAddRequest(builder, body)
	builder.FinishWithFileIdentifier(session.ClientMessageEnd(builder), []byte("NXCM"))
	if err := e.stream.SendMsg(builder.FinishedBytes()); err != nil {
		return nil, err
	}
	for {
		message, err := e.receive()
		if err != nil {
			return nil, err
		}
		if message.BodyType() != session.ServerBodyReply {
			e.pending = append(e.pending, message)
			continue
		}
		reply := new(session.Reply)
		if err := union(message.Body, reply); err != nil {
			return nil, err
		}
		if reply.RequestId() != id {
			return nil, fmt.Errorf("a reply names request %d while %d is the only one in flight",
				reply.RequestId(), id)
		}
		if reply.BodyType() == session.ReplyBodyTransferPart {
			return nil, errors.New("the probe's replies are small and never arrive in parts")
		}
		return reply, nil
	}
}

// event returns the next unsolicited server message.
func (e *exchange) event() (*session.ServerMessage, error) {
	if len(e.pending) > 0 {
		message := e.pending[0]
		e.pending = e.pending[1:]
		return message, nil
	}
	message, err := e.receive()
	if err != nil {
		return nil, err
	}
	if message.BodyType() == session.ServerBodyReply {
		return nil, errors.New("a reply arrived while no request was in flight")
	}
	return message, nil
}

// union reads a union member, failing when the discriminant is present without its value.
func union(read func(*flatbuffers.Table) bool, table interface {
	Init([]byte, flatbuffers.UOffsetT)
}) error {
	var raw flatbuffers.Table
	if !read(&raw) {
		return errors.New("a union discriminant has no value")
	}
	table.Init(raw.Bytes, raw.Pos)
	return nil
}

// tableOf wraps the generated `Table()` accessor so union can initialize any generated table.
type tableOf struct{ flatbuffers.Table }

func (t *tableOf) Init(buf []byte, pos flatbuffers.UOffsetT) { t.Bytes, t.Pos = buf, pos }

func newReference() string {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		panic(err)
	}
	return hex.EncodeToString(random)
}

var dispositions = map[session.CommandDisposition]string{
	session.CommandDispositionCommandCompleted:           "completed",
	session.CommandDispositionRequestFailed:              "failed",
	session.CommandDispositionLeaderRedirect:             "not_leader",
	session.CommandDispositionTransactionDetached:        "transaction_detached",
	session.CommandDispositionTransactionTakenOver:       "transaction_taken_over",
	session.CommandDispositionOutcomeUnknown:             "outcome_unknown",
	session.CommandDispositionExecutionReferenceConflict: "execution_reference_conflict",
	session.CommandDispositionExecutionReferenceExpired:  "execution_reference_expired",
	session.CommandDispositionPreviewStale:               "preview_stale",
}

type probe struct {
	ctx    context.Context
	target target
	// The session the probe opened on its node. Its subscription lives here.
	home *exchange
	// The session commands are sent on: the home session until a redirect names the leader.
	commands *exchange
}

// command runs one statement, following leader redirects with the same execution reference.
// An outcome the server could not settle is a failure of the probe, never a success.
func (p *probe) command(query string) (*session.CommandOutcome, error) {
	reference := newReference()
	for attempt := 0; attempt < 50; attempt++ {
		reply, err := p.commands.request(session.ClientRequestCommandRequest,
			func(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
				queryOffset := builder.CreateString(query)
				domain := builder.CreateString(p.target.domain)
				referenceOffset := builder.CreateString(reference)
				session.CommandRequestStart(builder)
				session.CommandRequestAddQuery(builder, queryOffset)
				session.CommandRequestAddDomain(builder, domain)
				session.CommandRequestAddExecutionReference(builder, referenceOffset)
				return session.CommandRequestEnd(builder)
			})
		if err != nil {
			return nil, err
		}
		if reply.BodyType() != session.ReplyBodyCommandOutcome {
			return nil, fmt.Errorf("a command was answered with %s", reply.BodyType())
		}
		outcome := new(session.CommandOutcome)
		if err := union(reply.Body, outcome); err != nil {
			return nil, err
		}
		if string(outcome.ExecutionReference()) != reference {
			return nil, errors.New("an outcome names another execution reference")
		}
		switch outcome.DispositionType() {
		case session.CommandDispositionLeaderRedirect:
			redirect := new(session.LeaderRedirect)
			if err := union(outcome.Disposition, redirect); err != nil {
				return nil, err
			}
			leader := redirect.Leader(nil)
			if leader == nil || len(leader.GrpcUri()) == 0 {
				// No leader is known yet, for example during an election.
				time.Sleep(200 * time.Millisecond)
				continue
			}
			next, err := dial(p.ctx, string(leader.GrpcUri()), p.target)
			if err != nil {
				return nil, err
			}
			if p.commands != p.home {
				p.commands.close()
			}
			p.commands = next
			continue
		case session.CommandDispositionOutcomeUnknown:
			return nil, errors.New("a command's outcome is unknown; the probe never reports it as settled")
		}
		if _, known := dispositions[outcome.DispositionType()]; !known {
			return nil, fmt.Errorf("undeclared command disposition %d", outcome.DispositionType())
		}
		return outcome, nil
	}
	return nil, errors.New("no leader accepted the command")
}

type field struct {
	name, kind          string
	nullable, sensitive bool
}

func (f field) line(prefix string) string {
	nullable, sensitive := "required", "public"
	if f.nullable {
		nullable = "nullable"
	}
	if f.sensitive {
		sensitive = "sensitive"
	}
	return fmt.Sprintf("%s %s %s %s %s", prefix, f.name, f.kind, nullable, sensitive)
}

// typeName names a field type as the report does, lists with their element type and length.
func typeName(fieldType *session.FieldType) (string, error) {
	if fieldType == nil {
		return "", errors.New("a field type is missing")
	}
	switch fieldType.ShapeType() {
	case session.FieldTypeShapeScalarFieldType:
		scalar := new(session.ScalarFieldType)
		if err := union(fieldType.Shape, scalar); err != nil {
			return "", err
		}
		value := scalar.Scalar()
		if value == nil {
			return "", errors.New("a scalar field type has no scalar")
		}
		name, known := session.EnumNamesScalarType[*value]
		if !known {
			return "", fmt.Errorf("undeclared scalar type %d", *value)
		}
		return strings.ToUpper(name), nil
	case session.FieldTypeShapeFixedListFieldType:
		list := new(session.FixedListFieldType)
		if err := union(fieldType.Shape, list); err != nil {
			return "", err
		}
		element, err := typeName(list.Element(nil))
		if err != nil {
			return "", err
		}
		if list.Length() == 0 {
			return "", errors.New("a fixed list has no length")
		}
		return fmt.Sprintf("FIXED_LIST<%s,%d>", element, list.Length()), nil
	case session.FieldTypeShapeListFieldType:
		list := new(session.ListFieldType)
		if err := union(fieldType.Shape, list); err != nil {
			return "", err
		}
		element, err := typeName(list.Element(nil))
		if err != nil {
			return "", err
		}
		return "LIST<" + element + ">", nil
	}
	return "", fmt.Errorf("undeclared field type shape %d", fieldType.ShapeType())
}

func readField(row *session.RowField) (field, error) {
	kind, err := typeName(row.FieldType(nil))
	if err != nil {
		return field{}, err
	}
	return field{string(row.Name()), kind, row.Nullable(), row.Sensitive()}, nil
}

// render prints one cell as the report does: integers in decimal, floats as their bits, strings
// and bytes as hex.
func render(cell *session.Cell, schema field) (string, error) {
	if schema.sensitive != (cell.ValueType() == session.CellValueRedactedCell) {
		return "", fmt.Errorf("field %s is sensitive=%t but holds %s", schema.name, schema.sensitive,
			cell.ValueType())
	}
	switch cell.ValueType() {
	case session.CellValueNullCell:
		if !schema.nullable {
			return "", fmt.Errorf("required field %s holds a null", schema.name)
		}
		return "null", nil
	case session.CellValueRedactedCell:
		return "redacted", nil
	}
	return renderValue(cell)
}

// renderValue prints a cell that holds a value, recursing through list elements.
func renderValue(cell *session.Cell) (string, error) {
	var table tableOf
	if err := union(cell.Value, &table); err != nil {
		return "", err
	}
	bytes, pos := table.Bytes, table.Pos
	switch cell.ValueType() {
	case session.CellValueU8Cell:
		value := new(session.U8Cell)
		value.Init(bytes, pos)
		return fmt.Sprintf("u8:%d", value.Value()), nil
	case session.CellValueI8Cell:
		value := new(session.I8Cell)
		value.Init(bytes, pos)
		return fmt.Sprintf("i8:%d", value.Value()), nil
	case session.CellValueU16Cell:
		value := new(session.U16Cell)
		value.Init(bytes, pos)
		return fmt.Sprintf("u16:%d", value.Value()), nil
	case session.CellValueI16Cell:
		value := new(session.I16Cell)
		value.Init(bytes, pos)
		return fmt.Sprintf("i16:%d", value.Value()), nil
	case session.CellValueU32Cell:
		value := new(session.U32Cell)
		value.Init(bytes, pos)
		return fmt.Sprintf("u32:%d", value.Value()), nil
	case session.CellValueI32Cell:
		value := new(session.I32Cell)
		value.Init(bytes, pos)
		return fmt.Sprintf("i32:%d", value.Value()), nil
	case session.CellValueU64Cell:
		value := new(session.U64Cell)
		value.Init(bytes, pos)
		return fmt.Sprintf("u64:%d", value.Value()), nil
	case session.CellValueI64Cell:
		value := new(session.I64Cell)
		value.Init(bytes, pos)
		return fmt.Sprintf("i64:%d", value.Value()), nil
	case session.CellValueF32Cell:
		value := new(session.F32Cell)
		value.Init(bytes, pos)
		if value.Value() == nil {
			return "", errors.New("a float cell has no value")
		}
		return fmt.Sprintf("f32:%08x", math.Float32bits(*value.Value())), nil
	case session.CellValueF64Cell:
		value := new(session.F64Cell)
		value.Init(bytes, pos)
		if value.Value() == nil {
			return "", errors.New("a float cell has no value")
		}
		return fmt.Sprintf("f64:%016x", math.Float64bits(*value.Value())), nil
	case session.CellValueBoolCell:
		value := new(session.BoolCell)
		value.Init(bytes, pos)
		return fmt.Sprintf("bool:%t", value.Value()), nil
	case session.CellValueStringCell:
		value := new(session.StringCell)
		value.Init(bytes, pos)
		return "str:" + hex.EncodeToString(value.Value()), nil
	case session.CellValueBytesCell:
		value := new(session.BytesCell)
		value.Init(bytes, pos)
		return "bytes:" + hex.EncodeToString(value.ValueBytes()), nil
	case session.CellValueDatetimeCell:
		value := new(session.DatetimeCell)
		value.Init(bytes, pos)
		return fmt.Sprintf("datetime:%d", value.UnixNanos()), nil
	case session.CellValueListCell:
		list := new(session.ListCell)
		list.Init(bytes, pos)
		elements := make([]string, 0, list.ElementsLength())
		for index := 0; index < list.ElementsLength(); index++ {
			element := new(session.Cell)
			list.Elements(element, index)
			if kind := element.ValueType(); kind == session.CellValueNullCell || kind == session.CellValueRedactedCell {
				return "", errors.New("a list element is null or redacted")
			}
			rendered, err := renderValue(element)
			if err != nil {
				return "", err
			}
			elements = append(elements, rendered)
		}
		return "list[" + strings.Join(elements, ",") + "]", nil
	}
	return "", fmt.Errorf("undeclared cell kind %d", cell.ValueType())
}

func renderCells(length int, cellAt func(*session.Cell, int) bool, fields []field) (string, error) {
	if length != len(fields) {
		return "", fmt.Errorf("%d cells for %d fields", length, len(fields))
	}
	parts := make([]string, 0, length)
	for index, schema := range fields {
		cell := new(session.Cell)
		if !cellAt(cell, index) {
			return "", errors.New("a cell is missing")
		}
		rendered, err := render(cell, schema)
		if err != nil {
			return "", err
		}
		parts = append(parts, schema.name+"="+rendered)
	}
	return strings.Join(parts, " "), nil
}

func report(line string) { fmt.Println(line) }

func run() error {
	rows, err := strconv.Atoi(os.Getenv("NERVIX_PROBE_ROWS"))
	if err != nil {
		return err
	}
	probeTarget := target{
		grpcURI:      os.Getenv("NERVIX_PROBE_GRPC_URI"),
		username:     os.Getenv("NERVIX_PROBE_USERNAME"),
		password:     os.Getenv("NERVIX_PROBE_PASSWORD"),
		domain:       os.Getenv("NERVIX_PROBE_DOMAIN"),
		relay:        os.Getenv("NERVIX_PROBE_RELAY"),
		subscription: os.Getenv("NERVIX_PROBE_SUBSCRIPTION"),
		rows:         rows,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 170*time.Second)
	defer cancel()
	home, err := dial(ctx, probeTarget.grpcURI, probeTarget)
	if err != nil {
		return err
	}
	defer home.close()
	p := &probe{ctx: ctx, target: probeTarget, home: home, commands: home}

	operation, err := p.command("SHOW CREATE RELAY " + probeTarget.relay + ";")
	if err != nil {
		return err
	}
	report("OPERATION " + dispositions[operation.DispositionType()])

	failed, err := p.command("CREATE RELAY;")
	if err != nil {
		return err
	}
	span := "none"
	if failed.DiagnosticsLength() > 0 {
		diagnostic := new(session.Diagnostic)
		failed.Diagnostics(diagnostic, 0)
		if location := diagnostic.Span(nil); location != nil {
			span = fmt.Sprintf("%d..%d", location.Start(), location.End())
		}
	}
	report(fmt.Sprintf("ERROR %s diagnostics=%d span=%s", dispositions[failed.DispositionType()],
		failed.DiagnosticsLength(), span))

	subscribe := func(withType bool) (*session.Reply, error) {
		return home.request(session.ClientRequestSubscribeRequest,
			func(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
				domain := builder.CreateString(probeTarget.domain)
				statement := builder.CreateString(
					"CREATE SUBSCRIPTION " + probeTarget.subscription + " TO " + probeTarget.relay + ";")
				session.SubscribeRequestStart(builder)
				session.SubscribeRequestAddDomain(builder, domain)
				session.SubscribeRequestAddStatement(builder, statement)
				if withType {
					session.SubscribeRequestAddSubscriptionType(builder, session.SubscriptionTypeRow)
				}
				return session.SubscribeRequestEnd(builder)
			})
	}
	reply, err := subscribe(true)
	if err != nil {
		return err
	}
	if reply.BodyType() != session.ReplyBodySubscribeOutcome {
		return fmt.Errorf("a subscribe request was answered with %s", reply.BodyType())
	}
	subscribed := new(session.SubscribeOutcome)
	if err := union(reply.Body, subscribed); err != nil {
		return err
	}
	if subscribed.DispositionType() != session.SubscribeDispositionSubscriptionOpened {
		return fmt.Errorf("the subscription did not open: %s", subscribed.Message())
	}
	opened := new(session.SubscriptionOpened)
	if err := union(subscribed.Disposition, opened); err != nil {
		return err
	}
	if kind := opened.SubscriptionType(); kind == nil || *kind != session.SubscriptionTypeRow {
		return errors.New("the opened subscription does not confirm the Row type")
	}
	handle := opened.Subscription(nil)
	if handle == nil || handle.Generation() == 0 || string(handle.Name()) != probeTarget.subscription {
		return errors.New("the opened subscription has no valid handle")
	}
	generation := handle.Generation()
	schema := opened.Schema(nil)
	if schema == nil {
		return errors.New("the opened subscription has no schema")
	}
	var fields, keyFields []field
	for index := 0; index < schema.FieldsLength(); index++ {
		row := new(session.RowField)
		schema.Fields(row, index)
		parsed, err := readField(row)
		if err != nil {
			return err
		}
		fields = append(fields, parsed)
		report(parsed.line("FIELD"))
	}
	if branch := schema.Branch(nil); branch != nil {
		report("BRANCH " + string(branch.Branch()))
		for index := 0; index < branch.FieldsLength(); index++ {
			row := new(session.RowField)
			branch.Fields(row, index)
			parsed, err := readField(row)
			if err != nil {
				return err
			}
			keyFields = append(keyFields, parsed)
			report(parsed.line("KEY"))
		}
	}
	report("SUBSCRIBED")

	seen := 0
	for seen < probeTarget.rows {
		message, err := home.event()
		if err != nil {
			return err
		}
		if message.BodyType() != session.ServerBodySubscriptionRows {
			// Leadership, domain and cluster observations interleave with rows.
			switch message.BodyType() {
			case session.ServerBodyLeadershipObserved, session.ServerBodyDomainsObserved,
				session.ServerBodyDomainSnapshotObserved, session.ServerBodyClusterObserved,
				session.ServerBodyServerNotice:
				continue
			}
			return fmt.Errorf("the subscription reported %s before its rows arrived", message.BodyType())
		}
		rowsMessage := new(session.SubscriptionRows)
		if err := union(message.Body, rowsMessage); err != nil {
			return err
		}
		rowsHandle := rowsMessage.Subscription(nil)
		if rowsHandle == nil || string(rowsHandle.Name()) != probeTarget.subscription ||
			rowsHandle.Generation() != generation {
			return errors.New("rows arrived for another subscription")
		}
		batch := rowsMessage.Batch(nil)
		if batch == nil || batch.RowsLength() == 0 {
			return errors.New("a row batch is missing or empty")
		}
		key := ""
		if branchKey := batch.BranchKey(nil); branchKey != nil {
			key, err = renderCells(branchKey.CellsLength(), branchKey.Cells, keyFields)
			if err != nil {
				return err
			}
		} else if len(keyFields) > 0 {
			return errors.New("a batch of a branched relay has no branch key")
		}
		for index := 0; index < batch.RowsLength(); index++ {
			row := new(session.Row)
			batch.Rows(row, index)
			cells, err := renderCells(row.CellsLength(), row.Cells, fields)
			if err != nil {
				return err
			}
			report("ROW [" + key + "] " + cells)
			seen++
		}
	}

	// Protocol checks only a native client can make: a request that omits a required optional
	// scalar is refused rather than defaulted, and cancelling an identity that is not in flight
	// says so.
	refused, err := subscribe(false)
	if err != nil {
		return err
	}
	if refused.BodyType() != session.ReplyBodyRequestRejected {
		return fmt.Errorf("a subscribe request without a type was answered with %s", refused.BodyType())
	}
	cancelReply, err := home.request(session.ClientRequestCancelRequest,
		func(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
			session.CancelRequestStart(builder)
			session.CancelRequestAddTargetRequestId(builder, math.MaxUint64)
			return session.CancelRequestEnd(builder)
		})
	if err != nil {
		return err
	}
	cancelled := new(session.CancelOutcome)
	if cancelReply.BodyType() != session.ReplyBodyCancelOutcome || union(cancelReply.Body, cancelled) != nil {
		return errors.New("a cancel request was not answered with its outcome")
	}
	if state := cancelled.State(); state == nil || *state != session.CancelStateNotInFlight ||
		cancelled.TargetRequestId() != math.MaxUint64 {
		return errors.New("cancelling an identity that is not in flight did not say so")
	}
	report("CHECKS ok")

	unsubscribed, err := home.request(session.ClientRequestUnsubscribeRequest,
		func(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
			name := builder.CreateString(probeTarget.subscription)
			session.UnsubscribeRequestStart(builder)
			session.UnsubscribeRequestAddSubscription(builder, name)
			return session.UnsubscribeRequestEnd(builder)
		})
	if err != nil {
		return err
	}
	closed := new(session.UnsubscribeOutcome)
	if unsubscribed.BodyType() != session.ReplyBodyUnsubscribeOutcome || union(unsubscribed.Body, closed) != nil {
		return errors.New("an unsubscribe request was not answered with its outcome")
	}
	switch closed.DispositionType() {
	case session.UnsubscribeDispositionSubscriptionDeleted:
		report("CLOSED completed")
	case session.UnsubscribeDispositionRequestFailed:
		report("CLOSED failed")
	default:
		return fmt.Errorf("undeclared unsubscribe disposition %d", closed.DispositionType())
	}
	if p.commands != home {
		p.commands.close()
	}
	report("PASS")
	return nil
}

func main() {
	if len(os.Args) == 3 && os.Args[1] == "corpus" {
		if err := corpus(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, "probe failed:", err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "probe failed:", err)
		os.Exit(1)
	}
}
