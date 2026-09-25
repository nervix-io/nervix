package main

// The corpus mode: decodes every frame of the checked-in conformance corpus the Rust encoder
// wrote and prints the corpus report, which the scenario compares with the report checked in
// beside the frames.

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	session "nervix.dev/conformance/nervix/session"
)

const openedFrame = "server_subscription_opened.nxsm"

func text(value []byte) string { return "str:" + hex.EncodeToString(value) }

func orNone(value []byte) string {
	if value == nil {
		return "none"
	}
	return string(value)
}

func frameRoot(frame []byte, identifier string) error {
	if len(frame) < 8 || string(frame[4:8]) != identifier {
		return fmt.Errorf("the frame lacks the %s identifier", identifier)
	}
	return nil
}

// reply reads the reply a server frame holds.
func reply(frame []byte) (*session.Reply, error) {
	if err := frameRoot(frame, "NXSM"); err != nil {
		return nil, err
	}
	message := session.GetRootAsServerMessage(frame, 0)
	if message.BodyType() != session.ServerBodyReply {
		return nil, errors.New("the frame holds no reply")
	}
	value := new(session.Reply)
	return value, union(message.Body, value)
}

// schemaLines reads the opened subscription of a reply and the report lines of its schema.
func schemaLines(frame []byte) (*session.SubscriptionOpened, []field, []field, []string, error) {
	value, err := reply(frame)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	outcome := new(session.SubscribeOutcome)
	if value.BodyType() != session.ReplyBodySubscribeOutcome || union(value.Body, outcome) != nil {
		return nil, nil, nil, nil, errors.New("the reply opens no subscription")
	}
	opened := new(session.SubscriptionOpened)
	if outcome.DispositionType() != session.SubscribeDispositionSubscriptionOpened ||
		union(outcome.Disposition, opened) != nil {
		return nil, nil, nil, nil, errors.New("the subscription did not open")
	}
	schema := opened.Schema(nil)
	if schema == nil {
		return nil, nil, nil, nil, errors.New("the opened subscription has no schema")
	}
	var fields, keys []field
	var lines []string
	for index := 0; index < schema.FieldsLength(); index++ {
		row := new(session.RowField)
		schema.Fields(row, index)
		parsed, err := readField(row)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		fields = append(fields, parsed)
		lines = append(lines, parsed.line("FIELD"))
	}
	if branch := schema.Branch(nil); branch != nil {
		lines = append(lines, "BRANCH "+string(branch.Branch()))
		for index := 0; index < branch.FieldsLength(); index++ {
			row := new(session.RowField)
			branch.Fields(row, index)
			parsed, err := readField(row)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			keys = append(keys, parsed)
			lines = append(lines, parsed.line("KEY"))
		}
	}
	return opened, fields, keys, lines, nil
}

func commandLines(id uint64, outcome *session.CommandOutcome) ([]string, error) {
	origin := outcome.Origin()
	if origin == nil {
		return nil, errors.New("a command outcome has no origin")
	}
	name, known := dispositions[outcome.DispositionType()]
	if !known {
		return nil, fmt.Errorf("undeclared command disposition %d", outcome.DispositionType())
	}
	lines := []string{fmt.Sprintf("REPLY %d COMMAND %s reference=%s origin=%s message=%s", id, name,
		outcome.ExecutionReference(), session.EnumNamesOutcomeOrigin[*origin], text(outcome.Message()))}
	for index := 0; index < outcome.DiagnosticsLength(); index++ {
		diagnostic := new(session.Diagnostic)
		outcome.Diagnostics(diagnostic, index)
		span := "none"
		if location := diagnostic.Span(nil); location != nil {
			span = fmt.Sprintf("%d..%d", location.Start(), location.End())
		}
		lines = append(lines, fmt.Sprintf("DIAGNOSTIC span=%s message=%s", span, text(diagnostic.Message())))
	}
	switch outcome.DispositionType() {
	case session.CommandDispositionLeaderRedirect:
		redirect := new(session.LeaderRedirect)
		if err := union(outcome.Disposition, redirect); err != nil {
			return nil, err
		}
		if leader := redirect.Leader(nil); leader != nil {
			lines = append(lines, fmt.Sprintf("LEADER node=%s grpc=%s console=%s", leader.Node(),
				orNone(leader.GrpcUri()), orNone(leader.WebConsoleUri())))
		} else {
			lines = append(lines, "LEADER none")
		}
	case session.CommandDispositionOutcomeUnknown:
		unknown := new(session.OutcomeUnknown)
		if err := union(outcome.Disposition, unknown); err != nil {
			return nil, err
		}
		cause := unknown.Cause()
		if cause == nil {
			return nil, errors.New("an unknown outcome has no cause")
		}
		lines = append(lines, "UNKNOWN cause="+session.EnumNamesUnknownOutcomeCause[*cause])
	}
	return lines, nil
}

func serverLines(frame []byte, fields, keys []field) ([]string, error) {
	if err := frameRoot(frame, "NXSM"); err != nil {
		return nil, err
	}
	message := session.GetRootAsServerMessage(frame, 0)
	switch message.BodyType() {
	case session.ServerBodyReply:
		value := new(session.Reply)
		if err := union(message.Body, value); err != nil {
			return nil, err
		}
		id := value.RequestId()
		switch value.BodyType() {
		case session.ReplyBodyCommandOutcome:
			outcome := new(session.CommandOutcome)
			if err := union(value.Body, outcome); err != nil {
				return nil, err
			}
			return commandLines(id, outcome)
		case session.ReplyBodyRequestRejected:
			rejected := new(session.RequestRejected)
			if err := union(value.Body, rejected); err != nil {
				return nil, err
			}
			rejection := rejected.Rejection()
			if rejection == nil {
				return nil, errors.New("a rejection has no reason")
			}
			return []string{fmt.Sprintf("REPLY %d REJECTED %s field=%s message=%s", id,
				session.EnumNamesRequestRejection[*rejection], orNone(rejected.Field()),
				text(rejected.Message()))}, nil
		case session.ReplyBodySubscribeOutcome:
			opened, _, _, lines, err := schemaLines(frame)
			if err != nil {
				return nil, err
			}
			handle := opened.Subscription(nil)
			kind := opened.SubscriptionType()
			if handle == nil || kind == nil {
				return nil, errors.New("an opened subscription lacks its handle or type")
			}
			head := fmt.Sprintf("REPLY %d SUBSCRIBED name=%s generation=%d domain=%s relay=%s type=%s", id,
				handle.Name(), handle.Generation(), opened.Domain(), opened.Relay(),
				session.EnumNamesSubscriptionType[*kind])
			return append([]string{head}, lines...), nil
		}
		return nil, fmt.Errorf("the corpus holds no %s reply", value.BodyType())
	case session.ServerBodySubscriptionRows:
		rows := new(session.SubscriptionRows)
		if err := union(message.Body, rows); err != nil {
			return nil, err
		}
		handle := rows.Subscription(nil)
		batch := rows.Batch(nil)
		if handle == nil || batch == nil {
			return nil, errors.New("subscription rows lack their handle or batch")
		}
		lines := []string{fmt.Sprintf("EVENT ROWS name=%s generation=%d", handle.Name(), handle.Generation())}
		key := ""
		if branchKey := batch.BranchKey(nil); branchKey != nil {
			rendered, err := renderCells(branchKey.CellsLength(), branchKey.Cells, keys)
			if err != nil {
				return nil, err
			}
			key = rendered
		}
		for index := 0; index < batch.RowsLength(); index++ {
			row := new(session.Row)
			batch.Rows(row, index)
			rendered, err := renderCells(row.CellsLength(), row.Cells, fields)
			if err != nil {
				return nil, err
			}
			lines = append(lines, "ROW ["+key+"] "+rendered)
		}
		return lines, nil
	case session.ServerBodySubscriptionEnded:
		ended := new(session.SubscriptionEnded)
		if err := union(message.Body, ended); err != nil {
			return nil, err
		}
		handle := ended.Subscription(nil)
		reason := ended.Reason()
		if handle == nil || reason == nil {
			return nil, errors.New("an ended subscription lacks its handle or reason")
		}
		return []string{fmt.Sprintf("EVENT ENDED name=%s generation=%d reason=%s message=%s",
			handle.Name(), handle.Generation(), session.EnumNamesSubscriptionEndReason[*reason],
			text(ended.Message()))}, nil
	}
	return nil, fmt.Errorf("the corpus holds no %s message", message.BodyType())
}

func clientLines(frame []byte) ([]string, error) {
	if err := frameRoot(frame, "NXCM"); err != nil {
		return nil, err
	}
	message := session.GetRootAsClientMessage(frame, 0)
	id := message.RequestId()
	var table tableOf
	if err := union(message.Request, &table); err != nil {
		return nil, err
	}
	switch message.RequestType() {
	case session.ClientRequestCommandRequest:
		command := new(session.CommandRequest)
		command.Init(table.Bytes, table.Pos)
		position := "none"
		if expected := command.ExpectedTransactionPosition(); expected != nil {
			position = fmt.Sprintf("%d", *expected)
		}
		preview := "none"
		if expected := command.ExpectedPreview(nil); expected != nil {
			basis := expected.PlanningBasis(nil)
			if basis == nil || basis.BytesLength() != 32 {
				return nil, errors.New("a preview's planning basis is not a 32-byte fingerprint")
			}
			preview = fmt.Sprintf("%s/%d/%s", expected.TransactionId(), expected.Position(),
				hex.EncodeToString(basis.BytesBytes()))
		}
		return []string{fmt.Sprintf(
			"REQUEST %d COMMAND query=%s domain=%s reference=%s expected_position=%s expected_preview=%s",
			id, text(command.Query()), orNone(command.Domain()), command.ExecutionReference(), position,
			preview)}, nil
	case session.ClientRequestSubscribeRequest:
		subscribe := new(session.SubscribeRequest)
		subscribe.Init(table.Bytes, table.Pos)
		kind := subscribe.SubscriptionType()
		if kind == nil {
			return nil, errors.New("a subscribe request has no type")
		}
		return []string{fmt.Sprintf("REQUEST %d SUBSCRIBE domain=%s statement=%s type=%s", id,
			subscribe.Domain(), text(subscribe.Statement()), session.EnumNamesSubscriptionType[*kind])}, nil
	case session.ClientRequestCancelRequest:
		cancel := new(session.CancelRequest)
		cancel.Init(table.Bytes, table.Pos)
		return []string{fmt.Sprintf("REQUEST %d CANCEL target=%d", id, cancel.TargetRequestId())}, nil
	}
	return nil, fmt.Errorf("the corpus holds no %s request", message.RequestType())
}

func corpus(directory string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	var files []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".nxcm") || strings.HasSuffix(entry.Name(), ".nxsm") {
			files = append(files, entry.Name())
		}
	}
	sort.Strings(files)
	opened, err := os.ReadFile(filepath.Join(directory, openedFrame))
	if err != nil {
		return err
	}
	_, fields, keys, _, err := schemaLines(opened)
	if err != nil {
		return err
	}
	for _, file := range files {
		frame, err := os.ReadFile(filepath.Join(directory, file))
		if err != nil {
			return err
		}
		var lines []string
		if strings.HasSuffix(file, ".nxcm") {
			lines, err = clientLines(frame)
		} else {
			lines, err = serverLines(frame, fields, keys)
		}
		if err != nil {
			return fmt.Errorf("%s: %w", file, err)
		}
		report("FRAME " + file)
		for _, line := range lines {
			report(line)
		}
	}
	return nil
}
