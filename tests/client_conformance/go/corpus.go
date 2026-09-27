package main

// The corpus mode: decodes every frame of the checked-in conformance corpus the Rust encoder
// wrote and prints the corpus report, which the scenario compares with the report checked in
// beside the frames.

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
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

func optionalText(value []byte) string {
	if value == nil {
		return "none"
	}
	return text(value)
}

func choiceValue(kind session.ChoiceValue, table tableOf) (string, error) {
	switch kind {
	case session.ChoiceValueDomainPaceVariant:
		value := new(session.DomainPaceVariant)
		value.Init(table.Bytes, table.Pos)
		pace := value.Value()
		if pace == nil {
			return "", errors.New("a domain pace choice has no value")
		}
		return "pace:" + session.EnumNamesDomainPaceChoice[*pace], nil
	case session.ChoiceValuePlacementPolicyVariant:
		value := new(session.PlacementPolicyVariant)
		value.Init(table.Bytes, table.Pos)
		policy := value.Value()
		if policy == nil {
			return "", errors.New("a placement choice has no value")
		}
		return "placement:" + session.EnumNamesPlacementPolicyChoice[*policy], nil
	case session.ChoiceValueDomainChoiceReference:
		value := new(session.DomainChoiceReference)
		value.Init(table.Bytes, table.Pos)
		return "domain:" + string(value.Domain()), nil
	case session.ChoiceValueResourceChoiceReference:
		value := new(session.ResourceChoiceReference)
		value.Init(table.Bytes, table.Pos)
		return "resource:" + string(value.Resource()), nil
	case session.ChoiceValueModelChoiceReference:
		value := new(session.ModelChoiceReference)
		value.Init(table.Bytes, table.Pos)
		node := value.Node(nil)
		if node == nil || node.Kind() == nil {
			return "", errors.New("a model choice has no typed node reference")
		}
		return fmt.Sprintf("model:%s/%s", strings.ToLower(session.EnumNamesModelKind[*node.Kind()]), node.Name()), nil
	}
	return "", fmt.Errorf("undeclared choice value %d", kind)
}

// clockLine renders a domain clock as the serving node has it installed.
func clockLine(clock *session.DomainClockObservation) (string, error) {
	generation := clock.Generation()
	switch clock.StateType() {
	case session.DomainClockObservedStateStoppedDomainClock:
		return fmt.Sprintf("CLOCK generation=%d state=stopped", generation), nil
	case session.DomainClockObservedStateUninstalledDomainClock:
		return fmt.Sprintf("CLOCK generation=%d state=uninstalled", generation), nil
	case session.DomainClockObservedStateUnpacedDomainClock:
		return fmt.Sprintf("CLOCK generation=%d state=unpaced", generation), nil
	case session.DomainClockObservedStatePacedDomainClock:
		paced := new(session.PacedDomainClock)
		if err := union(clock.State, paced); err != nil {
			return "", err
		}
		rate := paced.TimeRate()
		if paced.PeriodNanos() == 0 || rate == nil {
			return "", errors.New("a paced clock lacks its period or rate")
		}
		return fmt.Sprintf(
			"CLOCK generation=%d state=paced period=%d skew=%d origin=%d anchor=%d rate=f64:%016x",
			generation, paced.PeriodNanos(), paced.SkewNanos(), paced.LogicalOriginUnixNanos(),
			paced.UtcAnchorUnixNanos(), math.Float64bits(*rate)), nil
	}
	return "", fmt.Errorf("undeclared domain clock state %d", clock.StateType())
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
		case session.ReplyBodySuggestOutcome:
			outcome := new(session.SuggestOutcome)
			if err := union(value.Body, outcome); err != nil {
				return nil, err
			}
			status := outcome.Status()
			if status == nil {
				return nil, errors.New("a suggestion reply has no status")
			}
			lines := []string{fmt.Sprintf("REPLY %d SUGGEST status=%s continuation=%s", id,
				session.EnumNamesSuggestionStatus[*status], orNone(outcome.Continuation()))}
			for index := 0; index < outcome.SuggestionsLength(); index++ {
				suggestion := new(session.Suggestion)
				if !outcome.Suggestions(suggestion, index) {
					return nil, errors.New("a suggestion is missing")
				}
				kind := suggestion.Kind()
				edit := suggestion.Edit(nil)
				if kind == nil || edit == nil {
					return nil, errors.New("a suggestion has no kind or edit")
				}
				lines = append(lines, fmt.Sprintf(
					"SUGGESTION kind=%s value=%s edit=%d..%d replacement=%s",
					session.EnumNamesSuggestionKind[*kind], text(suggestion.Value()),
					edit.Start(), edit.End(), text(edit.Replacement())))
			}
			return lines, nil
		case session.ReplyBodyChoiceOutcome:
			outcome := new(session.ChoiceOutcome)
			if err := union(value.Body, outcome); err != nil {
				return nil, err
			}
			status := outcome.Status()
			if status == nil {
				return nil, errors.New("a choice reply has no status")
			}
			lines := []string{fmt.Sprintf("REPLY %d CHOICE status=%s cursor=%s", id,
				session.EnumNamesChoiceStatus[*status], orNone(outcome.PageCursor()))}
			for index := 0; index < outcome.ChoicesLength(); index++ {
				choice := new(session.Choice)
				if !outcome.Choices(choice, index) {
					return nil, errors.New("a choice is missing")
				}
				var selected tableOf
				if err := union(choice.Value, &selected); err != nil {
					return nil, err
				}
				typed, err := choiceValue(choice.ValueType(), selected)
				if err != nil {
					return nil, err
				}
				presentation := choice.Presentation(nil)
				if presentation == nil {
					return nil, errors.New("a choice has no presentation")
				}
				lines = append(lines, fmt.Sprintf("CHOICE value=%s label=%s detail=%s group=%s",
					typed, text(presentation.Label()), optionalText(presentation.Detail()),
					optionalText(presentation.Group())))
			}
			return lines, nil
		case session.ReplyBodyDomainClockAttachOutcome:
			outcome := new(session.DomainClockAttachOutcome)
			if err := union(value.Body, outcome); err != nil {
				return nil, err
			}
			message := text(outcome.Message())
			switch outcome.DispositionType() {
			case session.DomainClockAttachDispositionDomainClockAttached:
				attached := new(session.DomainClockAttached)
				if err := union(outcome.Disposition, attached); err != nil {
					return nil, err
				}
				clock := attached.Clock(nil)
				if clock == nil {
					return nil, errors.New("an attached clock is missing")
				}
				line, err := clockLine(clock)
				if err != nil {
					return nil, err
				}
				return []string{fmt.Sprintf("REPLY %d DOMAIN_CLOCK_ATTACH attached domain=%s message=%s",
					id, attached.Domain(), message), line}, nil
			case session.DomainClockAttachDispositionDomainClockAlreadyAttached:
				already := new(session.DomainClockAlreadyAttached)
				if err := union(outcome.Disposition, already); err != nil {
					return nil, err
				}
				return []string{fmt.Sprintf("REPLY %d DOMAIN_CLOCK_ATTACH already_attached domain=%s message=%s",
					id, already.Domain(), message)}, nil
			case session.DomainClockAttachDispositionDomainNotFound:
				notFound := new(session.DomainNotFound)
				if err := union(outcome.Disposition, notFound); err != nil {
					return nil, err
				}
				return []string{fmt.Sprintf("REPLY %d DOMAIN_CLOCK_ATTACH domain_not_found domain=%s message=%s",
					id, notFound.Domain(), message)}, nil
			case session.DomainClockAttachDispositionRequestFailed:
				return []string{fmt.Sprintf("REPLY %d DOMAIN_CLOCK_ATTACH failed message=%s", id, message)}, nil
			}
			return nil, fmt.Errorf("undeclared attach disposition %d", outcome.DispositionType())
		case session.ReplyBodyDomainClockDetachOutcome:
			outcome := new(session.DomainClockDetachOutcome)
			if err := union(value.Body, outcome); err != nil {
				return nil, err
			}
			message := text(outcome.Message())
			switch outcome.DispositionType() {
			case session.DomainClockDetachDispositionDomainClockDetached:
				detached := new(session.DomainClockDetached)
				if err := union(outcome.Disposition, detached); err != nil {
					return nil, err
				}
				return []string{fmt.Sprintf("REPLY %d DOMAIN_CLOCK_DETACH detached domain=%s message=%s",
					id, detached.Domain(), message)}, nil
			case session.DomainClockDetachDispositionDomainClockNotAttached:
				notAttached := new(session.DomainClockNotAttached)
				if err := union(outcome.Disposition, notAttached); err != nil {
					return nil, err
				}
				return []string{fmt.Sprintf("REPLY %d DOMAIN_CLOCK_DETACH not_attached domain=%s message=%s",
					id, notAttached.Domain(), message)}, nil
			}
			return nil, fmt.Errorf("the corpus holds no %d detach disposition", outcome.DispositionType())
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
	case session.ServerBodyDomainClockObserved:
		observed := new(session.DomainClockObserved)
		if err := union(message.Body, observed); err != nil {
			return nil, err
		}
		clock := observed.Clock(nil)
		if clock == nil {
			return nil, errors.New("an observed clock is missing")
		}
		line, err := clockLine(clock)
		if err != nil {
			return nil, err
		}
		return []string{"EVENT DOMAIN_CLOCK domain=" + string(observed.Domain()), line}, nil
	case session.ServerBodyDomainClockAttachmentEnded:
		ended := new(session.DomainClockAttachmentEnded)
		if err := union(message.Body, ended); err != nil {
			return nil, err
		}
		reason := ended.Reason()
		if reason == nil {
			return nil, errors.New("an ended attachment has no reason")
		}
		return []string{fmt.Sprintf("EVENT DOMAIN_CLOCK_ENDED domain=%s reason=%s", ended.Domain(),
			session.EnumNamesDomainClockAttachmentEndReason[*reason])}, nil
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
	case session.ClientRequestSuggestRequest:
		suggest := new(session.SuggestRequest)
		suggest.Init(table.Bytes, table.Pos)
		return []string{fmt.Sprintf(
			"REQUEST %d SUGGEST input=%s cursor=%d domain=%s page_size=%d continuation=%s",
			id, text(suggest.Input()), suggest.Cursor(), orNone(suggest.Domain()),
			suggest.PageSize(), orNone(suggest.Continuation()))}, nil
	case session.ClientRequestChoiceLookupRequest:
		lookup := new(session.ChoiceLookupRequest)
		lookup.Init(table.Bytes, table.Pos)
		target := lookup.Target()
		if target == nil {
			return nil, errors.New("a choice lookup has no target")
		}
		dependencies := make([]string, 0, lookup.DependenciesLength())
		for index := 0; index < lookup.DependenciesLength(); index++ {
			selection := new(session.ChoiceSelection)
			if !lookup.Dependencies(selection, index) {
				return nil, errors.New("a choice dependency is missing")
			}
			var selected tableOf
			if err := union(selection.Value, &selected); err != nil {
				return nil, err
			}
			value, err := choiceValue(selection.ValueType(), selected)
			if err != nil {
				return nil, err
			}
			dependencies = append(dependencies, value)
		}
		return []string{fmt.Sprintf(
			"REQUEST %d CHOICE target=%s dependencies=[%s] search=%s page_size=%d cursor=%s",
			id, session.EnumNamesChoiceTarget[*target], strings.Join(dependencies, ","),
			text(lookup.Search()), lookup.PageSize(), orNone(lookup.PageCursor()))}, nil
	case session.ClientRequestCancelRequest:
		cancel := new(session.CancelRequest)
		cancel.Init(table.Bytes, table.Pos)
		return []string{fmt.Sprintf("REQUEST %d CANCEL target=%d", id, cancel.TargetRequestId())}, nil
	case session.ClientRequestAttachDomainClockRequest:
		attach := new(session.AttachDomainClockRequest)
		attach.Init(table.Bytes, table.Pos)
		return []string{fmt.Sprintf("REQUEST %d ATTACH_DOMAIN_CLOCK domain=%s", id, attach.Domain())}, nil
	case session.ClientRequestDetachDomainClockRequest:
		detach := new(session.DetachDomainClockRequest)
		detach.Init(table.Bytes, table.Pos)
		return []string{fmt.Sprintf("REQUEST %d DETACH_DOMAIN_CLOCK domain=%s", id, detach.Domain())}, nil
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
