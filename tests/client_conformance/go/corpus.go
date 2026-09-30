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
	case session.ChoiceValueResourceVersionNumber:
		value := new(session.ResourceVersionNumber)
		value.Init(table.Bytes, table.Pos)
		return fmt.Sprintf("resource-version:%d", value.Version()), nil
	case session.ChoiceValueLatestResourceVersion:
		return "resource-version:LATEST", nil
	case session.ChoiceValueModelChoiceReference:
		value := new(session.ModelChoiceReference)
		value.Init(table.Bytes, table.Pos)
		node := value.Node(nil)
		if node == nil || node.Kind() == nil {
			return "", errors.New("a model choice has no typed node reference")
		}
		return fmt.Sprintf("model:%s/%s", strings.ToLower(session.EnumNamesModelKind[*node.Kind()]), node.Name()), nil
	case session.ChoiceValueFieldChoiceReference:
		value := new(session.FieldChoiceReference)
		value.Init(table.Bytes, table.Pos)
		return "field:" + string(value.Field()), nil
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

// commandLines renders one command outcome, its first line after head.
func commandLines(head string, outcome *session.CommandOutcome) ([]string, error) {
	origin := outcome.Origin()
	if origin == nil {
		return nil, errors.New("a command outcome has no origin")
	}
	name, known := dispositions[outcome.DispositionType()]
	if !known {
		return nil, fmt.Errorf("undeclared command disposition %d", outcome.DispositionType())
	}
	lines := []string{fmt.Sprintf("%s %s reference=%s origin=%s message=%s", head, name,
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
	if archive := outcome.Backup(nil); archive != nil {
		backup, err := backupLines(archive)
		if err != nil {
			return nil, err
		}
		lines = append(lines, backup...)
	}
	if report := outcome.Restore(nil); report != nil {
		restore, err := restoreReportLines(report)
		if err != nil {
			return nil, err
		}
		lines = append(lines, restore...)
	}
	return lines, nil
}

// producerLines renders an opened producer and the input schema its batches carry.
func producerLines(id uint64, opened *session.ProducerOpened, message []byte) ([]string, error) {
	contract := opened.Contract(nil)
	if contract == nil || contract.BytesLength() != 32 {
		return nil, errors.New("an opened producer's contract is not a 32-byte fingerprint")
	}
	if opened.AttachmentLength() != 16 {
		return nil, errors.New("an opened producer's attachment is not 16 bytes")
	}
	window := ""
	switch opened.WindowType() {
	case session.ProducerWindowSequentialProducerWindow:
		window = "sequential"
	case session.ProducerWindowParallelProducerWindow:
		parallel := new(session.ParallelProducerWindow)
		if err := union(opened.Window, parallel); err != nil {
			return nil, err
		}
		window = fmt.Sprintf("parallel:%d", parallel.Max())
	default:
		return nil, fmt.Errorf("undeclared producer window %d", opened.WindowType())
	}
	admission := opened.Admission()
	if admission == nil {
		return nil, errors.New("an opened producer has no admission state")
	}
	lines := []string{fmt.Sprintf(
		"REPLY %d INGESTOR_OPENED domain=%s ingestor=%s generation=%d contract=%s attachment=%s "+
			"window=%s ack_timeout=%d retry=%d/%d granted=%d/%d max_batch=%d/%d admission=%s message=%s",
		id, opened.Domain(), opened.Ingestor(), opened.Generation(),
		hex.EncodeToString(contract.BytesBytes()), hex.EncodeToString(opened.AttachmentBytes()), window,
		opened.AckTimeoutNanos(), opened.RetryBackoffNanos(), opened.RetryMaxBackoffNanos(),
		opened.GrantedBatches(), opened.GrantedBytes(), opened.MaxBatchBytes(), opened.MaxBatchRows(),
		session.EnumNamesProducerAdmission[*admission], text(message))}
	for index := 0; index < opened.FieldsLength(); index++ {
		row := new(session.RowField)
		opened.Fields(row, index)
		parsed, err := readField(row)
		if err != nil {
			return nil, err
		}
		lines = append(lines, parsed.line("FIELD"))
	}
	return lines, nil
}

// restoreReportLines renders what a restore applied, or for a dry run would apply.
func restoreReportLines(report *session.RestoreReport) ([]string, error) {
	mode := report.Mode()
	digest := report.Digest(nil)
	if mode == nil || report.TotalBytes() == 0 || digest == nil || digest.BytesLength() != 32 {
		return nil, errors.New("a restore report lacks its mode, size or digest")
	}
	users := "none"
	if restored := report.Users(nil); restored != nil {
		users = fmt.Sprintf("created:%d,skipped:%d,replaced:%d", restored.Created(),
			restored.Skipped(), restored.Replaced())
	}
	lines := []string{fmt.Sprintf("RESTORE mode=%s total_bytes=%d digest=%s captured_at=%d users=%s",
		session.EnumNamesRestoreMode[*mode], report.TotalBytes(),
		hex.EncodeToString(digest.BytesBytes()), report.CapturedAt(), users)}
	for index := 0; index < report.DomainsLength(); index++ {
		domain := new(session.RestoredDomain)
		if !report.Domains(domain, index) {
			return nil, errors.New("a restored domain is missing")
		}
		planned := "none"
		if domain.PlannedModels(nil) != nil {
			planned = "present"
		}
		lines = append(lines, fmt.Sprintf(
			"RESTORE_DOMAIN source=%s domain=%s resource_versions=%d models=%d planned_models=%s",
			domain.Source(), domain.Domain(), domain.ResourceVersions(), domain.Models(), planned))
	}
	for index := 0; index < report.StepsLength(); index++ {
		step := new(session.RestoreStepReport)
		if !report.Steps(step, index) {
			return nil, errors.New("a restore step is missing")
		}
		kind := step.Kind()
		outcome := step.Outcome()
		if kind == nil || outcome == nil {
			return nil, errors.New("a restore step lacks its kind or outcome")
		}
		domain := step.Domain()
		if (*kind == session.RestoreStepKindUsers) != (domain == nil) {
			return nil, errors.New("a restore step names a domain exactly when it changes one")
		}
		lines = append(lines, fmt.Sprintf("RESTORE_STEP kind=%s domain=%s outcome=%s",
			session.EnumNamesRestoreStepKind[*kind], orNone(domain),
			session.EnumNamesRestoreStepOutcome[*outcome]))
	}
	return lines, nil
}

// restoreLines renders one frame of a restore stream.
func restoreLines(frame []byte) ([]string, error) {
	if err := frameRoot(frame, "NXRM"); err != nil {
		return nil, err
	}
	message := session.GetRootAsRestoreMessage(frame, 0)
	switch message.PartType() {
	case session.RestorePartRestoreStart:
		start := new(session.RestoreStart)
		if err := union(message.Part, start); err != nil {
			return nil, err
		}
		digest := start.Digest(nil)
		if start.RequestId() == 0 || start.TotalBytes() == 0 || digest == nil || digest.BytesLength() != 32 {
			return nil, errors.New("a restore start lacks its request, size or digest")
		}
		return []string{fmt.Sprintf(
			"RESTORE_START request=%d reference=%s statement=%s total_bytes=%d digest=%s",
			start.RequestId(), start.ExecutionReference(), text(start.Statement()), start.TotalBytes(),
			hex.EncodeToString(digest.BytesBytes()))}, nil
	case session.RestorePartRestoreChunk:
		chunk := new(session.RestoreChunk)
		if err := union(message.Part, chunk); err != nil {
			return nil, err
		}
		if chunk.BytesLength() == 0 {
			return nil, errors.New("a restore chunk is empty")
		}
		return []string{"RESTORE_CHUNK bytes=" + hex.EncodeToString(chunk.BytesBytes())}, nil
	}
	return nil, fmt.Errorf("undeclared restore part %d", message.PartType())
}

// restoreReplyLines renders the frame that answers a restore stream.
func restoreReplyLines(frame []byte) ([]string, error) {
	if err := frameRoot(frame, "NXRR"); err != nil {
		return nil, err
	}
	reply := session.GetRootAsRestoreReply(frame, 0)
	request := "none"
	if id := reply.RequestId(); id != nil {
		request = fmt.Sprintf("%d", *id)
	}
	switch reply.DispositionType() {
	case session.RestoreDispositionCommandOutcome:
		outcome := new(session.CommandOutcome)
		if err := union(reply.Disposition, outcome); err != nil {
			return nil, err
		}
		return commandLines("RESTORE_REPLY "+request+" COMMAND", outcome)
	case session.RestoreDispositionRestoreUploadFailed:
		failed := new(session.RestoreUploadFailed)
		if err := union(reply.Disposition, failed); err != nil {
			return nil, err
		}
		failure := failed.Failure()
		if failure == nil {
			return nil, errors.New("a restore refusal has no reason")
		}
		return []string{fmt.Sprintf("RESTORE_REPLY %s FAILED failure=%s message=%s", request,
			session.EnumNamesRestoreUploadFailure[*failure], text(failed.Message()))}, nil
	}
	return nil, fmt.Errorf("undeclared restore disposition %d", reply.DispositionType())
}

// backupLines renders the archive a completed backup reports.
func backupLines(archive *session.BackupArchiveSummary) ([]string, error) {
	digest := archive.Digest(nil)
	resources := archive.Resources()
	if archive.TotalBytes() == 0 || digest == nil || digest.BytesLength() != 32 || resources == nil {
		return nil, errors.New("a backup archive lacks its size, digest or resources")
	}
	users := "none"
	if count := archive.Users(); count != nil {
		users = fmt.Sprintf("%d", *count)
	}
	lines := []string{fmt.Sprintf(
		"BACKUP total_bytes=%d digest=%s captured_at=%d retained_until=%d resources=%s users=%s",
		archive.TotalBytes(), hex.EncodeToString(digest.BytesBytes()), archive.CapturedAt(),
		archive.RetainedUntil(), session.EnumNamesBackupResources[*resources], users)}
	for index := 0; index < archive.DomainsLength(); index++ {
		domain := new(session.BackupDomainSummary)
		if !archive.Domains(domain, index) {
			return nil, errors.New("a backup domain is missing")
		}
		lines = append(lines, fmt.Sprintf(
			"BACKUP_DOMAIN domain=%s revision=%d sections=%d section_bytes=%d",
			domain.Domain(), domain.Revision(), domain.Sections(), domain.SectionBytes()))
	}
	return lines, nil
}

// submissionLine renders the terminal outcome of one submitted batch.
func submissionLine(id uint64, outcome *session.SubmissionOutcome) (string, error) {
	message := text(outcome.Message())
	switch outcome.DispositionType() {
	case session.SubmissionDispositionSubmissionCompleted:
		return fmt.Sprintf("REPLY %d SUBMISSION completed message=%s", id, message), nil
	case session.SubmissionDispositionSubmissionNotAdmitted:
		notAdmitted := new(session.SubmissionNotAdmitted)
		if err := union(outcome.Disposition, notAdmitted); err != nil {
			return "", err
		}
		refusal := notAdmitted.Refusal()
		if refusal == nil {
			return "", errors.New("a refused submission has no refusal")
		}
		return fmt.Sprintf("REPLY %d SUBMISSION not_admitted refusal=%s message=%s", id,
			session.EnumNamesSubmissionRefusal[*refusal], message), nil
	case session.SubmissionDispositionSubmissionFailed:
		failed := new(session.SubmissionFailed)
		if err := union(outcome.Disposition, failed); err != nil {
			return "", err
		}
		failure := failed.Failure()
		if failure == nil {
			return "", errors.New("a failed submission has no failure")
		}
		return fmt.Sprintf("REPLY %d SUBMISSION failed failure=%s message=%s", id,
			session.EnumNamesProcessingFailure[*failure], message), nil
	case session.SubmissionDispositionSubmissionOutcomeUnknown:
		unknown := new(session.SubmissionOutcomeUnknown)
		if err := union(outcome.Disposition, unknown); err != nil {
			return "", err
		}
		cause := unknown.Cause()
		if cause == nil {
			return "", errors.New("an unknown submission outcome has no cause")
		}
		return fmt.Sprintf("REPLY %d SUBMISSION unknown cause=%s message=%s", id,
			session.EnumNamesOutcomeUncertainty[*cause], message), nil
	}
	return "", fmt.Errorf("undeclared submission disposition %d", outcome.DispositionType())
}

// downloadRequestLines renders the request of a backup download.
func downloadRequestLines(frame []byte) ([]string, error) {
	if err := frameRoot(frame, "NXBQ"); err != nil {
		return nil, err
	}
	request := session.GetRootAsBackupDownloadRequest(frame, 0)
	return []string{"REQUEST DOWNLOAD_BACKUP reference=" + string(request.ExecutionReference())}, nil
}

// downloadLines renders one frame of a backup download stream.
func downloadLines(frame []byte) ([]string, error) {
	if err := frameRoot(frame, "NXBD"); err != nil {
		return nil, err
	}
	message := session.GetRootAsBackupDownloadMessage(frame, 0)
	switch message.PartType() {
	case session.BackupDownloadPartBackupArchiveStart:
		start := new(session.BackupArchiveStart)
		if err := union(message.Part, start); err != nil {
			return nil, err
		}
		digest := start.Digest(nil)
		if start.TotalBytes() == 0 || digest == nil || digest.BytesLength() != 32 {
			return nil, errors.New("a download start lacks its size or digest")
		}
		return []string{fmt.Sprintf("DOWNLOAD START total_bytes=%d digest=%s", start.TotalBytes(),
			hex.EncodeToString(digest.BytesBytes()))}, nil
	case session.BackupDownloadPartBackupArchiveChunk:
		chunk := new(session.BackupArchiveChunk)
		if err := union(message.Part, chunk); err != nil {
			return nil, err
		}
		if chunk.BytesLength() == 0 {
			return nil, errors.New("a download chunk is empty")
		}
		return []string{"DOWNLOAD CHUNK bytes=" + hex.EncodeToString(chunk.BytesBytes())}, nil
	case session.BackupDownloadPartBackupArchiveComplete:
		return []string{"DOWNLOAD COMPLETE"}, nil
	case session.BackupDownloadPartBackupDownloadFailed:
		failed := new(session.BackupDownloadFailed)
		if err := union(message.Part, failed); err != nil {
			return nil, err
		}
		failure := failed.Failure()
		if failure == nil {
			return nil, errors.New("a download refusal has no reason")
		}
		return []string{fmt.Sprintf("DOWNLOAD FAILED failure=%s message=%s",
			session.EnumNamesBackupDownloadFailure[*failure], text(failed.Message()))}, nil
	case session.BackupDownloadPartLeaderRedirect:
		redirect := new(session.LeaderRedirect)
		if err := union(message.Part, redirect); err != nil {
			return nil, err
		}
		if leader := redirect.Leader(nil); leader != nil {
			return []string{fmt.Sprintf("DOWNLOAD LEADER node=%s grpc=%s console=%s", leader.Node(),
				orNone(leader.GrpcUri()), orNone(leader.WebConsoleUri()))}, nil
		}
		return []string{"DOWNLOAD LEADER none"}, nil
	}
	return nil, fmt.Errorf("undeclared download part %d", message.PartType())
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
			return commandLines(fmt.Sprintf("REPLY %d COMMAND", id), outcome)
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
		case session.ReplyBodyOpenIngestorOutcome:
			outcome := new(session.OpenIngestorOutcome)
			if err := union(value.Body, outcome); err != nil {
				return nil, err
			}
			switch outcome.DispositionType() {
			case session.OpenIngestorDispositionProducerOpened:
				opened := new(session.ProducerOpened)
				if err := union(outcome.Disposition, opened); err != nil {
					return nil, err
				}
				return producerLines(id, opened, outcome.Message())
			case session.OpenIngestorDispositionProducerRefused:
				refused := new(session.ProducerRefused)
				if err := union(outcome.Disposition, refused); err != nil {
					return nil, err
				}
				refusal := refused.Refusal()
				if refusal == nil {
					return nil, errors.New("a refused producer has no refusal")
				}
				return []string{fmt.Sprintf("REPLY %d INGESTOR_REFUSED refusal=%s message=%s", id,
					session.EnumNamesProducerRefusal[*refusal], text(outcome.Message()))}, nil
			}
			return nil, fmt.Errorf("undeclared open disposition %d", outcome.DispositionType())
		case session.ReplyBodySubmissionOutcome:
			outcome := new(session.SubmissionOutcome)
			if err := union(value.Body, outcome); err != nil {
				return nil, err
			}
			line, err := submissionLine(id, outcome)
			if err != nil {
				return nil, err
			}
			return []string{line}, nil
		case session.ReplyBodyCloseIngestorOutcome:
			outcome := new(session.CloseIngestorOutcome)
			if err := union(value.Body, outcome); err != nil {
				return nil, err
			}
			disposition := ""
			switch outcome.DispositionType() {
			case session.CloseIngestorDispositionProducerClosed:
				disposition = "closed"
			case session.CloseIngestorDispositionProducerNotOpen:
				disposition = "not_open"
			default:
				return nil, fmt.Errorf("undeclared close disposition %d", outcome.DispositionType())
			}
			return []string{fmt.Sprintf("REPLY %d INGESTOR_CLOSE %s message=%s", id, disposition,
				text(outcome.Message()))}, nil
		case session.ReplyBodyOpenEmitterOutcome:
			outcome := new(session.OpenEmitterOutcome)
			if err := union(value.Body, outcome); err != nil {
				return nil, err
			}
			switch outcome.DispositionType() {
			case session.OpenEmitterDispositionEmitterOpened:
				opened := new(session.EmitterOpened)
				if err := union(outcome.Disposition, opened); err != nil {
					return nil, err
				}
				contract := opened.Contract(nil)
				if contract == nil || contract.BytesLength() != 32 {
					return nil, errors.New("an opened consumer's contract is not a 32-byte fingerprint")
				}
				window := "sequential"
				if opened.WindowType() == session.ConsumerWindowParallelConsumerWindow {
					parallel := new(session.ParallelConsumerWindow)
					if err := union(opened.Window, parallel); err != nil {
						return nil, err
					}
					window = fmt.Sprintf("parallel:%d", parallel.Max())
				}
				lines := []string{fmt.Sprintf("REPLY %d EMITTER_OPENED domain=%s emitter=%s generation=%d contract=%s window=%s ack_timeout=%d retry=%d/%d granted=%d/%d max_batch=%d/%d message=%s",
					id, opened.Domain(), opened.Emitter(), opened.Generation(), hex.EncodeToString(contract.BytesBytes()), window, opened.AckTimeoutNanos(), opened.RetryBackoffNanos(),
					opened.RetryMaxBackoffNanos(), opened.GrantedBatches(), opened.GrantedBytes(), opened.MaxBatchBytes(),
					opened.MaxBatchRows(), text(outcome.Message()))}
				for index := 0; index < opened.FieldsLength(); index++ {
					row := new(session.RowField)
					opened.Fields(row, index)
					parsed, err := readField(row)
					if err != nil {
						return nil, err
					}
					lines = append(lines, parsed.line("FIELD"))
				}
				return lines, nil
			case session.OpenEmitterDispositionEmitterRefused:
				refused := new(session.EmitterRefused)
				if err := union(outcome.Disposition, refused); err != nil {
					return nil, err
				}
				refusal := refused.Refusal()
				if refusal == nil {
					return nil, errors.New("emitter refusal is absent")
				}
				return []string{fmt.Sprintf("REPLY %d EMITTER_REFUSED refusal=%s message=%s", id,
					session.EnumNamesEmitterOpenRefusal[*refusal], text(outcome.Message()))}, nil
			}
		case session.ReplyBodyReadEmitterBatchOutcome:
			outcome := new(session.ReadEmitterBatchOutcome)
			if err := union(value.Body, outcome); err != nil {
				return nil, err
			}
			switch outcome.DispositionType() {
			case session.ReadEmitterDispositionEmitterBatchReceived:
				batch := new(session.EmitterBatchReceived)
				if err := union(outcome.Disposition, batch); err != nil {
					return nil, err
				}
				branch := "none"
				if bytes := batch.BranchFingerprintBytes(); bytes != nil {
					branch = hex.EncodeToString(bytes)
				}
				return []string{fmt.Sprintf("REPLY %d EMITTER_BATCH identity=%s reference=%s source=%s branch=%s body=%s members=%d now=%d",
					id, hex.EncodeToString(batch.IdentityBytes()), hex.EncodeToString(batch.ReferenceBytes()),
					batch.SourceRelay(), branch, hex.EncodeToString(batch.BatchBytes()), batch.Members(),
					batch.ExecutionNowUnixNanos())}, nil
			case session.ReadEmitterDispositionEmitterConsumerEnded:
				return []string{fmt.Sprintf("REPLY %d EMITTER_ENDED message=%s", id, text(outcome.Message()))}, nil
			}
		case session.ReplyBodySettleEmitterBatchOutcome:
			outcome := new(session.SettleEmitterBatchOutcome)
			if err := union(value.Body, outcome); err != nil {
				return nil, err
			}
			disposition := outcome.Disposition()
			if disposition == nil {
				return nil, errors.New("emitter settlement is absent")
			}
			return []string{fmt.Sprintf("REPLY %d EMITTER_SETTLED disposition=%s message=%s", id,
				session.EnumNamesEmitterSettlement[*disposition], text(outcome.Message()))}, nil
		case session.ReplyBodyCloseEmitterOutcome:
			outcome := new(session.CloseEmitterOutcome)
			if err := union(value.Body, outcome); err != nil {
				return nil, err
			}
			disposition := outcome.Disposition()
			if disposition == nil {
				return nil, errors.New("emitter close disposition is absent")
			}
			return []string{fmt.Sprintf("REPLY %d EMITTER_CLOSE disposition=%s message=%s", id,
				session.EnumNamesEmitterCloseDisposition[*disposition], text(outcome.Message()))}, nil
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
	case session.ServerBodyDomainClockTicked:
		ticked := new(session.DomainClockTicked)
		if err := union(message.Body, ticked); err != nil {
			return nil, err
		}
		return []string{fmt.Sprintf("EVENT DOMAIN_CLOCK_TICK domain=%s generation=%d id=%d boundary=%d authority_utc=%d serving_logical=%d",
			ticked.Domain(), ticked.Generation(), ticked.TickId(), ticked.LogicalBoundaryUnixNanos(),
			ticked.AuthorityUtcUnixNanos(), ticked.ServingLogicalUnixNanos())}, nil
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
	case session.ServerBodyProducerAdmissionChanged:
		changed := new(session.ProducerAdmissionChanged)
		if err := union(message.Body, changed); err != nil {
			return nil, err
		}
		admission := changed.Admission()
		if admission == nil {
			return nil, errors.New("an admission change has no admission state")
		}
		return []string{fmt.Sprintf("EVENT PRODUCER_ADMISSION producer=%d admission=%s",
			changed.Producer(), session.EnumNamesProducerAdmission[*admission])}, nil
	case session.ServerBodyProducerEnded:
		ended := new(session.ProducerEnded)
		if err := union(message.Body, ended); err != nil {
			return nil, err
		}
		reason := ended.Reason()
		if reason == nil {
			return nil, errors.New("an ended producer has no reason")
		}
		return []string{fmt.Sprintf("EVENT PRODUCER_ENDED producer=%d reason=%s message=%s",
			ended.Producer(), session.EnumNamesProducerEndReason[*reason], text(ended.Message()))}, nil
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
	case session.ClientRequestOpenIngestorRequest:
		open := new(session.OpenIngestorRequest)
		open.Init(table.Bytes, table.Pos)
		lines := []string{fmt.Sprintf("REQUEST %d OPEN_INGESTOR domain=%s ingestor=%s batches=%d bytes=%d",
			id, open.Domain(), open.Ingestor(), open.MaxOutstandingBatches(), open.MaxOutstandingBytes())}
		for index := 0; index < open.ExpectedFieldsLength(); index++ {
			row := new(session.RowField)
			open.ExpectedFields(row, index)
			parsed, err := readField(row)
			if err != nil {
				return nil, err
			}
			lines = append(lines, parsed.line("FIELD"))
		}
		return lines, nil
	case session.ClientRequestSubmitBatchRequest:
		submit := new(session.SubmitBatchRequest)
		submit.Init(table.Bytes, table.Pos)
		return []string{fmt.Sprintf("REQUEST %d SUBMIT_BATCH producer=%d batch=%s", id, submit.Producer(),
			hex.EncodeToString(submit.BatchBytes()))}, nil
	case session.ClientRequestCloseIngestorRequest:
		closing := new(session.CloseIngestorRequest)
		closing.Init(table.Bytes, table.Pos)
		return []string{fmt.Sprintf("REQUEST %d CLOSE_INGESTOR producer=%d", id, closing.Producer())}, nil
	case session.ClientRequestOpenEmitterRequest:
		open := new(session.OpenEmitterRequest)
		open.Init(table.Bytes, table.Pos)
		lines := []string{fmt.Sprintf("REQUEST %d OPEN_EMITTER domain=%s emitter=%s batches=%d bytes=%d",
			id, open.Domain(), open.Emitter(), open.MaxOutstandingBatches(), open.MaxOutstandingBytes())}
		for index := 0; index < open.ExpectedFieldsLength(); index++ {
			row := new(session.RowField)
			open.ExpectedFields(row, index)
			parsed, err := readField(row)
			if err != nil {
				return nil, err
			}
			lines = append(lines, parsed.line("FIELD"))
		}
		return lines, nil
	case session.ClientRequestReadEmitterBatchRequest:
		read := new(session.ReadEmitterBatchRequest)
		read.Init(table.Bytes, table.Pos)
		return []string{fmt.Sprintf("REQUEST %d READ_EMITTER_BATCH consumer=%d", id, read.Consumer())}, nil
	case session.ClientRequestSettleEmitterBatchRequest:
		settle := new(session.SettleEmitterBatchRequest)
		settle.Init(table.Bytes, table.Pos)
		decision := "ack"
		if selected := settle.Decision(); selected != nil {
			switch *selected {
			case session.EmitterBatchDecisionRetry:
				decision = "retry"
			case session.EmitterBatchDecisionReject:
				decision = "reject:" + text(settle.Reason())
			}
		}
		return []string{fmt.Sprintf("REQUEST %d SETTLE_EMITTER_BATCH consumer=%d reference=%s decision=%s",
			id, settle.Consumer(), hex.EncodeToString(settle.ReferenceBytes()), decision)}, nil
	case session.ClientRequestCloseEmitterRequest:
		close := new(session.CloseEmitterRequest)
		close.Init(table.Bytes, table.Pos)
		return []string{fmt.Sprintf("REQUEST %d CLOSE_EMITTER consumer=%d", id, close.Consumer())}, nil
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
		switch filepath.Ext(entry.Name()) {
		case ".nxcm", ".nxsm", ".nxbq", ".nxbd", ".nxrm", ".nxrr":
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
		switch filepath.Ext(file) {
		case ".nxcm":
			lines, err = clientLines(frame)
		case ".nxbq":
			lines, err = downloadRequestLines(frame)
		case ".nxbd":
			lines, err = downloadLines(frame)
		case ".nxrm":
			lines, err = restoreLines(frame)
		case ".nxrr":
			lines, err = restoreReplyLines(frame)
		default:
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
