// The TypeScript probe: an independent implementation of the session protocol over the binary
// WebSocket, run unchanged by Node.js and by Bun.
//
// It shares no code with the Rust client. It writes ClientMessage frames and reads ServerMessage
// frames with the code flatc generates for TypeScript from the session schema, sends each frame as
// exactly one binary message, correlates replies by request identity, follows leader redirects
// with the same execution reference, and prints the same conformance report as every other probe.
// Every 64-bit value is read as a BigInt, so values past Number.MAX_SAFE_INTEGER stay exact. The
// TypeScript FlatBuffers runtime has no verifier, so the probe checks each frame's identifier and
// every union discriminant and required value it reads.

import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import * as flatbuffers from 'flatbuffers';

import * as wire from './generated/nervix/session.js';

type Build = (builder: flatbuffers.Builder) => flatbuffers.Offset;

const environment = (name: string): string => {
  const value = process.env[name];
  if (value === undefined) {
    throw new Error(`${name} is not set`);
  }
  return value;
};

const corpusMode = process.argv[2] === 'corpus';

const target = corpusMode
  ? { websocketUri: '', domain: '', relay: '', subscription: '', rows: 0 }
  : {
      websocketUri: environment('NERVIX_PROBE_WEBSOCKET_URI'),
      domain: environment('NERVIX_PROBE_DOMAIN'),
      relay: environment('NERVIX_PROBE_RELAY'),
      subscription: environment('NERVIX_PROBE_SUBSCRIPTION'),
      rows: Number.parseInt(environment('NERVIX_PROBE_ROWS'), 10),
    };

const report = (line: string): void => {
  process.stdout.write(`${line}\n`);
};

const hex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');

/** One session: a WebSocket carrying one frame per binary message. */
class Exchange {
  private nextId = 0n;
  private readonly frames: Uint8Array[] = [];
  private readonly waiters: Array<(frame: Uint8Array | Error) => void> = [];
  private failure: Error | null = null;
  // Unsolicited messages that arrived while a reply was awaited, kept in arrival order.
  private readonly pending: wire.ServerMessage[] = [];

  private constructor(private readonly socket: WebSocket) {
    socket.addEventListener('message', (event: MessageEvent) => {
      if (!(event.data instanceof ArrayBuffer)) {
        this.fail(new Error('the server sent a text message'));
        return;
      }
      this.deliver(new Uint8Array(event.data));
    });
    socket.addEventListener('close', (event: CloseEvent) => {
      this.fail(new Error(`the session closed with code ${event.code}: ${event.reason}`));
    });
    socket.addEventListener('error', () => this.fail(new Error('the WebSocket failed')));
  }

  static open(uri: string): Promise<Exchange> {
    return new Promise((resolve, reject) => {
      const socket = new WebSocket(uri);
      socket.binaryType = 'arraybuffer';
      socket.addEventListener('open', () => resolve(new Exchange(socket)), { once: true });
      socket.addEventListener('error', () => reject(new Error(`failed to open ${uri}`)), {
        once: true,
      });
    });
  }

  close(): void {
    this.socket.close(1000);
  }

  private deliver(frame: Uint8Array): void {
    const waiter = this.waiters.shift();
    if (waiter === undefined) {
      this.frames.push(frame);
    } else {
      waiter(frame);
    }
  }

  private fail(error: Error): void {
    if (this.failure !== null) {
      return;
    }
    this.failure = error;
    for (const waiter of this.waiters.splice(0)) {
      waiter(error);
    }
  }

  /** Reads and checks one ServerMessage frame. */
  private async receive(): Promise<wire.ServerMessage> {
    const queued = this.frames.shift();
    const frame =
      queued ??
      (await new Promise<Uint8Array | Error>((resolve) => {
        if (this.failure !== null) {
          resolve(this.failure);
          return;
        }
        this.waiters.push(resolve);
      }));
    if (frame instanceof Error) {
      throw frame;
    }
    const buffer = new flatbuffers.ByteBuffer(frame);
    if (!buffer.__has_identifier('NXSM')) {
      throw new Error('a server frame lacks the NXSM identifier');
    }
    return wire.ServerMessage.getRootAsServerMessage(buffer);
  }

  /** Sends one ClientMessage and returns the reply that carries its request identity. */
  async request(kind: wire.ClientRequest, build: Build): Promise<wire.Reply> {
    this.nextId += 1n;
    const id = this.nextId;
    const builder = new flatbuffers.Builder(256);
    const body = build(builder);
    wire.ClientMessage.startClientMessage(builder);
    wire.ClientMessage.addRequestId(builder, id);
    wire.ClientMessage.addRequestType(builder, kind);
    wire.ClientMessage.addRequest(builder, body);
    builder.finish(wire.ClientMessage.endClientMessage(builder), 'NXCM');
    this.socket.send(builder.asUint8Array());
    for (;;) {
      const message = await this.receive();
      if (message.bodyType() !== wire.ServerBody.Reply) {
        this.pending.push(message);
        continue;
      }
      const reply = member(message.body(new wire.Reply()) as wire.Reply | null);
      if (reply.requestId() !== id) {
        throw new Error(`a reply names request ${reply.requestId()} while ${id} is in flight`);
      }
      if (reply.bodyType() === wire.ReplyBody.TransferPart) {
        throw new Error("the probe's replies are small and never arrive in parts");
      }
      return reply;
    }
  }

  /** The next unsolicited server message. */
  async event(): Promise<wire.ServerMessage> {
    const pending = this.pending.shift();
    if (pending !== undefined) {
      return pending;
    }
    const message = await this.receive();
    if (message.bodyType() === wire.ServerBody.Reply) {
      throw new Error('a reply arrived while no request was in flight');
    }
    return message;
  }
}

/** A union member or required table, failing when the frame lacks it. */
function member<T>(value: T | null): T {
  if (value === null) {
    throw new Error('a required table or union value is missing');
  }
  return value;
}

const DISPOSITIONS = new Map<wire.CommandDisposition, string>([
  [wire.CommandDisposition.CommandCompleted, 'completed'],
  [wire.CommandDisposition.RequestFailed, 'failed'],
  [wire.CommandDisposition.LeaderRedirect, 'not_leader'],
  [wire.CommandDisposition.TransactionDetached, 'transaction_detached'],
  [wire.CommandDisposition.TransactionTakenOver, 'transaction_taken_over'],
  [wire.CommandDisposition.OutcomeUnknown, 'outcome_unknown'],
  [wire.CommandDisposition.ExecutionReferenceConflict, 'execution_reference_conflict'],
  [wire.CommandDisposition.ExecutionReferenceExpired, 'execution_reference_expired'],
  [wire.CommandDisposition.PreviewStale, 'preview_stale'],
]);

/** The console session URI of a leader a redirect names, carrying this probe's credentials. */
function leaderWebsocketUri(webConsoleUri: string): string {
  const current = new URL(target.websocketUri);
  const leader = new URL(webConsoleUri);
  leader.protocol = leader.protocol === 'https:' ? 'wss:' : 'ws:';
  leader.pathname = '/console/ws';
  leader.search = current.search;
  return leader.toString();
}

class Probe {
  commands: Exchange;

  constructor(readonly home: Exchange) {
    this.commands = home;
  }

  /**
   * Runs one statement, following leader redirects with the same execution reference. An outcome
   * the server could not settle is a failure of the probe, never a success.
   */
  async command(query: string): Promise<wire.CommandOutcome> {
    const reference = crypto.randomUUID();
    for (let attempt = 0; attempt < 50; attempt += 1) {
      const reply = await this.commands.request(wire.ClientRequest.CommandRequest, (builder) => {
        const queryOffset = builder.createString(query);
        const domain = builder.createString(target.domain);
        const referenceOffset = builder.createString(reference);
        wire.CommandRequest.startCommandRequest(builder);
        wire.CommandRequest.addQuery(builder, queryOffset);
        wire.CommandRequest.addDomain(builder, domain);
        wire.CommandRequest.addExecutionReference(builder, referenceOffset);
        return wire.CommandRequest.endCommandRequest(builder);
      });
      if (reply.bodyType() !== wire.ReplyBody.CommandOutcome) {
        throw new Error(`a command was answered with ${wire.ReplyBody[reply.bodyType()]}`);
      }
      const outcome = member(reply.body(new wire.CommandOutcome()) as wire.CommandOutcome | null);
      if (outcome.executionReference() !== reference) {
        throw new Error('an outcome names another execution reference');
      }
      if (outcome.dispositionType() === wire.CommandDisposition.LeaderRedirect) {
        const redirect = member(
          outcome.disposition(new wire.LeaderRedirect()) as wire.LeaderRedirect | null,
        );
        const console = redirect.leader()?.webConsoleUri() ?? null;
        if (console === null) {
          // No leader is known yet, for example during an election.
          await new Promise((resolve) => setTimeout(resolve, 200));
          continue;
        }
        const next = await Exchange.open(leaderWebsocketUri(console));
        if (this.commands !== this.home) {
          this.commands.close();
        }
        this.commands = next;
        continue;
      }
      if (outcome.dispositionType() === wire.CommandDisposition.OutcomeUnknown) {
        throw new Error("a command's outcome is unknown; the probe never reports it as settled");
      }
      if (!DISPOSITIONS.has(outcome.dispositionType())) {
        throw new Error(`undeclared command disposition ${outcome.dispositionType()}`);
      }
      return outcome;
    }
    throw new Error('no leader accepted the command');
  }
}

interface Field {
  name: string;
  kind: string;
  nullable: boolean;
  sensitive: boolean;
}

const fieldLine = (prefix: string, field: Field): string =>
  `${prefix} ${field.name} ${field.kind} ${field.nullable ? 'nullable' : 'required'} ${
    field.sensitive ? 'sensitive' : 'public'
  }`;

/** Names a field type as the report does, lists with their element type and length. */
function typeName(fieldType: wire.FieldType | null): string {
  const type = member(fieldType);
  switch (type.shapeType()) {
    case wire.FieldTypeShape.ScalarFieldType: {
      const scalar = member(type.shape(new wire.ScalarFieldType()) as wire.ScalarFieldType | null);
      const value = scalar.scalar();
      if (value === null || wire.ScalarType[value] === undefined) {
        throw new Error('a scalar field type has no declared scalar');
      }
      return wire.ScalarType[value].toUpperCase();
    }
    case wire.FieldTypeShape.FixedListFieldType: {
      const list = member(type.shape(new wire.FixedListFieldType()) as wire.FixedListFieldType | null);
      if (list.length() === 0) {
        throw new Error('a fixed list has no length');
      }
      return `FIXED_LIST<${typeName(list.element())},${list.length()}>`;
    }
    case wire.FieldTypeShape.ListFieldType: {
      const list = member(type.shape(new wire.ListFieldType()) as wire.ListFieldType | null);
      return `LIST<${typeName(list.element())}>`;
    }
    default:
      throw new Error(`undeclared field type shape ${type.shapeType()}`);
  }
}

function readField(row: wire.RowField): Field {
  return {
    name: member(row.name()),
    kind: typeName(row.fieldType()),
    nullable: row.nullable(),
    sensitive: row.sensitive(),
  };
}

/**
 * The exact bits of a float cell, read as an unsigned integer of the float's width.
 *
 * The generated `value()` returns a Number, and a JavaScript engine may canonicalize a NaN it
 * materializes: JavaScriptCore, which Bun runs on, rewrites a NaN payload that V8 keeps. Reading
 * the bits never builds a Number, so every engine reports the bits the frame carries.
 */
function floatBits(cell: { bb: flatbuffers.ByteBuffer | null; bb_pos: number }, width: 4 | 8): string {
  const buffer = member(cell.bb);
  const offset = buffer.__offset(cell.bb_pos, 4);
  if (offset === 0) {
    throw new Error('a float cell has no value');
  }
  if (width === 4) {
    return buffer.readUint32(cell.bb_pos + offset).toString(16).padStart(8, '0');
  }
  return buffer.readUint64(cell.bb_pos + offset).toString(16).padStart(16, '0');
}

/** 64-bit values read so far, each checked to be a BigInt. */
const wideValues: bigint[] = [];

function wide(value: bigint): bigint {
  if (typeof value !== 'bigint') {
    throw new Error('a 64-bit value was not read as a BigInt');
  }
  wideValues.push(value);
  return value;
}

/** Prints one cell as the report does: integers in decimal, floats as bits, text as hex. */
function render(cell: wire.Cell, field: Field): string {
  const kind = cell.valueType();
  if (field.sensitive !== (kind === wire.CellValue.RedactedCell)) {
    throw new Error(`field ${field.name} is sensitive=${field.sensitive} but holds ${wire.CellValue[kind]}`);
  }
  switch (kind) {
    case wire.CellValue.NullCell:
      if (!field.nullable) {
        throw new Error(`required field ${field.name} holds a null`);
      }
      return 'null';
    case wire.CellValue.RedactedCell:
      return 'redacted';
    default:
      return renderValue(cell);
  }
}

/** Prints a cell that holds a value, recursing through list elements. */
function renderValue(cell: wire.Cell): string {
  const kind = cell.valueType();
  const value = <T>(table: T): T => member(cell.value(table) as T | null);
  switch (kind) {
    case wire.CellValue.U8Cell:
      return `u8:${value(new wire.U8Cell()).value()}`;
    case wire.CellValue.I8Cell:
      return `i8:${value(new wire.I8Cell()).value()}`;
    case wire.CellValue.U16Cell:
      return `u16:${value(new wire.U16Cell()).value()}`;
    case wire.CellValue.I16Cell:
      return `i16:${value(new wire.I16Cell()).value()}`;
    case wire.CellValue.U32Cell:
      return `u32:${value(new wire.U32Cell()).value()}`;
    case wire.CellValue.I32Cell:
      return `i32:${value(new wire.I32Cell()).value()}`;
    case wire.CellValue.U64Cell:
      return `u64:${wide(value(new wire.U64Cell()).value())}`;
    case wire.CellValue.I64Cell:
      return `i64:${wide(value(new wire.I64Cell()).value())}`;
    case wire.CellValue.F32Cell:
      return `f32:${floatBits(value(new wire.F32Cell()), 4)}`;
    case wire.CellValue.F64Cell:
      return `f64:${floatBits(value(new wire.F64Cell()), 8)}`;
    case wire.CellValue.BoolCell:
      return `bool:${value(new wire.BoolCell()).value()}`;
    case wire.CellValue.StringCell: {
      // The exact UTF-8 bytes, borrowed from the frame, including an embedded NUL.
      const text = value(new wire.StringCell()).value(flatbuffers.Encoding.UTF8_BYTES);
      return `str:${hex(member(text) as Uint8Array)}`;
    }
    case wire.CellValue.BytesCell:
      return `bytes:${hex(member(value(new wire.BytesCell()).valueArray()))}`;
    case wire.CellValue.DatetimeCell:
      return `datetime:${wide(value(new wire.DatetimeCell()).unixNanos())}`;
    case wire.CellValue.ListCell: {
      const list = value(new wire.ListCell());
      const elements: string[] = [];
      for (let index = 0; index < list.elementsLength(); index += 1) {
        const element = member(list.elements(index));
        const elementKind = element.valueType();
        if (elementKind === wire.CellValue.NullCell || elementKind === wire.CellValue.RedactedCell) {
          throw new Error('a list element is null or redacted');
        }
        elements.push(renderValue(element));
      }
      return `list[${elements.join(',')}]`;
    }
    default:
      throw new Error(`undeclared cell kind ${kind}`);
  }
}

function renderCells(
  length: number,
  cellAt: (index: number, obj: wire.Cell) => wire.Cell | null,
  fields: Field[],
): string {
  if (length !== fields.length) {
    throw new Error(`${length} cells for ${fields.length} fields`);
  }
  return fields
    .map((field, index) => `${field.name}=${render(member(cellAt(index, new wire.Cell())), field)}`)
    .join(' ');
}

async function main(): Promise<void> {
  const home = await Exchange.open(target.websocketUri);
  const probe = new Probe(home);

  const operation = await probe.command(`SHOW CREATE RELAY ${target.relay};`);
  report(`OPERATION ${DISPOSITIONS.get(operation.dispositionType())}`);

  const failed = await probe.command('CREATE RELAY;');
  let span = 'none';
  if (failed.diagnosticsLength() > 0) {
    const location = member(failed.diagnostics(0)).span();
    if (location !== null) {
      span = `${location.start()}..${location.end()}`;
    }
  }
  report(
    `ERROR ${DISPOSITIONS.get(failed.dispositionType())} diagnostics=${failed.diagnosticsLength()} span=${span}`,
  );

  const subscribe = (withType: boolean): Promise<wire.Reply> =>
    home.request(wire.ClientRequest.SubscribeRequest, (builder) => {
      const domain = builder.createString(target.domain);
      const statement = builder.createString(
        `CREATE SUBSCRIPTION ${target.subscription} TO ${target.relay};`,
      );
      wire.SubscribeRequest.startSubscribeRequest(builder);
      wire.SubscribeRequest.addDomain(builder, domain);
      wire.SubscribeRequest.addStatement(builder, statement);
      if (withType) {
        wire.SubscribeRequest.addSubscriptionType(builder, wire.SubscriptionType.Row);
      }
      return wire.SubscribeRequest.endSubscribeRequest(builder);
    });
  const reply = await subscribe(true);
  if (reply.bodyType() !== wire.ReplyBody.SubscribeOutcome) {
    throw new Error(`a subscribe request was answered with ${wire.ReplyBody[reply.bodyType()]}`);
  }
  const subscribed = member(reply.body(new wire.SubscribeOutcome()) as wire.SubscribeOutcome | null);
  if (subscribed.dispositionType() !== wire.SubscribeDisposition.SubscriptionOpened) {
    throw new Error(`the subscription did not open: ${subscribed.message()}`);
  }
  const opened = member(
    subscribed.disposition(new wire.SubscriptionOpened()) as wire.SubscriptionOpened | null,
  );
  if (opened.subscriptionType() !== wire.SubscriptionType.Row) {
    throw new Error('the opened subscription does not confirm the Row type');
  }
  const handle = member(opened.subscription());
  const generation = wide(handle.generation());
  if (generation === 0n || handle.name() !== target.subscription) {
    throw new Error('the opened subscription has no valid handle');
  }
  const schema = member(opened.schema());
  const fields: Field[] = [];
  for (let index = 0; index < schema.fieldsLength(); index += 1) {
    const field = readField(member(schema.fields(index)));
    fields.push(field);
    report(fieldLine('FIELD', field));
  }
  const keyFields: Field[] = [];
  const branch = schema.branch();
  if (branch !== null) {
    report(`BRANCH ${member(branch.branch())}`);
    for (let index = 0; index < branch.fieldsLength(); index += 1) {
      const field = readField(member(branch.fields(index)));
      keyFields.push(field);
      report(fieldLine('KEY', field));
    }
  }
  report('SUBSCRIBED');

  const observations = new Set([
    wire.ServerBody.LeadershipObserved,
    wire.ServerBody.DomainsObserved,
    wire.ServerBody.DomainSnapshotObserved,
    wire.ServerBody.ClusterObserved,
    wire.ServerBody.ServerNotice,
  ]);
  let seen = 0;
  while (seen < target.rows) {
    const message = await home.event();
    if (message.bodyType() !== wire.ServerBody.SubscriptionRows) {
      if (observations.has(message.bodyType())) {
        continue;
      }
      throw new Error(`the subscription reported ${wire.ServerBody[message.bodyType()]} first`);
    }
    const rows = member(message.body(new wire.SubscriptionRows()) as wire.SubscriptionRows | null);
    const rowsHandle = member(rows.subscription());
    if (rowsHandle.name() !== target.subscription || rowsHandle.generation() !== generation) {
      throw new Error('rows arrived for another subscription');
    }
    const batch = member(rows.batch());
    if (batch.rowsLength() === 0) {
      throw new Error('a row batch is empty');
    }
    const branchKey = batch.branchKey();
    if ((branchKey === null) !== (keyFields.length === 0)) {
      throw new Error('a batch carries a branch key exactly when its relay is branched');
    }
    const key =
      branchKey === null
        ? ''
        : renderCells(branchKey.cellsLength(), (index, obj) => branchKey.cells(index, obj), keyFields);
    for (let index = 0; index < batch.rowsLength(); index += 1) {
      const row = member(batch.rows(index));
      report(`ROW [${key}] ${renderCells(row.cellsLength(), (cell, obj) => row.cells(cell, obj), fields)}`);
      seen += 1;
    }
  }

  // Protocol checks only a native client can make: every 64-bit value stayed a BigInt, including
  // values past the safe-integer boundary a Number would round; a request that omits a required
  // optional scalar is refused rather than defaulted; and cancelling an identity that is not in
  // flight says so.
  if (!wideValues.some((value) => value > BigInt(Number.MAX_SAFE_INTEGER))) {
    throw new Error('no 64-bit value past the safe-integer boundary was read');
  }
  if (wideValues.some((value) => typeof value !== 'bigint')) {
    throw new Error('a 64-bit value was not read as a BigInt');
  }
  const refused = await subscribe(false);
  if (refused.bodyType() !== wire.ReplyBody.RequestRejected) {
    throw new Error(`a subscribe request without a type was answered with ${wire.ReplyBody[refused.bodyType()]}`);
  }
  const maximum = 0xffff_ffff_ffff_ffffn;
  const cancelReply = await home.request(wire.ClientRequest.CancelRequest, (builder) => {
    wire.CancelRequest.startCancelRequest(builder);
    wire.CancelRequest.addTargetRequestId(builder, maximum);
    return wire.CancelRequest.endCancelRequest(builder);
  });
  if (cancelReply.bodyType() !== wire.ReplyBody.CancelOutcome) {
    throw new Error('a cancel request was not answered with its outcome');
  }
  const cancelled = member(cancelReply.body(new wire.CancelOutcome()) as wire.CancelOutcome | null);
  if (cancelled.state() !== wire.CancelState.NotInFlight || cancelled.targetRequestId() !== maximum) {
    throw new Error('cancelling an identity that is not in flight did not say so');
  }
  report('CHECKS ok');

  const unsubscribed = await home.request(wire.ClientRequest.UnsubscribeRequest, (builder) => {
    const name = builder.createString(target.subscription);
    wire.UnsubscribeRequest.startUnsubscribeRequest(builder);
    wire.UnsubscribeRequest.addSubscription(builder, name);
    return wire.UnsubscribeRequest.endUnsubscribeRequest(builder);
  });
  if (unsubscribed.bodyType() !== wire.ReplyBody.UnsubscribeOutcome) {
    throw new Error('an unsubscribe request was not answered with its outcome');
  }
  const closed = member(unsubscribed.body(new wire.UnsubscribeOutcome()) as wire.UnsubscribeOutcome | null);
  switch (closed.dispositionType()) {
    case wire.UnsubscribeDisposition.SubscriptionDeleted:
      report('CLOSED completed');
      break;
    case wire.UnsubscribeDisposition.RequestFailed:
      report('CLOSED failed');
      break;
    default:
      throw new Error(`undeclared unsubscribe disposition ${closed.dispositionType()}`);
  }
  if (probe.commands !== home) {
    probe.commands.close();
  }
  home.close();
  report('PASS');
}

const deadline = setTimeout(() => {
  process.stderr.write('probe failed: the probe did not finish within its deadline\n');
  process.exit(1);
}, 170_000);

const text = (value: Uint8Array | string | null): string => {
  if (value === null) {
    return 'str:';
  }
  return `str:${hex(typeof value === 'string' ? new TextEncoder().encode(value) : value)}`;
};

const bytesOf = (read: (encoding: flatbuffers.Encoding) => string | Uint8Array | null): Uint8Array =>
  member(read(flatbuffers.Encoding.UTF8_BYTES)) as Uint8Array;

const optionalText = (
  read: (encoding: flatbuffers.Encoding) => string | Uint8Array | null,
): string => {
  const value = read(flatbuffers.Encoding.UTF8_BYTES);
  return value === null ? 'none' : text(value);
};

function choiceValue(source: wire.Choice | wire.ChoiceSelection): string {
  switch (source.valueType()) {
    case wire.ChoiceValue.DomainPaceVariant: {
      const value = member(
        source.value(new wire.DomainPaceVariant()) as wire.DomainPaceVariant | null,
      );
      return `pace:${wire.DomainPaceChoice[member(value.value())]}`;
    }
    case wire.ChoiceValue.PlacementPolicyVariant: {
      const value = member(
        source.value(new wire.PlacementPolicyVariant()) as wire.PlacementPolicyVariant | null,
      );
      return `placement:${wire.PlacementPolicyChoice[member(value.value())]}`;
    }
    case wire.ChoiceValue.DomainChoiceReference: {
      const value = member(
        source.value(new wire.DomainChoiceReference()) as wire.DomainChoiceReference | null,
      );
      return `domain:${value.domain()}`;
    }
    case wire.ChoiceValue.ResourceChoiceReference: {
      const value = member(
        source.value(new wire.ResourceChoiceReference()) as wire.ResourceChoiceReference | null,
      );
      return `resource:${value.resource()}`;
    }
    case wire.ChoiceValue.ResourceVersionNumber: {
      const value = member(
        source.value(new wire.ResourceVersionNumber()) as wire.ResourceVersionNumber | null,
      );
      return `resource-version:${value.version().toString()}`;
    }
    case wire.ChoiceValue.LatestResourceVersion:
      return 'resource-version:LATEST';
    case wire.ChoiceValue.ModelChoiceReference: {
      const value = member(
        source.value(new wire.ModelChoiceReference()) as wire.ModelChoiceReference | null,
      );
      const node = member(value.node());
      return `model:${wire.ModelKind[member(node.kind())].toLowerCase()}/${node.name()}`;
    }
    case wire.ChoiceValue.FieldChoiceReference: {
      const value = member(
        source.value(new wire.FieldChoiceReference()) as wire.FieldChoiceReference | null,
      );
      return `field:${value.field()}`;
    }
    default:
      throw new Error(`undeclared choice value ${source.valueType()}`);
  }
}

function frameBuffer(frame: Uint8Array, identifier: string): flatbuffers.ByteBuffer {
  const buffer = new flatbuffers.ByteBuffer(frame);
  if (frame.length < 8 || !buffer.__has_identifier(identifier)) {
    throw new Error(`the frame lacks the ${identifier} identifier`);
  }
  return buffer;
}

interface OpenedSchema {
  opened: wire.SubscriptionOpened;
  fields: Field[];
  keys: Field[];
  lines: string[];
}

function openedSchema(reply: wire.Reply): OpenedSchema {
  const outcome = member(reply.body(new wire.SubscribeOutcome()) as wire.SubscribeOutcome | null);
  if (outcome.dispositionType() !== wire.SubscribeDisposition.SubscriptionOpened) {
    throw new Error('the subscription did not open');
  }
  const opened = member(outcome.disposition(new wire.SubscriptionOpened()) as wire.SubscriptionOpened | null);
  const schema = member(opened.schema());
  const fields: Field[] = [];
  const keys: Field[] = [];
  const lines: string[] = [];
  for (let index = 0; index < schema.fieldsLength(); index += 1) {
    const field = readField(member(schema.fields(index)));
    fields.push(field);
    lines.push(fieldLine('FIELD', field));
  }
  const branch = schema.branch();
  if (branch !== null) {
    lines.push(`BRANCH ${member(branch.branch())}`);
    for (let index = 0; index < branch.fieldsLength(); index += 1) {
      const field = readField(member(branch.fields(index)));
      keys.push(field);
      lines.push(fieldLine('KEY', field));
    }
  }
  return { opened, fields, keys, lines };
}

/** Renders one command outcome, its first line after `head`. */
function commandLines(head: string, outcome: wire.CommandOutcome): string[] {
  const name = DISPOSITIONS.get(outcome.dispositionType());
  const origin = outcome.origin();
  if (name === undefined || origin === null) {
    throw new Error('a command outcome lacks a declared disposition or its origin');
  }
  const lines = [
    `${head} ${name} reference=${outcome.executionReference()} origin=${wire.OutcomeOrigin[origin]} message=${text(bytesOf((encoding) => outcome.message(encoding)))}`,
  ];
  for (let index = 0; index < outcome.diagnosticsLength(); index += 1) {
    const diagnostic = member(outcome.diagnostics(index));
    const span = diagnostic.span();
    const location = span === null ? 'none' : `${span.start()}..${span.end()}`;
    lines.push(`DIAGNOSTIC span=${location} message=${text(bytesOf((encoding) => diagnostic.message(encoding)))}`);
  }
  if (outcome.dispositionType() === wire.CommandDisposition.LeaderRedirect) {
    const redirect = member(outcome.disposition(new wire.LeaderRedirect()) as wire.LeaderRedirect | null);
    const leader = redirect.leader();
    lines.push(
      leader === null
        ? 'LEADER none'
        : `LEADER node=${leader.node()} grpc=${leader.grpcUri() ?? 'none'} console=${leader.webConsoleUri() ?? 'none'}`,
    );
  }
  if (outcome.dispositionType() === wire.CommandDisposition.OutcomeUnknown) {
    const unknown = member(outcome.disposition(new wire.OutcomeUnknown()) as wire.OutcomeUnknown | null);
    lines.push(`UNKNOWN cause=${wire.UnknownOutcomeCause[member(unknown.cause())]}`);
  }
  const archive = outcome.backup();
  if (archive !== null) {
    lines.push(...backupLines(archive));
  }
  const restore = outcome.restore();
  if (restore !== null) {
    lines.push(...restoreReportLines(restore));
  }
  return lines;
}

/** Renders what a restore applied, or for a dry run would apply. */
function restoreReportLines(report: wire.RestoreReport): string[] {
  const mode = report.mode();
  const digest = member(report.digest()).bytesArray();
  if (mode === null || report.totalBytes() === 0n || digest === null || digest.length !== 32) {
    throw new Error('a restore report lacks its mode, size or digest');
  }
  const restored = report.users();
  const users =
    restored === null
      ? 'none'
      : `created:${restored.created()},skipped:${restored.skipped()},replaced:${restored.replaced()}`;
  const lines = [
    `RESTORE mode=${wire.RestoreMode[mode]} total_bytes=${report.totalBytes()} digest=${hex(digest)} captured_at=${report.capturedAt()} users=${users}`,
  ];
  for (let index = 0; index < report.domainsLength(); index += 1) {
    const domain = member(report.domains(index));
    const planned = domain.plannedModels() === null ? 'none' : 'present';
    lines.push(
      `RESTORE_DOMAIN source=${domain.source()} domain=${domain.domain()} resource_versions=${domain.resourceVersions()} models=${domain.models()} planned_models=${planned}`,
    );
  }
  for (let index = 0; index < report.stepsLength(); index += 1) {
    const step = member(report.steps(index));
    const kind = step.kind();
    const outcome = step.outcome();
    if (kind === null || outcome === null) {
      throw new Error('a restore step lacks its kind or outcome');
    }
    const domain = step.domain();
    if ((kind === wire.RestoreStepKind.Users) !== (domain === null)) {
      throw new Error('a restore step names a domain exactly when it changes one');
    }
    lines.push(
      `RESTORE_STEP kind=${wire.RestoreStepKind[kind]} domain=${domain ?? 'none'} outcome=${wire.RestoreStepOutcome[outcome]}`,
    );
  }
  return lines;
}

/** Renders one frame of a restore stream. */
function restoreLines(frame: Uint8Array): string[] {
  const message = wire.RestoreMessage.getRootAsRestoreMessage(frameBuffer(frame, 'NXRM'));
  switch (message.partType()) {
    case wire.RestorePart.RestoreStart: {
      const start = member(message.part(new wire.RestoreStart()) as wire.RestoreStart | null);
      const digest = member(start.digest()).bytesArray();
      if (start.requestId() === 0n || start.totalBytes() === 0n || digest === null || digest.length !== 32) {
        throw new Error('a restore start lacks its request, size or digest');
      }
      return [
        `RESTORE_START request=${start.requestId()} reference=${start.executionReference()} statement=${text(bytesOf((encoding) => start.statement(encoding)))} total_bytes=${start.totalBytes()} digest=${hex(digest)}`,
      ];
    }
    case wire.RestorePart.RestoreChunk: {
      const chunk = member(message.part(new wire.RestoreChunk()) as wire.RestoreChunk | null);
      const bytes = chunk.bytesArray();
      if (bytes === null || bytes.length === 0) {
        throw new Error('a restore chunk is empty');
      }
      return [`RESTORE_CHUNK bytes=${hex(bytes)}`];
    }
    default:
      throw new Error(`undeclared restore part ${message.partType()}`);
  }
}

/** Renders the frame that answers a restore stream. */
function restoreReplyLines(frame: Uint8Array): string[] {
  const reply = wire.RestoreReply.getRootAsRestoreReply(frameBuffer(frame, 'NXRR'));
  const request = reply.requestId() ?? 'none';
  switch (reply.dispositionType()) {
    case wire.RestoreDisposition.CommandOutcome:
      return commandLines(
        `RESTORE_REPLY ${request} COMMAND`,
        member(reply.disposition(new wire.CommandOutcome()) as wire.CommandOutcome | null),
      );
    case wire.RestoreDisposition.RestoreUploadFailed: {
      const failed = member(reply.disposition(new wire.RestoreUploadFailed()) as wire.RestoreUploadFailed | null);
      return [
        `RESTORE_REPLY ${request} FAILED failure=${wire.RestoreUploadFailure[member(failed.failure())]} message=${text(bytesOf((encoding) => failed.message(encoding)))}`,
      ];
    }
    default:
      throw new Error(`undeclared restore disposition ${reply.dispositionType()}`);
  }
}

/** Renders the archive a completed backup reports. */
function backupLines(archive: wire.BackupArchiveSummary): string[] {
  const digest = member(archive.digest()).bytesArray();
  const resources = archive.resources();
  if (archive.totalBytes() === 0n || digest === null || digest.length !== 32 || resources === null) {
    throw new Error('a backup archive lacks its size, digest or resources');
  }
  const users = archive.users();
  const lines = [
    `BACKUP total_bytes=${archive.totalBytes()} digest=${hex(digest)} captured_at=${archive.capturedAt()} retained_until=${archive.retainedUntil()} resources=${wire.BackupResources[resources]} users=${users ?? 'none'}`,
  ];
  for (let index = 0; index < archive.domainsLength(); index += 1) {
    const domain = member(archive.domains(index));
    lines.push(
      `BACKUP_DOMAIN domain=${domain.domain()} revision=${domain.revision()} sections=${domain.sections()} section_bytes=${domain.sectionBytes()}`,
    );
  }
  return lines;
}

/** Renders the request of a backup download. */
function downloadRequestLines(frame: Uint8Array): string[] {
  const request = wire.BackupDownloadRequest.getRootAsBackupDownloadRequest(frameBuffer(frame, 'NXBQ'));
  return [`REQUEST DOWNLOAD_BACKUP reference=${request.executionReference()}`];
}

/** Renders one frame of a backup download stream. */
function downloadLines(frame: Uint8Array): string[] {
  const message = wire.BackupDownloadMessage.getRootAsBackupDownloadMessage(frameBuffer(frame, 'NXBD'));
  switch (message.partType()) {
    case wire.BackupDownloadPart.BackupArchiveStart: {
      const start = member(message.part(new wire.BackupArchiveStart()) as wire.BackupArchiveStart | null);
      const digest = member(start.digest()).bytesArray();
      if (start.totalBytes() === 0n || digest === null || digest.length !== 32) {
        throw new Error('a download start lacks its size or digest');
      }
      return [`DOWNLOAD START total_bytes=${start.totalBytes()} digest=${hex(digest)}`];
    }
    case wire.BackupDownloadPart.BackupArchiveChunk: {
      const chunk = member(message.part(new wire.BackupArchiveChunk()) as wire.BackupArchiveChunk | null);
      const bytes = chunk.bytesArray();
      if (bytes === null || bytes.length === 0) {
        throw new Error('a download chunk is empty');
      }
      return [`DOWNLOAD CHUNK bytes=${hex(bytes)}`];
    }
    case wire.BackupDownloadPart.BackupArchiveComplete:
      return ['DOWNLOAD COMPLETE'];
    case wire.BackupDownloadPart.BackupDownloadFailed: {
      const failed = member(message.part(new wire.BackupDownloadFailed()) as wire.BackupDownloadFailed | null);
      return [
        `DOWNLOAD FAILED failure=${wire.BackupDownloadFailure[member(failed.failure())]} message=${text(bytesOf((encoding) => failed.message(encoding)))}`,
      ];
    }
    case wire.BackupDownloadPart.LeaderRedirect: {
      const redirect = member(message.part(new wire.LeaderRedirect()) as wire.LeaderRedirect | null);
      const leader = redirect.leader();
      return [
        leader === null
          ? 'DOWNLOAD LEADER none'
          : `DOWNLOAD LEADER node=${leader.node()} grpc=${leader.grpcUri() ?? 'none'} console=${leader.webConsoleUri() ?? 'none'}`,
      ];
    }
    default:
      throw new Error(`undeclared download part ${message.partType()}`);
  }
}

/** Renders a domain clock as the serving node has it installed. */
function clockLine(clock: wire.DomainClockObservation): string {
  const generation = clock.generation();
  switch (clock.stateType()) {
    case wire.DomainClockObservedState.StoppedDomainClock:
      return `CLOCK generation=${generation} state=stopped`;
    case wire.DomainClockObservedState.UninstalledDomainClock:
      return `CLOCK generation=${generation} state=uninstalled`;
    case wire.DomainClockObservedState.UnpacedDomainClock:
      return `CLOCK generation=${generation} state=unpaced`;
    case wire.DomainClockObservedState.PacedDomainClock: {
      const paced = member(clock.state(new wire.PacedDomainClock()) as wire.PacedDomainClock | null);
      // A rate is positive and finite, so no engine rewrites its bits when it becomes a Number.
      const bits = new DataView(new ArrayBuffer(8));
      bits.setFloat64(0, member(paced.timeRate()));
      const rate = bits.getBigUint64(0).toString(16).padStart(16, '0');
      return `CLOCK generation=${generation} state=paced period=${paced.periodNanos()} skew=${paced.skewNanos()} origin=${paced.logicalOriginUnixNanos()} anchor=${paced.utcAnchorUnixNanos()} rate=f64:${rate}`;
    }
    default:
      throw new Error(`undeclared domain clock state ${clock.stateType()}`);
  }
}

/** Renders an opened producer and the input schema its batches carry. */
function producerLines(id: bigint, opened: wire.ProducerOpened, message: string): string[] {
  const contract = member(opened.contract()).bytesArray();
  if (contract === null || contract.length !== 32) {
    throw new Error("an opened producer's contract is not a 32-byte fingerprint");
  }
  const attachment = opened.attachmentArray();
  if (attachment === null || attachment.length !== 16) {
    throw new Error("an opened producer's attachment is not 16 bytes");
  }
  let window: string;
  switch (opened.windowType()) {
    case wire.ProducerWindow.SequentialProducerWindow:
      window = 'sequential';
      break;
    case wire.ProducerWindow.ParallelProducerWindow: {
      const parallel = member(
        opened.window(new wire.ParallelProducerWindow()) as wire.ParallelProducerWindow | null,
      );
      window = `parallel:${parallel.max()}`;
      break;
    }
    default:
      throw new Error(`undeclared producer window ${opened.windowType()}`);
  }
  const lines = [
    `REPLY ${id} INGESTOR_OPENED domain=${opened.domain()} ingestor=${opened.ingestor()} generation=${opened.generation()} contract=${hex(contract)} attachment=${hex(attachment)} window=${window} ack_timeout=${opened.ackTimeoutNanos()} retry=${opened.retryBackoffNanos()}/${opened.retryMaxBackoffNanos()} granted=${opened.grantedBatches()}/${opened.grantedBytes()} max_batch=${opened.maxBatchBytes()}/${opened.maxBatchRows()} admission=${wire.ProducerAdmission[member(opened.admission())]} message=${message}`,
  ];
  for (let index = 0; index < opened.fieldsLength(); index += 1) {
    lines.push(fieldLine('FIELD', readField(member(opened.fields(index)))));
  }
  return lines;
}

/** Renders the terminal outcome of one submitted batch. */
function submissionLine(id: bigint, outcome: wire.SubmissionOutcome): string {
  const message = text(bytesOf((encoding) => outcome.message(encoding)));
  switch (outcome.dispositionType()) {
    case wire.SubmissionDisposition.SubmissionCompleted:
      return `REPLY ${id} SUBMISSION completed message=${message}`;
    case wire.SubmissionDisposition.SubmissionNotAdmitted: {
      const notAdmitted = member(
        outcome.disposition(new wire.SubmissionNotAdmitted()) as wire.SubmissionNotAdmitted | null,
      );
      return `REPLY ${id} SUBMISSION not_admitted refusal=${wire.SubmissionRefusal[member(notAdmitted.refusal())]} message=${message}`;
    }
    case wire.SubmissionDisposition.SubmissionFailed: {
      const failed = member(outcome.disposition(new wire.SubmissionFailed()) as wire.SubmissionFailed | null);
      return `REPLY ${id} SUBMISSION failed failure=${wire.ProcessingFailure[member(failed.failure())]} message=${message}`;
    }
    case wire.SubmissionDisposition.SubmissionOutcomeUnknown: {
      const unknown = member(
        outcome.disposition(new wire.SubmissionOutcomeUnknown()) as wire.SubmissionOutcomeUnknown | null,
      );
      return `REPLY ${id} SUBMISSION unknown cause=${wire.OutcomeUncertainty[member(unknown.cause())]} message=${message}`;
    }
    default:
      throw new Error(`undeclared submission disposition ${outcome.dispositionType()}`);
  }
}

function serverLines(frame: Uint8Array, schema: OpenedSchema): string[] {
  const message = wire.ServerMessage.getRootAsServerMessage(frameBuffer(frame, 'NXSM'));
  switch (message.bodyType()) {
    case wire.ServerBody.Reply: {
      const reply = member(message.body(new wire.Reply()) as wire.Reply | null);
      const id = reply.requestId();
      switch (reply.bodyType()) {
        case wire.ReplyBody.CommandOutcome:
          return commandLines(`REPLY ${id} COMMAND`, member(reply.body(new wire.CommandOutcome()) as wire.CommandOutcome | null));
        case wire.ReplyBody.RequestRejected: {
          const rejected = member(reply.body(new wire.RequestRejected()) as wire.RequestRejected | null);
          return [
            `REPLY ${id} REJECTED ${wire.RequestRejection[member(rejected.rejection())]} field=${rejected.field() ?? 'none'} message=${text(bytesOf((encoding) => rejected.message(encoding)))}`,
          ];
        }
        case wire.ReplyBody.SubscribeOutcome: {
          const read = openedSchema(reply);
          const handle = member(read.opened.subscription());
          return [
            `REPLY ${id} SUBSCRIBED name=${handle.name()} generation=${handle.generation()} domain=${read.opened.domain()} relay=${read.opened.relay()} type=${wire.SubscriptionType[member(read.opened.subscriptionType())]}`,
            ...read.lines,
          ];
        }
        case wire.ReplyBody.SuggestOutcome: {
          const outcome = member(reply.body(new wire.SuggestOutcome()) as wire.SuggestOutcome | null);
          const lines = [
            `REPLY ${id} SUGGEST status=${wire.SuggestionStatus[member(outcome.status())]} continuation=${outcome.continuation() ?? 'none'}`,
          ];
          for (let index = 0; index < outcome.suggestionsLength(); index += 1) {
            const suggestion = member(outcome.suggestions(index));
            const edit = member(suggestion.edit());
            lines.push(
              `SUGGESTION kind=${wire.SuggestionKind[member(suggestion.kind())]} value=${text(bytesOf((encoding) => suggestion.value(encoding)))} edit=${edit.start()}..${edit.end()} replacement=${text(bytesOf((encoding) => edit.replacement(encoding)))}`,
            );
          }
          return lines;
        }
        case wire.ReplyBody.ChoiceOutcome: {
          const outcome = member(reply.body(new wire.ChoiceOutcome()) as wire.ChoiceOutcome | null);
          const lines = [
            `REPLY ${id} CHOICE status=${wire.ChoiceStatus[member(outcome.status())]} cursor=${outcome.pageCursor() ?? 'none'}`,
          ];
          for (let index = 0; index < outcome.choicesLength(); index += 1) {
            const choice = member(outcome.choices(index));
            const presentation = member(choice.presentation());
            lines.push(
              `CHOICE value=${choiceValue(choice)} label=${text(bytesOf((encoding) => presentation.label(encoding)))} detail=${optionalText((encoding) => presentation.detail(encoding))} group=${optionalText((encoding) => presentation.group(encoding))}`,
            );
          }
          return lines;
        }
        case wire.ReplyBody.DomainClockAttachOutcome: {
          const outcome = member(
            reply.body(new wire.DomainClockAttachOutcome()) as wire.DomainClockAttachOutcome | null,
          );
          const message = text(bytesOf((encoding) => outcome.message(encoding)));
          switch (outcome.dispositionType()) {
            case wire.DomainClockAttachDisposition.DomainClockAttached: {
              const attached = member(
                outcome.disposition(new wire.DomainClockAttached()) as wire.DomainClockAttached | null,
              );
              return [
                `REPLY ${id} DOMAIN_CLOCK_ATTACH attached domain=${attached.domain()} message=${message}`,
                clockLine(member(attached.clock())),
              ];
            }
            case wire.DomainClockAttachDisposition.DomainClockAlreadyAttached: {
              const already = member(
                outcome.disposition(new wire.DomainClockAlreadyAttached()) as wire.DomainClockAlreadyAttached | null,
              );
              return [`REPLY ${id} DOMAIN_CLOCK_ATTACH already_attached domain=${already.domain()} message=${message}`];
            }
            case wire.DomainClockAttachDisposition.DomainNotFound: {
              const notFound = member(outcome.disposition(new wire.DomainNotFound()) as wire.DomainNotFound | null);
              return [`REPLY ${id} DOMAIN_CLOCK_ATTACH domain_not_found domain=${notFound.domain()} message=${message}`];
            }
            case wire.DomainClockAttachDisposition.RequestFailed:
              return [`REPLY ${id} DOMAIN_CLOCK_ATTACH failed message=${message}`];
            default:
              throw new Error(`undeclared attach disposition ${outcome.dispositionType()}`);
          }
        }
        case wire.ReplyBody.DomainClockDetachOutcome: {
          const outcome = member(
            reply.body(new wire.DomainClockDetachOutcome()) as wire.DomainClockDetachOutcome | null,
          );
          const message = text(bytesOf((encoding) => outcome.message(encoding)));
          switch (outcome.dispositionType()) {
            case wire.DomainClockDetachDisposition.DomainClockDetached: {
              const detached = member(
                outcome.disposition(new wire.DomainClockDetached()) as wire.DomainClockDetached | null,
              );
              return [`REPLY ${id} DOMAIN_CLOCK_DETACH detached domain=${detached.domain()} message=${message}`];
            }
            case wire.DomainClockDetachDisposition.DomainClockNotAttached: {
              const notAttached = member(
                outcome.disposition(new wire.DomainClockNotAttached()) as wire.DomainClockNotAttached | null,
              );
              return [`REPLY ${id} DOMAIN_CLOCK_DETACH not_attached domain=${notAttached.domain()} message=${message}`];
            }
            default:
              throw new Error(`the corpus holds no ${outcome.dispositionType()} detach disposition`);
          }
        }
        case wire.ReplyBody.OpenIngestorOutcome: {
          const outcome = member(reply.body(new wire.OpenIngestorOutcome()) as wire.OpenIngestorOutcome | null);
          const message = text(bytesOf((encoding) => outcome.message(encoding)));
          switch (outcome.dispositionType()) {
            case wire.OpenIngestorDisposition.ProducerOpened:
              return producerLines(
                id,
                member(outcome.disposition(new wire.ProducerOpened()) as wire.ProducerOpened | null),
                message,
              );
            case wire.OpenIngestorDisposition.ProducerRefused: {
              const refused = member(outcome.disposition(new wire.ProducerRefused()) as wire.ProducerRefused | null);
              return [
                `REPLY ${id} INGESTOR_REFUSED refusal=${wire.ProducerRefusal[member(refused.refusal())]} message=${message}`,
              ];
            }
            default:
              throw new Error(`undeclared open disposition ${outcome.dispositionType()}`);
          }
        }
        case wire.ReplyBody.SubmissionOutcome:
          return [
            submissionLine(id, member(reply.body(new wire.SubmissionOutcome()) as wire.SubmissionOutcome | null)),
          ];
        case wire.ReplyBody.CloseIngestorOutcome: {
          const outcome = member(reply.body(new wire.CloseIngestorOutcome()) as wire.CloseIngestorOutcome | null);
          const message = text(bytesOf((encoding) => outcome.message(encoding)));
          switch (outcome.dispositionType()) {
            case wire.CloseIngestorDisposition.ProducerClosed:
              return [`REPLY ${id} INGESTOR_CLOSE closed message=${message}`];
            case wire.CloseIngestorDisposition.ProducerNotOpen:
              return [`REPLY ${id} INGESTOR_CLOSE not_open message=${message}`];
            default:
              throw new Error(`undeclared close disposition ${outcome.dispositionType()}`);
          }
        }
        case wire.ReplyBody.OpenEmitterOutcome: {
          const outcome = member(reply.body(new wire.OpenEmitterOutcome()) as wire.OpenEmitterOutcome | null);
          const message = text(bytesOf((encoding) => outcome.message(encoding)));
          switch (outcome.dispositionType()) {
            case wire.OpenEmitterDisposition.EmitterOpened: {
              const opened = member(outcome.disposition(new wire.EmitterOpened()) as wire.EmitterOpened | null);
              const window = opened.windowType() === wire.ConsumerWindow.ParallelConsumerWindow
                ? `parallel:${member(opened.window(new wire.ParallelConsumerWindow()) as wire.ParallelConsumerWindow | null).max()}`
                : 'sequential';
              const lines = [`REPLY ${id} EMITTER_OPENED domain=${opened.domain()} emitter=${opened.emitter()} window=${window} ack_timeout=${opened.ackTimeoutNanos()} retry=${opened.retryBackoffNanos()}/${opened.retryMaxBackoffNanos()} granted=${opened.grantedBatches()}/${opened.grantedBytes()} max_batch=${opened.maxBatchBytes()}/${opened.maxBatchRows()} message=${message}`];
              for (let index = 0; index < opened.fieldsLength(); index += 1) {
                lines.push(fieldLine('FIELD', readField(member(opened.fields(index)))));
              }
              return lines;
            }
            case wire.OpenEmitterDisposition.EmitterRefused: {
              const refused = member(outcome.disposition(new wire.EmitterRefused()) as wire.EmitterRefused | null);
              return [`REPLY ${id} EMITTER_REFUSED refusal=${wire.EmitterOpenRefusal[member(refused.refusal())]} message=${message}`];
            }
            default:
              throw new Error(`undeclared emitter open disposition ${outcome.dispositionType()}`);
          }
        }
        case wire.ReplyBody.ReadEmitterBatchOutcome: {
          const outcome = member(reply.body(new wire.ReadEmitterBatchOutcome()) as wire.ReadEmitterBatchOutcome | null);
          switch (outcome.dispositionType()) {
            case wire.ReadEmitterDisposition.EmitterBatchReceived: {
              const batch = member(outcome.disposition(new wire.EmitterBatchReceived()) as wire.EmitterBatchReceived | null);
              const branch = batch.branchFingerprintArray();
              return [`REPLY ${id} EMITTER_BATCH identity=${hex(member(batch.identityArray()))} reference=${hex(member(batch.referenceArray()))} source=${batch.sourceRelay()} branch=${branch === null ? 'none' : hex(branch)} body=${hex(member(batch.batchArray()))} members=${batch.members()} now=${batch.executionNowUnixNanos()}`];
            }
            case wire.ReadEmitterDisposition.EmitterConsumerEnded:
              return [`REPLY ${id} EMITTER_ENDED message=${text(bytesOf((encoding) => outcome.message(encoding)))}`];
            default:
              throw new Error(`undeclared emitter read disposition ${outcome.dispositionType()}`);
          }
        }
        case wire.ReplyBody.SettleEmitterBatchOutcome: {
          const outcome = member(reply.body(new wire.SettleEmitterBatchOutcome()) as wire.SettleEmitterBatchOutcome | null);
          return [`REPLY ${id} EMITTER_SETTLED disposition=${wire.EmitterSettlement[member(outcome.disposition())]} message=${text(bytesOf((encoding) => outcome.message(encoding)))}`];
        }
        case wire.ReplyBody.CloseEmitterOutcome: {
          const outcome = member(reply.body(new wire.CloseEmitterOutcome()) as wire.CloseEmitterOutcome | null);
          return [`REPLY ${id} EMITTER_CLOSE disposition=${wire.EmitterCloseDisposition[member(outcome.disposition())]} message=${text(bytesOf((encoding) => outcome.message(encoding)))}`];
        }
        default:
          throw new Error(`the corpus holds no ${wire.ReplyBody[reply.bodyType()]} reply`);
      }
    }
    case wire.ServerBody.SubscriptionRows: {
      const rows = member(message.body(new wire.SubscriptionRows()) as wire.SubscriptionRows | null);
      const handle = member(rows.subscription());
      const batch = member(rows.batch());
      const branchKey = batch.branchKey();
      const key =
        branchKey === null
          ? ''
          : renderCells(branchKey.cellsLength(), (index, obj) => branchKey.cells(index, obj), schema.keys);
      const lines = [`EVENT ROWS name=${handle.name()} generation=${handle.generation()}`];
      for (let index = 0; index < batch.rowsLength(); index += 1) {
        const row = member(batch.rows(index));
        lines.push(`ROW [${key}] ${renderCells(row.cellsLength(), (cell, obj) => row.cells(cell, obj), schema.fields)}`);
      }
      return lines;
    }
    case wire.ServerBody.SubscriptionEnded: {
      const ended = member(message.body(new wire.SubscriptionEnded()) as wire.SubscriptionEnded | null);
      const handle = member(ended.subscription());
      return [
        `EVENT ENDED name=${handle.name()} generation=${handle.generation()} reason=${wire.SubscriptionEndReason[member(ended.reason())]} message=${text(bytesOf((encoding) => ended.message(encoding)))}`,
      ];
    }
    case wire.ServerBody.DomainClockObserved: {
      const observed = member(message.body(new wire.DomainClockObserved()) as wire.DomainClockObserved | null);
      return [`EVENT DOMAIN_CLOCK domain=${observed.domain()}`, clockLine(member(observed.clock()))];
    }
    case wire.ServerBody.DomainClockTicked: {
      const ticked = member(message.body(new wire.DomainClockTicked()) as wire.DomainClockTicked | null);
      return [`EVENT DOMAIN_CLOCK_TICK domain=${ticked.domain()} generation=${ticked.generation()} id=${ticked.tickId()} boundary=${ticked.logicalBoundaryUnixNanos()} authority_utc=${ticked.authorityUtcUnixNanos()} serving_logical=${ticked.servingLogicalUnixNanos()}`];
    }
    case wire.ServerBody.DomainClockAttachmentEnded: {
      const ended = member(
        message.body(new wire.DomainClockAttachmentEnded()) as wire.DomainClockAttachmentEnded | null,
      );
      return [
        `EVENT DOMAIN_CLOCK_ENDED domain=${ended.domain()} reason=${wire.DomainClockAttachmentEndReason[member(ended.reason())]}`,
      ];
    }
    case wire.ServerBody.ProducerAdmissionChanged: {
      const changed = member(
        message.body(new wire.ProducerAdmissionChanged()) as wire.ProducerAdmissionChanged | null,
      );
      return [
        `EVENT PRODUCER_ADMISSION producer=${changed.producer()} admission=${wire.ProducerAdmission[member(changed.admission())]}`,
      ];
    }
    case wire.ServerBody.ProducerEnded: {
      const ended = member(message.body(new wire.ProducerEnded()) as wire.ProducerEnded | null);
      return [
        `EVENT PRODUCER_ENDED producer=${ended.producer()} reason=${wire.ProducerEndReason[member(ended.reason())]} message=${text(bytesOf((encoding) => ended.message(encoding)))}`,
      ];
    }
    default:
      throw new Error(`the corpus holds no ${wire.ServerBody[message.bodyType()]} message`);
  }
}

function clientLines(frame: Uint8Array): string[] {
  const message = wire.ClientMessage.getRootAsClientMessage(frameBuffer(frame, 'NXCM'));
  const id = message.requestId();
  switch (message.requestType()) {
    case wire.ClientRequest.CommandRequest: {
      const command = member(message.request(new wire.CommandRequest()) as wire.CommandRequest | null);
      const position = command.expectedTransactionPosition();
      const expected = command.expectedPreview();
      let preview = 'none';
      if (expected !== null) {
        const basis = member(expected.planningBasis()).bytesArray();
        if (basis === null || basis.length !== 32) {
          throw new Error("a preview's planning basis is not a 32-byte fingerprint");
        }
        preview = `${expected.transactionId()}/${expected.position()}/${hex(basis)}`;
      }
      return [
        `REQUEST ${id} COMMAND query=${text(bytesOf((encoding) => command.query(encoding)))} domain=${command.domain() ?? 'none'} reference=${command.executionReference()} expected_position=${position ?? 'none'} expected_preview=${preview}`,
      ];
    }
    case wire.ClientRequest.SubscribeRequest: {
      const subscribe = member(message.request(new wire.SubscribeRequest()) as wire.SubscribeRequest | null);
      return [
        `REQUEST ${id} SUBSCRIBE domain=${subscribe.domain()} statement=${text(bytesOf((encoding) => subscribe.statement(encoding)))} type=${wire.SubscriptionType[member(subscribe.subscriptionType())]}`,
      ];
    }
    case wire.ClientRequest.SuggestRequest: {
      const suggest = member(message.request(new wire.SuggestRequest()) as wire.SuggestRequest | null);
      return [
        `REQUEST ${id} SUGGEST input=${text(bytesOf((encoding) => suggest.input(encoding)))} cursor=${suggest.cursor()} domain=${suggest.domain() ?? 'none'} page_size=${suggest.pageSize()} continuation=${suggest.continuation() ?? 'none'}`,
      ];
    }
    case wire.ClientRequest.ChoiceLookupRequest: {
      const lookup = member(
        message.request(new wire.ChoiceLookupRequest()) as wire.ChoiceLookupRequest | null,
      );
      const dependencies: string[] = [];
      for (let index = 0; index < lookup.dependenciesLength(); index += 1) {
        dependencies.push(choiceValue(member(lookup.dependencies(index))));
      }
      return [
        `REQUEST ${id} CHOICE target=${wire.ChoiceTarget[member(lookup.target())]} dependencies=[${dependencies.join(',')}] search=${text(bytesOf((encoding) => lookup.search(encoding)))} page_size=${lookup.pageSize()} cursor=${lookup.pageCursor() ?? 'none'}`,
      ];
    }
    case wire.ClientRequest.CancelRequest: {
      const cancel = member(message.request(new wire.CancelRequest()) as wire.CancelRequest | null);
      return [`REQUEST ${id} CANCEL target=${cancel.targetRequestId()}`];
    }
    case wire.ClientRequest.AttachDomainClockRequest: {
      const attach = member(
        message.request(new wire.AttachDomainClockRequest()) as wire.AttachDomainClockRequest | null,
      );
      return [`REQUEST ${id} ATTACH_DOMAIN_CLOCK domain=${attach.domain()}`];
    }
    case wire.ClientRequest.DetachDomainClockRequest: {
      const detach = member(
        message.request(new wire.DetachDomainClockRequest()) as wire.DetachDomainClockRequest | null,
      );
      return [`REQUEST ${id} DETACH_DOMAIN_CLOCK domain=${detach.domain()}`];
    }
    case wire.ClientRequest.OpenIngestorRequest: {
      const open = member(message.request(new wire.OpenIngestorRequest()) as wire.OpenIngestorRequest | null);
      const lines = [
        `REQUEST ${id} OPEN_INGESTOR domain=${open.domain()} ingestor=${open.ingestor()} batches=${open.maxOutstandingBatches()} bytes=${open.maxOutstandingBytes()}`,
      ];
      for (let index = 0; index < open.expectedFieldsLength(); index += 1) {
        lines.push(fieldLine('FIELD', readField(member(open.expectedFields(index)))));
      }
      return lines;
    }
    case wire.ClientRequest.SubmitBatchRequest: {
      const submit = member(message.request(new wire.SubmitBatchRequest()) as wire.SubmitBatchRequest | null);
      return [`REQUEST ${id} SUBMIT_BATCH producer=${submit.producer()} batch=${hex(member(submit.batchArray()))}`];
    }
    case wire.ClientRequest.CloseIngestorRequest: {
      const close = member(message.request(new wire.CloseIngestorRequest()) as wire.CloseIngestorRequest | null);
      return [`REQUEST ${id} CLOSE_INGESTOR producer=${close.producer()}`];
    }
    case wire.ClientRequest.OpenEmitterRequest: {
      const open = member(message.request(new wire.OpenEmitterRequest()) as wire.OpenEmitterRequest | null);
      const lines = [`REQUEST ${id} OPEN_EMITTER domain=${open.domain()} emitter=${open.emitter()} batches=${open.maxOutstandingBatches()} bytes=${open.maxOutstandingBytes()}`];
      for (let index = 0; index < open.expectedFieldsLength(); index += 1) {
        lines.push(fieldLine('FIELD', readField(member(open.expectedFields(index)))));
      }
      return lines;
    }
    case wire.ClientRequest.ReadEmitterBatchRequest: {
      const read = member(message.request(new wire.ReadEmitterBatchRequest()) as wire.ReadEmitterBatchRequest | null);
      return [`REQUEST ${id} READ_EMITTER_BATCH consumer=${read.consumer()}`];
    }
    case wire.ClientRequest.SettleEmitterBatchRequest: {
      const settle = member(message.request(new wire.SettleEmitterBatchRequest()) as wire.SettleEmitterBatchRequest | null);
      const selected = member(settle.decision());
      const decision = selected === wire.EmitterBatchDecision.Retry ? 'retry'
        : selected === wire.EmitterBatchDecision.Reject ? `reject:${text(bytesOf((encoding) => settle.reason(encoding)))}`
          : 'ack';
      return [`REQUEST ${id} SETTLE_EMITTER_BATCH consumer=${settle.consumer()} reference=${hex(member(settle.referenceArray()))} decision=${decision}`];
    }
    case wire.ClientRequest.CloseEmitterRequest: {
      const close = member(message.request(new wire.CloseEmitterRequest()) as wire.CloseEmitterRequest | null);
      return [`REQUEST ${id} CLOSE_EMITTER consumer=${close.consumer()}`];
    }
    default:
      throw new Error(`the corpus holds no ${wire.ClientRequest[message.requestType()]} request`);
  }
}

/** Decodes every frame of the checked-in corpus and prints its report. */
function corpus(directory: string): void {
  const files = readdirSync(directory)
    .filter((file) =>
      ['.nxcm', '.nxsm', '.nxbq', '.nxbd', '.nxrm', '.nxrr'].some((extension) => file.endsWith(extension)),
    )
    .sort();
  const opened = new Uint8Array(readFileSync(join(directory, 'server_subscription_opened.nxsm')));
  const openedMessage = wire.ServerMessage.getRootAsServerMessage(frameBuffer(opened, 'NXSM'));
  const schema = openedSchema(member(openedMessage.body(new wire.Reply()) as wire.Reply | null));
  for (const file of files) {
    const frame = new Uint8Array(readFileSync(join(directory, file)));
    let lines: string[];
    if (file.endsWith('.nxcm')) {
      lines = clientLines(frame);
    } else if (file.endsWith('.nxbq')) {
      lines = downloadRequestLines(frame);
    } else if (file.endsWith('.nxbd')) {
      lines = downloadLines(frame);
    } else if (file.endsWith('.nxrm')) {
      lines = restoreLines(frame);
    } else if (file.endsWith('.nxrr')) {
      lines = restoreReplyLines(frame);
    } else {
      lines = serverLines(frame, schema);
    }
    report(`FRAME ${file}`);
    lines.forEach(report);
  }
}

const run = corpusMode ? Promise.resolve().then(() => corpus(process.argv[3] ?? '')) : main();

run.then(
  () => {
    clearTimeout(deadline);
    process.exit(0);
  },
  (error: unknown) => {
    process.stderr.write(`probe failed: ${error instanceof Error ? error.stack : String(error)}\n`);
    process.exit(1);
  },
);
