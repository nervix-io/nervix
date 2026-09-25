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

function commandLines(id: bigint, outcome: wire.CommandOutcome): string[] {
  const name = DISPOSITIONS.get(outcome.dispositionType());
  const origin = outcome.origin();
  if (name === undefined || origin === null) {
    throw new Error('a command outcome lacks a declared disposition or its origin');
  }
  const lines = [
    `REPLY ${id} COMMAND ${name} reference=${outcome.executionReference()} origin=${wire.OutcomeOrigin[origin]} message=${text(bytesOf((encoding) => outcome.message(encoding)))}`,
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
  return lines;
}

function serverLines(frame: Uint8Array, schema: OpenedSchema): string[] {
  const message = wire.ServerMessage.getRootAsServerMessage(frameBuffer(frame, 'NXSM'));
  switch (message.bodyType()) {
    case wire.ServerBody.Reply: {
      const reply = member(message.body(new wire.Reply()) as wire.Reply | null);
      const id = reply.requestId();
      switch (reply.bodyType()) {
        case wire.ReplyBody.CommandOutcome:
          return commandLines(id, member(reply.body(new wire.CommandOutcome()) as wire.CommandOutcome | null));
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
    case wire.ClientRequest.CancelRequest: {
      const cancel = member(message.request(new wire.CancelRequest()) as wire.CancelRequest | null);
      return [`REQUEST ${id} CANCEL target=${cancel.targetRequestId()}`];
    }
    default:
      throw new Error(`the corpus holds no ${wire.ClientRequest[message.requestType()]} request`);
  }
}

/** Decodes every frame of the checked-in corpus and prints its report. */
function corpus(directory: string): void {
  const files = readdirSync(directory)
    .filter((file) => file.endsWith('.nxcm') || file.endsWith('.nxsm'))
    .sort();
  const opened = new Uint8Array(readFileSync(join(directory, 'server_subscription_opened.nxsm')));
  const openedMessage = wire.ServerMessage.getRootAsServerMessage(frameBuffer(opened, 'NXSM'));
  const schema = openedSchema(member(openedMessage.body(new wire.Reply()) as wire.Reply | null));
  for (const file of files) {
    const frame = new Uint8Array(readFileSync(join(directory, file)));
    const lines = file.endsWith('.nxcm') ? clientLines(frame) : serverLines(frame, schema);
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
