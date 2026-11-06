// A bounded test peer at the public browser transport boundary. Decode only the schema fields
// needed to correlate a real command; the Rust codec constructs the split-transfer fixture.
(() => {
  const NativeWebSocket = window.WebSocket;
  const decoder = new TextDecoder();
  const MAX_RECORDED_ATTEMPTS = __NERVIX_MAX_RECORDED_ATTEMPTS__;
  let armed = null;
  let observation = null;
  const view = bytes => new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const field = (bytes, table, index) => {
    const data = view(bytes);
    const vtable = table - data.getInt32(table, true);
    const slot = 4 + index * 2;
    if (slot >= data.getUint16(vtable, true)) return 0;
    const offset = data.getUint16(vtable + slot, true);
    return offset === 0 ? 0 : table + offset;
  };
  const indirect = (bytes, offset) => offset + view(bytes).getUint32(offset, true);
  const text = (bytes, offset) => {
    const start = indirect(bytes, offset);
    const size = view(bytes).getUint32(start, true);
    return decoder.decode(bytes.subarray(start + 4, start + 4 + size));
  };
  const root = bytes => view(bytes).getUint32(0, true);


  class ReplyPeer extends NativeWebSocket {
    constructor(...parameters) {
      super(...parameters);
      this.fixtureRequest = null;
      this.fixtureSends = Promise.resolve();
      this.addEventListener('message', event => {
        if (armed === null || this.fixtureRequest === null || !(event.data instanceof ArrayBuffer)) return;
        const bytes = new Uint8Array(event.data);
        const restore = armed.kind === 'restore';
        const table = root(bytes);
        let reply = table;
        if (!restore) {
          const kind = field(bytes, table, 0);
          if (kind === 0 || bytes[kind] !== 1) return; // ServerBody::Reply
          reply = indirect(bytes, field(bytes, table, 1));
        }
        const requestId = field(bytes, reply, 0);
        if (requestId === 0 || view(bytes).getBigUint64(requestId, true) !== this.fixtureRequest.id) return;
        const bodyKind = field(bytes, reply, 1);
        if (bodyKind === 0 || bytes[bodyKind] !== 1) return; // CommandOutcome in either reply union
        const outcome = indirect(bytes, field(bytes, reply, 2));
        const disposition = field(bytes, outcome, 2);
        if (disposition === 0 || bytes[disposition] !== 1) return; // CommandCompleted
        const reference = field(bytes, outcome, 0);
        if (text(bytes, reference) !== this.fixtureRequest.reference) {
          throw new Error('the outcome does not name the request execution reference');
        }
        event.stopImmediatePropagation();
        const active = armed;
        armed = null; // redispatched message and every later reply pass unchanged
        observation.injected += 1;
        observation.injected_at = observation.references.length - 1;
        let replacement = bytes.slice();
        if (active.kind === 'split') {
          replacement = new Uint8Array(__NERVIX_TRANSFER_PART__);
          const reply = indirect(replacement, field(replacement, root(replacement), 1));
          view(replacement).setBigUint64(field(replacement, reply, 0), this.fixtureRequest.id, true);
        } else {
          const vector = indirect(replacement, reference);
          const offset = vector + 4;
          const length = view(replacement).getUint32(vector, true);
          replacement[offset] = restore ? (replacement[offset] === 97 ? 98 : 97) : 126;
          observation.foreign = restore ? decoder.decode(replacement.subarray(offset, offset + length)) : null;
        }
        this.dispatchEvent(new MessageEvent('message', { data: replacement.buffer }));
      });
    }
    send(data) {
      // gloo-net sends a Blob; wait for its bytes before sending it so the correlation exists
      // before the server can answer. No response is delayed or reordered.
      if (data instanceof Blob) {
        this.fixtureSends = this.fixtureSends.then(async () => this.send(await data.arrayBuffer()));
        return;
      }
      const bytes = new Uint8Array(data);
      const path = new URL(this.url).pathname;
      const active = armed ?? observation;
      if (active !== null && bytes.byteLength <= 1024 * 1024) {
        let command;
        let query;
        let reference;
        let id;
        const table = root(bytes);
        if (path === '/console/ws') {
          const kind = field(bytes, table, 1);
          if (kind !== 0 && bytes[kind] === 1) { // ClientRequest::CommandRequest
            command = indirect(bytes, field(bytes, table, 2));
            query = text(bytes, field(bytes, command, 0));
            reference = text(bytes, field(bytes, command, 2));
            id = view(bytes).getBigUint64(field(bytes, table, 0), true);
          }
        } else if (path === '/console/backups/restore' && bytes[field(bytes, table, 0)] === 1) {
          command = indirect(bytes, field(bytes, table, 1));
          query = text(bytes, field(bytes, command, 2));
          reference = text(bytes, field(bytes, command, 1));
          id = view(bytes).getBigUint64(field(bytes, command, 0), true);
        }
        if (query === active.query) {
          if (observation.references.length >= MAX_RECORDED_ATTEMPTS) throw new Error('the reply fixture exceeded its attempt bound');
          observation.references.push(reference);
          this.fixtureRequest = { id, reference };
        }
      }
      return super.send(data);
    }
  }
  window.WebSocket = ReplyPeer;
  window.__nervixUnreadableReply = Object.freeze({
    arm: (query, kind) => {
      armed = { query, kind };
      observation = { query, kind, references: [], injected: 0, injected_at: null, foreign: null };
    },
    observed: () => observation,
  });
})();
