// The recorder a web console page starts with, before any script of its own runs.
//
// It notes every WebSocket the page constructs: the console it connects to, the path of the call it
// carries, when the page constructs it, sees it open, and lets go of it, by closing it or seeing it
// close, and the binary frames it receives, with when the page received each. One counter orders
// everything it notes, and only the page's own event loop advances it, so the record keeps the order
// the page acted in however late the scenario reads it. The page receives a frame before it can act
// on it, so a frame comes before every connection the page makes because of it.
//
// Of each connection it keeps the first FIRST_KEPT_FRAMES and the last LAST_KEPT_FRAMES binary
// frames of at most MAX_KEPT_FRAME_BYTES, so a busy page keeps a bounded record. A node reports which
// node leads at either end of a connection: the leader's report opens every session it serves and a
// download's redirect is its first frame, while the redirect that ends a session, a restore's reply
// and a command's redirect, after which the console lets the session go at once, come last. Such a
// report is far smaller than the bound; the larger frames, domain snapshots and archive chunks,
// report nothing about it.
//
// It can also close the newest connection the page holds at a path, as a connection lost under the
// console closes, and notes when it did.
(() => {
  const NativeWebSocket = window.WebSocket;
  const FIRST_KEPT_FRAMES = 16;
  const LAST_KEPT_FRAMES = 64;
  const MAX_KEPT_FRAME_BYTES = 16 * 1024;
  const connections = [];
  const losses = [];
  const entries = new WeakMap();
  let moment = 0;

  const next = () => {
    moment += 1;
    return moment;
  };

  const release = (entry) => {
    if (entry.released === null) {
      entry.released = next();
    }
  };

  // The console reads every frame as an ArrayBuffer; it never reads a text message as a frame.
  const keep = (frames, data) => {
    if (!(data instanceof ArrayBuffer) || data.byteLength > MAX_KEPT_FRAME_BYTES) {
      return;
    }
    const frame = { received: next(), bytes: new Uint8Array(data.slice(0)) };
    if (frames.first.length < FIRST_KEPT_FRAMES) {
      frames.first.push(frame);
      return;
    }
    frames.last.push(frame);
    if (frames.last.length > LAST_KEPT_FRAMES) {
      frames.last.shift();
    }
  };

  const readable = (frames) =>
    [...frames.first, ...frames.last].map(({ received, bytes }) => ({
      received,
      bytes: Array.from(bytes),
    }));

  class RecordedWebSocket extends NativeWebSocket {
    constructor(...parameters) {
      super(...parameters);
      // The origin and path name the console and the call. The query carries the credentials and
      // stays out of the record.
      const address = new URL(this.url);
      const entry = {
        address: `${address.protocol}//${address.host}`,
        path: address.pathname,
        opened: next(),
        connected: null,
        released: null,
      };
      const frames = { first: [], last: [] };
      entries.set(this, entry);
      connections.push({ entry, frames, socket: this });
      this.addEventListener("open", () => {
        if (entry.connected === null) {
          entry.connected = next();
        }
      });
      this.addEventListener("message", (event) => keep(frames, event.data));
      this.addEventListener("close", () => release(entry));
    }

    close(...parameters) {
      const entry = entries.get(this);
      if (entry !== undefined) {
        release(entry);
      }
      return super.close(...parameters);
    }
  }

  window.WebSocket = RecordedWebSocket;

  Object.defineProperty(window, "__nervixConsoleConnections", {
    value: Object.freeze({
      record: () => ({
        connections: connections.map(({ entry, frames }) => ({
          ...entry,
          frames: readable(frames),
        })),
        losses: [...losses],
      }),
      lose: (path) => {
        for (let index = connections.length - 1; index >= 0; index -= 1) {
          const { entry, socket } = connections[index];
          if (entry.path === path && entry.released === null) {
            losses.push(next());
            socket.close();
            return true;
          }
        }
        return false;
      },
    }),
  });
})();
