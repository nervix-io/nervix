// The recorder a web console page starts with, before any script of its own runs.
//
// It notes every WebSocket the page constructs: the console it connects to, the path of the call it
// carries, and when the page constructs it, sees it open, and lets go of it, by closing it or seeing
// it close. One counter orders everything it notes, and only the page's own event loop advances it,
// so the record keeps the order the page acted in however late the scenario reads it.
//
// It can also close the newest connection the page holds at a path, as a connection lost under the
// console closes, and notes when it did.
(() => {
  const NativeWebSocket = window.WebSocket;
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
      entries.set(this, entry);
      connections.push({ entry, socket: this });
      this.addEventListener("open", () => {
        if (entry.connected === null) {
          entry.connected = next();
        }
      });
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
        connections: connections.map(({ entry }) => ({ ...entry })),
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
