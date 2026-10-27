// Hold the session's first frames until the scenario releases them, preserving their receive order.
// The page still renders and accepts public clicks, so the fixture can establish whether its form
// opened before the initial domain publication. No application state or protocol value is changed.
((sessionPath) => {
  const RecordedWebSocket = window.WebSocket;
  const MAX_HELD_FRAMES = 64;
  const MAX_HELD_BYTES = 1024 * 1024;
  const pending = [];
  let heldBytes = 0;
  let held = true;
  let failure = null;

  class HeldInitialSession extends RecordedWebSocket {
    constructor(...parameters) {
      super(...parameters);
      if (new URL(this.url).pathname !== sessionPath) return;
      this.addEventListener('message', (event) => {
        if (!held) return;
        event.stopImmediatePropagation();
        if (!(event.data instanceof ArrayBuffer)) {
          failure = 'the initial session received a non-binary frame';
          return;
        }
        const bytes = event.data.byteLength;
        if (pending.length >= MAX_HELD_FRAMES || heldBytes + bytes > MAX_HELD_BYTES) {
          failure = 'the initial session exceeded its declared frame or byte bound';
          return;
        }
        pending.push({ socket: this, data: event.data });
        heldBytes += bytes;
      });
      this.addEventListener('close', () => {
        if (held) failure = 'the initial session closed before its frames were released';
      });
    }
  }

  window.WebSocket = HeldInitialSession;
  Object.defineProperty(window, '__nervixInitialSession', {
    value: Object.freeze({
      pending: () => {
        if (failure !== null) throw new Error(failure);
        return pending.length;
      },
      release: () => {
        if (failure !== null) throw new Error(failure);
        held = false;
        const released = pending.length;
        for (const { socket, data } of pending) {
          socket.dispatchEvent(new MessageEvent('message', { data }));
        }
        pending.length = 0;
        heldBytes = 0;
        return released;
      },
    }),
  });
})(__NERVIX_CONSOLE_SESSION_PATH__);
