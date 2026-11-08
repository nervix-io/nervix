// Hold one replacement session's outgoing requests until the scenario changes the graph.
// The existing incoming-frame gate is independent and keeps its original behavior.
((sessionPath) => {
  const RecordedWebSocket = window.WebSocket;
  const MAX_HELD_REQUESTS = 64;
  const MAX_HELD_BYTES = 1024 * 1024;
  let armed = false;
  let heldSocket = null;
  let heldBytes = 0;
  let failure = null;
  const requests = [];

  class HeldSessionRequests extends RecordedWebSocket {
    constructor(...parameters) {
      super(...parameters);
      if (armed && new URL(this.url).pathname === sessionPath) {
        armed = false;
        heldSocket = this;
        this.addEventListener('close', () => {
          if (heldSocket === this) failure = 'the held request session closed before release';
        });
      }
    }

    send(data) {
      if (this !== heldSocket || this.readyState !== RecordedWebSocket.OPEN) return super.send(data);
      let copy;
      let size;
      if (data instanceof Blob) {
        copy = data;
        size = data.size;
      } else if (data instanceof ArrayBuffer) {
        copy = data.slice(0);
        size = copy.byteLength;
      } else if (ArrayBuffer.isView(data)) {
        copy = new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice();
        size = copy.byteLength;
      } else {
        failure = 'the held session sent a non-binary request';
        return;
      }
      if (requests.length >= MAX_HELD_REQUESTS || heldBytes + size > MAX_HELD_BYTES) {
        failure = 'the held session exceeded its request or byte bound';
        return;
      }
      requests.push(copy);
      heldBytes += size;
    }

    releaseRequests() {
      for (const request of requests) super.send(request);
    }
  }

  window.WebSocket = HeldSessionRequests;
  Object.defineProperty(window, '__nervixSessionRequests', {
    value: Object.freeze({
      holdNext: () => {
        if (armed || heldSocket !== null || requests.length !== 0) {
          throw new Error('an outgoing session request gate is already armed');
        }
        failure = null;
        armed = true;
      },
      pending: () => {
        if (failure !== null) throw new Error(failure);
        return requests.length;
      },
      release: () => {
        if (failure !== null) throw new Error(failure);
        if (heldSocket === null) throw new Error('no outgoing request session is held');
        const released = requests.length;
        heldSocket.releaseRequests();
        heldSocket = null;
        requests.length = 0;
        heldBytes = 0;
        return released;
      },
    }),
  });
})(__NERVIX_CONSOLE_SESSION_PATH__);
