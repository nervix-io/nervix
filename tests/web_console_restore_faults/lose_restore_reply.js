// A fault a scenario installs on a console page once the page is open: the page's next restore
// attempt loses its reply, and the archive file can no longer be read after that.
//
// The first restore connection the page opens once the fault is installed keeps the leader's reply
// from the console. The fault takes the reply before the console's own handler sees it, keeps its
// bytes for the scenario, and closes the connection, as a connection lost before its reply arrived
// closes. From then on every read of a file's bytes fails, as a read of a file the browser can no
// longer read fails, and every message a later restore connection receives is held back, so no
// later attempt learns the restore's outcome before its read fails. The fault changes no frame and
// sends nothing.
//
// It holds at most MAX_HELD_MESSAGES messages and records what went wrong in itself instead of
// guessing, so a scenario reading its record never mistakes a broken fault for the console's
// behavior.
(restorePath) => {
  if (window.__nervixRestoreFault !== undefined) {
    throw new Error("a page installs the restore fault once");
  }
  const BaseWebSocket = window.WebSocket;
  const nativeArrayBuffer = Blob.prototype.arrayBuffer;
  const MAX_HELD_MESSAGES = 16;
  const state = {
    lostReply: null,
    restoreConnections: 0,
    failedReads: 0,
    heldMessages: 0,
    failure: null,
  };

  // The console reads every frame as an ArrayBuffer; a restore connection carries one reply.
  const loseReply = (socket, data) => {
    if (state.lostReply !== null) {
      state.failure = "the connection whose reply was lost received a second message";
      return;
    }
    if (!(data instanceof ArrayBuffer)) {
      state.failure = "the lost restore reply was not a binary message";
      return;
    }
    state.lostReply = Array.from(new Uint8Array(data));
    socket.close();
  };

  const holdBack = () => {
    if (state.heldMessages >= MAX_HELD_MESSAGES) {
      state.failure = "a later restore connection received more messages than the fault holds";
      return;
    }
    state.heldMessages += 1;
  };

  class LosingRestoreReply extends BaseWebSocket {
    constructor(...parameters) {
      super(...parameters);
      if (new URL(this.url).pathname !== restorePath) {
        return;
      }
      state.restoreConnections += 1;
      const losesReply = state.restoreConnections === 1;
      // Added before the console sets its own handler, so it runs first and keeps the message
      // from it.
      this.addEventListener("message", (event) => {
        event.stopImmediatePropagation();
        if (losesReply) {
          loseReply(this, event.data);
        } else {
          holdBack();
        }
      });
    }
  }

  window.WebSocket = LosingRestoreReply;

  Blob.prototype.arrayBuffer = function arrayBuffer(...parameters) {
    if (state.lostReply === null) {
      return nativeArrayBuffer.apply(this, parameters);
    }
    state.failedReads += 1;
    return Promise.reject(
      new DOMException("the archive file can no longer be read", "NotReadableError"),
    );
  };

  Object.defineProperty(window, "__nervixRestoreFault", {
    value: Object.freeze({
      record: () => ({
        ...state,
        lostReply: state.lostReply === null ? null : [...state.lostReply],
      }),
    }),
  });
  return true;
}
