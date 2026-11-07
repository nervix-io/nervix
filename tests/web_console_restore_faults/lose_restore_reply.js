// A fault a scenario installs on a console page once the page is open: the page's next restore
// attempt loses its reply, after which a later attempt cannot read the archive or receives a
// valid unrecorded failure encoded by the scenario's production Rust wire codec.
//
// The fault takes a reply before the console's own handler sees it, retains its bytes and closes
// the connection. In unreadable mode the first reply is lost and every later file read fails.
// In unrecorded mode Rust decodes each lost candidate: after a valid uncertain outcome it can
// request that the next held reply be lost instead, until it observes a real completed restore.
// The archive stays readable and the original start is retained without delaying its send.
// Exactly one reply after that completion is replaced by the supplied typed failure. Subsequent
// replies pass through unchanged. No frame sent to a node is changed.
//
// It holds back at most MAX_HELD_MESSAGES messages, retains one bounded start and replacement,
// and records what went wrong in itself, so a scenario never mistakes a broken fault for the
// console's behavior.
({ restorePath, laterFailure, replyFrameBytes }) => {
  if (window.__nervixRestoreFault !== undefined) {
    throw new Error("a page installs the restore fault once");
  }
  const BaseWebSocket = window.WebSocket;
  const nativeArrayBuffer = Blob.prototype.arrayBuffer;
  const MAX_HELD_MESSAGES = 16;
  const MAX_START_BYTES = 1024 * 1024;
  if (!Number.isSafeInteger(replyFrameBytes) || replyFrameBytes <= 0) {
    throw new Error("the fixture needs the normal restore reply frame bound");
  }
  const state = {
    lostReply: null,
    lostReplies: 0,
    start: null,
    substitutedFailures: 0,
    restoreConnections: 0,
    failedReads: 0,
    heldMessages: 0,
    failure: null,
  };
  let waitingSocket = null;
  let waitingReply = null;
  let loseNext = false;
  let replacement = null;

  const substituteFailure = () => {
    if (waitingSocket === null || (!loseNext && replacement === null)) return;
    const socket = waitingSocket;
    const reply = waitingReply;
    waitingSocket = null;
    waitingReply = null;
    if (loseNext) {
      loseNext = false;
      loseReply(socket, reply);
      return;
    }
    state.substitutedFailures += 1;
    socket.dispatchEvent(new MessageEvent("message", {
      data: new Uint8Array(replacement).buffer,
    }));
  };

  // The console reads every frame as an ArrayBuffer; a restore connection carries one reply.
  const loseReply = (socket, data) => {
    if (socket.replyLostByFault === true) {
      state.failure = "the connection whose reply was lost received a second message";
      return;
    }
    if (!(data instanceof ArrayBuffer) || data.byteLength > replyFrameBytes) {
      state.failure = "the lost restore reply was not a bounded binary message";
      return;
    }
    socket.replyLostByFault = true;
    state.lostReply = Array.from(new Uint8Array(data));
    state.lostReplies += 1;
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
      this.recordsStart = losesReply && laterFailure === "unrecorded";
      // Added before the console sets its own handler, so it runs first and keeps the message
      // from it.
      this.addEventListener("message", (event) => {
        if (laterFailure === "unrecorded" && state.substitutedFailures > 0) return;
        event.stopImmediatePropagation();
        if (losesReply) {
          loseReply(this, event.data);
        } else {
          holdBack();
          if (state.failure !== null) return;
          if (laterFailure === "unrecorded") {
            if (waitingSocket !== null) {
              state.failure = "a later restore connection received a second reply";
              return;
            }
            if (!(event.data instanceof ArrayBuffer) || event.data.byteLength > replyFrameBytes) {
              state.failure = "a held restore reply was not a bounded binary message";
              return;
            }
            waitingSocket = this;
            waitingReply = event.data;
            substituteFailure();
          }
        }
      });
    }

    send(data) {
      if (this.recordsStart) {
        this.recordsStart = false;
        // Observe only the first frame, without delaying or reordering the real send.
        const bytes = data instanceof Blob
          ? nativeArrayBuffer.call(data)
          : Promise.resolve(data);
        bytes.then((buffer) => {
          if (buffer.byteLength > MAX_START_BYTES) {
            state.failure = "the restore start exceeded the fixture's frame bound";
            return;
          }
          state.start = Array.from(new Uint8Array(buffer));
        }).catch((error) => { state.failure = String(error); });
      }
      return super.send(data);
    }
  }

  window.WebSocket = LosingRestoreReply;

  Blob.prototype.arrayBuffer = function arrayBuffer(...parameters) {
    if (laterFailure !== "unreadable" || state.lostReply === null) {
      return nativeArrayBuffer.apply(this, parameters);
    }
    state.failedReads += 1;
    return Promise.reject(
      new DOMException("the archive file can no longer be read", "NotReadableError"),
    );
  };

  Object.defineProperty(window, "__nervixRestoreFault", {
    value: Object.freeze({
      loseNextReply: (lostReplies) => {
        if (laterFailure !== "unrecorded" || lostReplies !== state.lostReplies ||
            state.lostReply === null || loseNext || replacement !== null) {
          throw new Error("the fixture advances one observed lost reply at a time");
        }
        loseNext = true;
        substituteFailure();
      },
      refuseNext: (bytes) => {
        if (laterFailure !== "unrecorded" || loseNext || replacement !== null || bytes.length > MAX_START_BYTES) {
          throw new Error("the fixture accepts one bounded unrecorded failure");
        }
        replacement = bytes;
        substituteFailure();
      },
      record: () => ({
        ...state,
        lostReply: state.lostReply === null ? null : [...state.lostReply],
        start: state.start === null ? null : [...state.start],
      }),
    }),
  });
  return true;
}
