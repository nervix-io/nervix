// Observe the page's real session frames without changing their delivery. All retained data is
// bounded, and an incomplete observation fails instead of silently dropping its evidence.
(() => {
  const NativeWebSocket = window.WebSocket;
  const setTimer = window.setTimeout;
  const clearTimer = window.clearTimeout;
  const frames = [];
  const timers = new Map();
  const MAX_FRAMES = 1024;
  const MAX_BYTES = 4 * 1024 * 1024;
  let bytes = 0;
  let failure = null;

  const keep = (direction, data) => {
    const blob = data instanceof Blob ? data : null;
    const view = data instanceof ArrayBuffer
      ? new Uint8Array(data)
      : ArrayBuffer.isView(data)
        ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
        : null;
    if (blob === null && view === null) {
      failure = `the request-window observer received nonbinary data: ${Object.prototype.toString.call(data)}`;
      return;
    }
    const size = blob === null ? view.byteLength : blob.size;
    if (frames.length >= MAX_FRAMES) {
      failure = `the request-window observation exceeded ${MAX_FRAMES} frames`;
      return;
    }
    if (bytes + size > MAX_BYTES) {
      failure = `the request-window observation exceeded ${MAX_BYTES} bytes: kept=${bytes}, next=${size}`;
      return;
    }
    bytes += size;
    // gloo-net sends binary payloads as Blobs. Reserve the observation's position now, before
    // copying the immutable Blob asynchronously, so a reply cannot pass its request in the log.
    const contents = blob === null
      ? Promise.resolve(Array.from(view))
      : blob.arrayBuffer().then(buffer => Array.from(new Uint8Array(buffer)), () => {
        failure = 'the request-window observer could not read a binary Blob';
        return [];
      });
    frames.push(contents.then(bytes => ({ direction, bytes })));
  };

  class ObservedWebSocket extends NativeWebSocket {
    constructor(...parameters) {
      super(...parameters);
      if (new URL(this.url).pathname === __NERVIX_SESSION_PATH__) {
        // Register before the application: its reply continuation may send the next request
        // at the microtask checkpoint between message listeners.
        this.addEventListener('message', event => keep('received', event.data));
      }
    }

    send(data) {
      const result = super.send(data);
      if (new URL(this.url).pathname === __NERVIX_SESSION_PATH__) keep('sent', data);
      return result;
    }
  }
  window.WebSocket = ObservedWebSocket;

  // The input schedules its real debounce callback. Waiting for the callbacks already scheduled
  // by that interaction establishes that it issued its lookup; no test guesses a debounce delay.
  window.setTimeout = function (callback, delay, ...args) {
    if (typeof callback !== 'function') return setTimer.call(this, callback, delay, ...args);
    let complete;
    const done = new Promise(resolve => { complete = resolve; });
    const id = setTimer.call(this, function (...values) {
      try { return callback.apply(this, values); }
      finally { timers.delete(id); complete(); }
    }, delay, ...args);
    timers.set(id, { done, complete });
    return id;
  };
  window.clearTimeout = function (id) {
    const pending = timers.get(id);
    if (pending !== undefined) {
      timers.delete(id);
      pending.complete();
    }
    return clearTimer.call(this, id);
  };

  Object.defineProperty(window, '__nervixRequestWindow', {
    value: Object.freeze({
      record: async () => {
        const complete = await Promise.all(frames);
        if (failure !== null) throw new Error(failure);
        return complete;
      },
      settle: async () => {
        await Promise.all(Array.from(timers.values(), timer => timer.done));
        // Let the browser run the continuations those completed callbacks made runnable.
        await new Promise(resolve => setTimer.call(window, resolve, 0));
      },
    }),
  });
})();
