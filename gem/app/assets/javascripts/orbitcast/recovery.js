// Orbitcast recovery helper.
//
// A plain ES module, importmap-ready: no build step, no bundler, no transpile.
// It uses only public `@rails/actioncable` APIs and is *never* injected into
// ordinary Turbo or ActionCable subscriptions - importing this file is the
// entire opt-in.
//
//   import { createRecovery } from "orbitcast/recovery"
//
//   const recovery = createRecovery(consumer, {
//     token: document.querySelector("meta[name=orbitcast-recovery]").content,
//     onMessage: ({ stream, payload, publicationId }) => render(stream, payload),
//     onGap: ({ stream, reason }) => location.reload()
//   })
//
// What it does:
//   1. subscribes to `Orbitcast::RecoveryChannel` with a Rails-minted token,
//   2. sends `resume` with the cursors it holds, on connect and on every
//      reconnect,
//   3. tracks the cursor of every delivery it receives,
//   4. acknowledges receipt periodically (best effort),
//   5. reports gaps so the application can resynchronize.
//
// Delivery semantics - read this before relying on it:
//
//   * **At-least-once, within retention.** The same publication can arrive
//     twice across a replay-to-live handoff. Deduplicate on `publicationId`;
//     this helper already drops repeats inside a bounded window, but an
//     application that must be exactly-once needs its own idempotency.
//   * **An ack is a receipt, not proof of effect.** It says bytes arrived, not
//     that the UI applied them. It never gates delivery.
//   * **A gap is real.** `onGap` means history the server cannot reconstruct:
//     it expired, it was trimmed, the stream's generation was wiped, another
//     tab won the fencing race, or live traffic outran the replay buffer. The
//     only correct response is an application-level resync (refetch the page,
//     the list, the document), never "ignore it".
//   * **Cursors are opaque.** Store and echo them; never parse or compare them.
//   * **Subscribe first, recover last.** The relay replays a stream only while
//     this connection holds a confirmed subscription to it (the token alone is
//     never enough), and it checks on every `resume`. Create the application
//     subscriptions before calling `createRecovery`; ActionCable confirms
//     subscriptions in creation order, on connect and on every reconnect, so
//     the order you create them in is the order the relay sees. A stream the
//     connection is not subscribed to arrives as an `unauthorized` gap.
//   * **No cursors means "where I left off".** A `resume` sent without any
//     cursors (a fresh page load that did not persist them) continues from the
//     last position this session acknowledged, as long as the relay still has
//     the session; otherwise it starts live. Persist `cursors()` to be exact.

const CHANNEL = "Orbitcast::RecoveryChannel";
const ACTION = "orbitcast_recovery";
const DELIVERY_TYPE = "orbitcast_delivery";
const CONTROL_TYPE = "orbitcast_recovery";
const DEFAULT_ACK_INTERVAL_MS = 5000;
const DEDUP_WINDOW = 2000;

/**
 * Subscribe to Orbitcast recovery on an existing ActionCable consumer.
 *
 * @param {object} consumer - a `@rails/actioncable` consumer
 * @param {object} options
 * @param {string} options.token - the Rails-minted recovery token
 * @param {function} [options.onMessage] - ({stream, payload, publicationId, cursor})
 * @param {function} [options.onGap] - ({stream, reason})
 * @param {function} [options.onReplayStart] - ({streams})
 * @param {function} [options.onReplayDone] - ({cursors})
 * @param {object} [options.cursors] - previously persisted {stream: cursor}
 * @param {number} [options.ackIntervalMs] - acknowledgement cadence
 * @returns {{subscription: object, cursors: function, unsubscribe: function}}
 */
export function createRecovery(consumer, options = {}) {
  const { token, cursors: initialCursors = {}, ackIntervalMs = DEFAULT_ACK_INTERVAL_MS } = options;
  if (!token) throw new Error("orbitcast/recovery: a recovery token is required");

  const cursors = { ...initialCursors };
  const seen = new Set();
  const order = [];
  let ackTimer = null;
  let acked = null;

  const call = (name, argument) => {
    const handler = options[name];
    if (typeof handler === "function") handler(argument);
  };

  // Bounded deduplication: the same publication may legitimately be delivered
  // twice across a replay-to-live handoff.
  const isNew = (publicationId) => {
    if (!publicationId) return true;
    if (seen.has(publicationId)) return false;
    seen.add(publicationId);
    order.push(publicationId);
    if (order.length > DEDUP_WINDOW) seen.delete(order.shift());
    return true;
  };

  const subscription = consumer.subscriptions.create({ channel: CHANNEL, token }, {
    // Called on connect *and* on every reconnect: resuming is what turns a
    // reconnect into "continue from my cursor" instead of "start from now".
    connected() {
      send("resume");
      if (ackIntervalMs > 0 && ackTimer === null) {
        ackTimer = setInterval(() => acknowledge(), ackIntervalMs);
      }
    },

    disconnected() {
      if (ackTimer !== null) {
        clearInterval(ackTimer);
        ackTimer = null;
      }
    },

    received(message) {
      if (!message || typeof message !== "object") return;

      if (message.type === DELIVERY_TYPE) {
        // The cursor is opaque: store it, never parse it.
        cursors[message.stream] = message.cursor;
        if (!isNew(message.publication_id)) return;

        let payload = null;
        try {
          // The payload travels as exact JSON *text*, so float and unicode
          // escaping survive the whole path unchanged.
          payload = JSON.parse(message.json_payload);
        } catch (error) {
          call("onGap", { stream: message.stream, reason: "undecodable_payload" });
          return;
        }

        call("onMessage", {
          stream: message.stream,
          payload,
          publicationId: message.publication_id,
          sequence: message.sequence, // a decimal string: JS numbers are too small
          generation: message.generation,
          cursor: message.cursor
        });
        return;
      }

      if (message.type !== CONTROL_TYPE) return;

      switch (message.event) {
        case "replay_start":
          call("onReplayStart", { streams: message.streams || [] });
          break;
        case "replay_done":
          call("onReplayDone", { cursors: { ...cursors } });
          break;
        case "gap":
          // History the server cannot reconstruct. Resynchronize; the cursor
          // is kept so live delivery continues from the current edge.
          call("onGap", { stream: message.stream, reason: message.reason });
          break;
        default:
          break; // An unknown event is never treated as success.
      }
    }
  });

  // Receipt only. Sent on a timer, skipped when nothing moved, and never
  // awaited: an unrecorded ack costs a longer replay, never a delivery.
  function acknowledge() {
    const encoded = JSON.stringify(cursors);
    if (encoded === acked) return;
    acked = encoded;
    send("ack");
  }

  // `subscription.perform(action, data)` is ActionCable's client API, and it
  // writes `data.action = action` before sending — so a call shaped
  // `perform("message", { action: ACTION, ... })` reaches the server as
  // `{"action":"message", ...}` and the relay never sees `orbitcast_recovery`.
  // The action name must therefore be the first argument; the remaining
  // fields are the v1 recovery command (`version`, `op`, `cursors`).
  function send(op) {
    subscription.perform(ACTION, { version: 1, op, cursors: { ...cursors } });
  }

  return {
    subscription,
    /** The cursors held right now; persist them to resume across page loads. */
    cursors: () => ({ ...cursors }),
    /** Force an acknowledgement (for example before unload). */
    acknowledge,
    unsubscribe() {
      if (ackTimer !== null) clearInterval(ackTimer);
      ackTimer = null;
      subscription.unsubscribe();
    }
  };
}

export default createRecovery;
