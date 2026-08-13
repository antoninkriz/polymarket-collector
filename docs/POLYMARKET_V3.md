# Polymarket orderbook v3

## Contract

V3 records the order observed by the collector before message expansion,
queueing, Redis transport, or ClickHouse batching. It supports exact replay of
the messages actually received for a market, subject to the upstream feed and
gap limitations below.

Polymarket's public market channel provides millisecond source timestamps but
no exchange sequence number and no unique public fill ID. A Polygon
transaction can settle multiple fills. Consequently, v3 never deduplicates
WebSocket events by transaction hash, content hash, timestamp, price, size, or
any combination of public payload fields. Two byte-identical messages received
from the authoritative socket are retained as two observations.

The public feed shapes used by this collector are documented in Polymarket's
[market stream reference](https://docs.polymarket.com/market-data/realtime-data#market-stream).

## Ingestion and ordering

Each binary market's two outcome assets are assigned to one authoritative
WebSocket connection. This preserves a parent `price_change` message containing
updates for both assets and avoids an impossible merge of redundant socket
deliveries.

The collector attaches these fields at the receive boundary:

| Field | Meaning |
|---|---|
| `collector_session_id` | UUID created once per collector process. |
| `collector_session_started_at` | UTC process-session start time, nanosecond precision. |
| `connection_id` | Stable socket-task identity within the collector session. |
| `connection_epoch` | Increments every time that socket reconnects. |
| `frame_sequence` | Monotonic text-frame order for that socket task, across reconnects. |
| `receive_sequence` | Monotonic parent-message merge order within the collector session. |
| `message_id` | Random UUID shared by all rows exploded from one parent message. |
| `message_index` / `message_count` | Parent position and count in a top-level JSON array frame. |
| `row_index` / `row_count` | Child position and count within the normalized parent message. |
| `timestamp_received` | UTC wall clock sampled immediately when tungstenite yields the text frame. |
| `raw_message` | Complete parent JSON object, repeated on its normalized child rows. |

`timestamp_received` is a real socket receive observation, represented as
`DateTime64(9, 'UTC')`. It includes kernel, TLS, and WebSocket-library delivery
time; it is not a hardware packet timestamp. It is not the ordering key because
wall clocks can step. All messages in the same WebSocket frame share the same
receive timestamp.

`receive_sequence` gives the collector's total merge order. The stricter
source-faithful order for any one connection is
`connection_epoch, frame_sequence, message_index, row_index`. There is no
exchange-defined total order between different WebSocket connections, so v3
does not claim one. Because both assets of a market stay on one connection,
this limitation does not prevent per-market book replay or per-market trade
delivery order.

## ClickHouse schema

The `polymarket_orderbook_v3` table contains:

- all ordering and provenance fields above;
- `schema_version` and the Redis `transport_id`;
- both parsed `timestamp` and lossless `timestamp_raw`;
- `market`, `event_type`, `asset_id`, and the Polymarket `hash` where supplied;
- typed depth arrays for `book`;
- nullable typed price, size, side, BBO, fee, transaction, and tick-size fields;
- the raw parent message for audit and forward-compatible reparsing.

The table uses `ReplacingMergeTree`. Its key retains every distinct collector
observation while collapsing retries of the same collector row. Hourly export
queries use `FINAL`, so Parquet never depends on background merge timing.

## Replay algorithm

For one market:

1. Order rows by `collector_session_started_at`, `collector_session_id`,
   `receive_sequence`, `message_index`, and `row_index`.
2. Group rows by `message_id`; validate the observed child indices against
   `row_count`.
3. Start or restart each asset only from a complete `book` snapshot. A new
   collector session, connection, or connection epoch is a gap boundary until
   the subscription snapshot for that asset arrives.
4. A `book` replaces the entire asset book. A `price_change` assigns the
   supplied size at `(side, price)`; size zero removes the level.
5. Apply all children of a parent in `row_index` order before exposing the next
   state. This preserves the atomic message boundary for downstream consumers.

The Parquet exporter uses that order directly. It does not group by
`asset_id`, `timestamp_received`, or Polymarket's source `timestamp`.

## Trades and deduplication

`last_trade_price` records use the same receive order as book messages. The
transaction hash is retained as data, not treated as an identifier. V3's only
deduplication identity is the collector-generated `(message_id, row_index)`;
it removes a retry created after socket receipt and cannot merge two genuine
feed deliveries.

Per-market trade delivery order is reconstructible inside a collector session.
Across different socket connections, v3 exposes the actual receive timestamp
and collector merge order but cannot manufacture an exchange sequence that
Polymarket did not publish.

## Durability and completeness boundaries

The publisher appends to Redis Streams and retries failed appends without
discarding its batch. The consumer leaves entries pending until ClickHouse
commits, and ClickHouse insert failures retry with backpressure. The exporter
waits until a later receive-time hour has committed before publishing an
immutable earlier hour.

These mechanisms protect against subscriber downtime and ordinary transient
Redis/ClickHouse failures. They do not turn the public feed into an exchange
audit log. Remaining explicit boundaries are:

- Polymarket supplies no source sequence or replay cursor;
- a socket disconnect has an unknowable gap until new snapshots arrive;
- malformed or unsupported server messages cannot be normalized (the raw
  frame should be captured separately if forward compatibility beyond the four
  supported event types is required);
- Redis AOF policy and host/process failure determine the crash-loss window;
- an overflowing pre-Redis process queue terminates the publisher; and
- no public manifest proves that Polymarket delivered every exchange event.

Consumers must surface these boundaries rather than replay across them as if
the book were continuous.
