# MySQL Source Connector

The MySQL source connector fetches data from MySQL databases and streams it to Iggy topics. It supports incremental table polling with flexible payload extraction.

> **Note:** Only polling mode is available. Binlog CDC support is planned for a future release.

## Features

- **Table Polling**: Incrementally fetch data from MySQL tables using a tracking column
- **Flexible Payload Extraction**: Extract BLOB, TEXT, or JSON columns directly as payload
- **Custom Queries**: Use custom SQL queries with parameter substitution
- **Delete After Read**: Automatically delete rows after processing
- **Mark as Processed**: Mark rows as processed using a boolean column
- **Multiple Tables**: Monitor multiple tables simultaneously
- **Batch Processing**: Fetch data in configurable batch sizes
- **Offset Tracking**: Resume incremental polling from the last processed tracking value (see [Delivery semantics](#delivery-semantics))

## Configuration

```toml
[plugin_config]
connection_string = "mysql://user:pass@localhost:3306/database"
tables = ["users", "orders"]
poll_interval = "1s"
batch_size = 1000
tracking_column = "id"
initial_offset = "0"
max_connections = 10
snake_case_columns = false
include_metadata = true

# Payload extraction (optional)
payload_column = "payload"
payload_format = "bytea"

# Delete/mark processed (optional)
delete_after_read = false
processed_column = "is_processed"
primary_key_column = "id"

# Custom query (optional)
custom_query = "SELECT * FROM $table WHERE id > $offset ORDER BY id LIMIT $limit"
```

## Configuration Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `connection_string` | string | required | MySQL connection string (`mysql://user:pass@host:3306/db`) |
| `tables` | array | required | List of tables to monitor |
| `poll_interval` | string | `10s` | How often to poll (e.g., `1s`, `5m`) |
| `batch_size` | u32 | `1000` | Max rows per poll; must be greater than 0 |
| `tracking_column` | string | required | Column for incremental polling; must be unique and monotonically increasing (see [Tracking Column Requirements](#tracking-column-requirements)) |
| `initial_offset` | string | none | Starting value for tracking column |
| `max_connections` | u32 | `10` | Max database connections |
| `snake_case_columns` | bool | `false` | Convert column names to snake_case |
| `include_metadata` | bool | `true` | Wrap results with metadata envelope |
| `payload_column` | string | none | Column to extract directly as payload |
| `payload_format` | string | `json` (invalid if `payload_column` is set — `payload_format` is required in that case) | Format of payload_column: `bytea`, `text`, or `json_direct` |
| `delete_after_read` | bool | `false` | Delete rows after reading |
| `processed_column` | string | none | Boolean column to mark as processed |
| `primary_key_column` | string | tracking_column | PK for delete/mark operations |
| `custom_query` | string | none | Custom SQL with parameter substitution |
| `verbose_logging` | bool | `false` | Log at info level instead of debug |
| `max_retries` | u32 | `3` | Retries after the initial attempt for transient errors (3 = 4 total attempts) |
| `retry_delay` | string | `1s` | Base delay between retries (e.g., `500ms`, `2s`) |

## Tracking Column Requirements

Polling is **insert-only**. Each poll runs roughly `SELECT ... WHERE tracking_column > last_offset ORDER BY tracking_column ASC LIMIT batch_size` and stores the largest tracking value it saw as the next offset. For this to be lossless the **`tracking_column` must be unique and monotonically increasing** — an auto-increment primary key is the canonical choice.

`tracking_column` is required. It is the single field that decides whether polling loses rows, and the connector cannot check the choice for you: on the built-in query it reads the column's type and nullability but not the schema's intent, and a `custom_query` is substituted as text and never parsed, so nothing can confirm that the column you named is the one the query orders by.

**On upgrade:** the field used to default to `id`. A deployment that relied on that default now fails at startup with an `InitError` naming the field; add `tracking_column = "id"` to restore the previous behavior, or take the opportunity to check that `id` really is unique and monotonically increasing in the order MySQL sorts it (a `CHAR(36)` UUIDv4 `id` passes every startup check and silently drops most of the table).

The requirement is not cosmetic: if more than `batch_size` rows share the same tracking value, only the first `batch_size` are read and the next poll's `>` filter skips the rest of that value permanently. The same skip applies to `processed_column` and `delete_after_read`, because the tracking filter runs before those.

For this reason a mutable, low-resolution column such as `updated_at` is **not** a safe tracking column: many rows commonly share the same second, and rows updated in place are never re-read. Capturing updates to existing rows is out of scope for polling.

Every row the connector publishes has to carry a usable value for that column, because the value is the cursor. Two ways that fails, both checked on every poll and against the rows themselves, so a `custom_query` is held to them just as the built-in query is:

- **The result set does not contain the column.** A query that never projects it, or projects it under another name — `SELECT id AS row_id` returns `row_id`, not `id` — leaves no row able to move the cursor, so the same result set would be published on every poll forever. The batch is rejected before anything is published and the table is **disabled until the connector restarts**: the query cannot start returning the column on its own. The error lists the columns the query did return.
- **A row's value is NULL, or of a type with no ordered scalar form** (`tinyint(1)`, `JSON`). The batch is rejected the same way, but the table **stays enabled**, since an `UPDATE` can repair the row and the next poll will then pick it up. Until that happens the table publishes nothing and the error repeats every poll — it is a stall, not a transient.

Neither of the two loses anything: nothing is published, no row is marked or deleted, and the stored offset does not move.

The name is matched the way MySQL matches identifiers, case-insensitively, so `tracking_column = "ID"` and a column declared `id` are the same column. An alias still is not. `primary_key_column` and `payload_column` are matched the same way.

At startup the connector also reads the column from `information_schema` and refuses to start on a type that cannot carry a cursor, whether or not a `custom_query` is set — a projection cannot change a column's type. Rejected types and why:

- `JSON` and `tinyint(1)` have no ordered scalar form, so no row would ever produce a cursor.
- `BINARY`, `VARBINARY` and the `BLOB` family are carried as base64 text, which does not order the way the underlying bytes do. The cursor would be compared in one order while `ORDER BY` used another, and the base64 literal would be compared against raw bytes. `BINARY(16)` UUID keys are the common case here; track an auto-increment key or a `CHAR(36)` column instead.
- `ENUM` and `SET` are sorted by MySQL on declaration index, not on the label the connector sees, so the two disagree whenever the members are not declared in alphabetical order. A bounded set of labels cannot be unique per row either.

A nullable column is refused only when the connector builds its own query. With a `custom_query` it is a warning instead, because a query moves nullability in both directions: a `LEFT JOIN` introduces NULLs into a `NOT NULL` column, `WHERE ts IS NOT NULL` removes them from a nullable one. The per-poll check above catches an actual NULL.

## Output Modes

### JSON Mode (Default)

When `payload_column` is not set, each row is wrapped in a `DatabaseRecord` JSON structure:

```json
{
  "table_name": "users",
  "operation_type": "SELECT",
  "timestamp": "2024-01-15T10:30:00Z",
  "data": {
    "id": 123,
    "name": "John Doe",
    "email": "john@example.com"
  },
  "old_data": null
}
```

The stream config should use `schema = "json"`.

When `include_metadata = false`, the envelope is omitted and the row columns are serialized directly:

```json
{
  "id": 123,
  "name": "John Doe",
  "email": "john@example.com"
}
```

### Payload Column Extraction

When `payload_column` is set, the connector extracts that column directly as the Iggy message payload. The `payload_format` option determines how the column is read:

| Format | Column Type | Schema | Description |
| ------ | ----------- | ------ | ----------- |
| `bytea` / `raw` | `BLOB`, `BINARY`, `VARBINARY` | `raw` | Raw bytes passthrough |
| `text` | `TEXT`, `VARCHAR` | `text` | UTF-8 text |
| `json_direct` / `jsonb` | `JSON` | `json` | JSON object serialized to bytes |

The column must be present in every polled result set. If it is missing (a typo, a dropped column, or a `custom_query` that does not project it), the connector fails that table each cycle and logs the table and column instead of falling back to whole-row JSON, which would publish bytes contradicting the configured schema. The rows stay in MySQL and are picked up once the config is corrected.

## Payload Format Examples

### BLOB (Raw Bytes)

Extract raw bytes from a BLOB column:

```sql
CREATE TABLE message_queue (
    id INT AUTO_INCREMENT PRIMARY KEY,
    payload BLOB NOT NULL
);
```

```toml
[[streams]]
stream = "messages"
topic = "queue"
schema = "raw"
batch_length = 100

[plugin_config]
tables = ["message_queue"]
tracking_column = "id"
payload_column = "payload"
payload_format = "bytea"
```

### TEXT

Extract text from a TEXT column:

```sql
CREATE TABLE logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    message TEXT NOT NULL
);
```

```toml
[[streams]]
stream = "logs"
topic = "app_logs"
schema = "text"
batch_length = 100

[plugin_config]
tables = ["logs"]
tracking_column = "id"
payload_column = "message"
payload_format = "text"
```

### JSON (Direct)

Extract a JSON column directly as JSON payload (without `DatabaseRecord` wrapper):

```sql
CREATE TABLE events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON NOT NULL
);
```

```toml
[[streams]]
stream = "events"
topic = "user_events"
schema = "json"
batch_length = 100

[plugin_config]
tables = ["events"]
tracking_column = "id"
payload_column = "data"
payload_format = "json_direct"
```

## Custom Query Parameters

When using `custom_query`, these placeholders are available:

| Placeholder | Replaced With |
| ----------- | ------------- |
| `$table` | Current table name (required when more than one table is configured) |
| `$offset` | Last processed offset (or `initial_offset`) |
| `$limit` | `batch_size` value |
| `$now` | Current UTC timestamp (RFC3339) |
| `$now_unix` | Current Unix timestamp (seconds) |

The query runs once per entry in `tables`, so with more than one table configured it must contain `$table`. A static query would otherwise execute unchanged for every table and publish the same rows repeatedly, each copy carrying a different table's metadata, message ID, and tracking offset. The connector rejects that combination at startup.

A query that hardcodes its table still needs that table listed in `tables`. The list is what the connector polls — an empty list means it polls nothing — and the name is also the offset key, the input to the deterministic message ID, and the `table_name` metadata field.

The query must also project `tracking_column` under exactly that name, with a non-null scalar value in every row, and should filter on `$offset` — a query that does not re-reads the same rows on every poll by design. See [Tracking Column Requirements](#tracking-column-requirements) for what happens when it does not.

### Ordering is enforced

If the query uses `$offset`, it must return rows ordered ascending by the tracking column (`ORDER BY <tracking_column>`; MySQL sorts ascending by default, so `ASC` doesn't need to be spelled out). The connector takes the tracking value of the *last row returned* as the next `$offset`, which is only the maximum when the batch is ascending.

The connector verifies this on the rows themselves rather than by inspecting your SQL. If a row's
tracking value is lower than the previous row's, the poll for that table is abandoned: no messages
are published, no rows are marked or deleted, and the stored offset is not advanced.

Because the same query returns the same misordered rows every cycle, the table is then **disabled
until the connector is restarted**, and the error names the table, the tracking column, and the two
offending values. Other configured tables keep running.

The check also covers the gap between polls. A batch is compared against the offset it resumed from,
so a batch that is ascending within itself but reaches back behind its own cursor is caught too —
without which a `batch_size` of `1`, or any table quiet enough to yield one row per poll, would slip
past unchecked. That case is logged and the cycle skipped, but the table stays enabled, because
unlike a misordered query it can come from the data rather than the SQL and resolve on its own. It is
only applied when the cursor is what limits the rows a poll can see: the built-in query always
filters on it, a `custom_query` only if it interpolates `$offset`.

When the cause *is* the query — an `$offset` filter applied to a column other than the tracking
column, say — the warning repeats every poll and the table publishes nothing until the query is
corrected. It is a stall, not a transient.

Checking the rows rather than the query text matters, because these all *look* ordered and are not:

```sql
-- ORDER BY inside a derived table; MySQL discards it when the outer query doesn't need it
SELECT * FROM (SELECT * FROM $table WHERE id > $offset ORDER BY id) t LIMIT $limit

-- ORDER BY confined to a CTE
WITH recent AS (SELECT * FROM $table ORDER BY id) SELECT * FROM recent LIMIT $limit

-- window ordering, which does not order the result set
SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM $table LIMIT $limit

-- ascending and explicit, but by the wrong column when tracking_column is `id`
SELECT * FROM $table WHERE id > $offset ORDER BY created_at ASC LIMIT $limit
```

The column you order by is your effective tracking column, so the same [Tracking Column Requirements](#tracking-column-requirements) apply: it must be unique and monotonically increasing, or rows sharing a value can be skipped across a batch boundary.

How ordering is judged depends on the tracking column's SQL type. Offsets are carried as strings, so
an `INT` `10` and a `VARCHAR` `"10"` are indistinguishable by content, while MySQL sorts them
differently: numeric columns order by value (`9` before `10`), string and temporal columns order by
collation (`"10"` before `"5"`).

The type is read from the result set the query returns, which also settles it for a `custom_query`
projecting a computed column that has no `information_schema` row of its own, and for a table that
did not exist when the connector started. `information_schema` is still consulted at startup, for the
`NOT NULL` and column-type checks above.

The result set only arrives after the query has run, so when startup could not resolve the column the
first poll is built from that unresolved answer. If the type the result set reports would have
written the `$offset` literal differently — a bare `200` against what turns out to be a `VARCHAR`
column, which MySQL compares numerically while `ORDER BY` compares by collation — that batch is
discarded: nothing is published, no row is marked or deleted, and the offset does not advance. The
corrected type is already stored, so the next poll rebuilds the query and reads the same rows under a
filter that agrees with its own ordering. It is logged once per occurrence and does not repeat.

For collation-ordered columns a decrease is only reported when case-sensitive and case-insensitive
comparisons agree, since the collation itself is not known (`utf8mb4_0900_ai_ci` orders `a` before
`B`, `utf8mb4_bin` orders `B` first). Non-ASCII values are always treated as ambiguous, because
accent-insensitive and language-specific collations sort them in ways a byte comparison cannot
reproduce.

The type also decides how the offset is written into the query. A collation-ordered column always
gets a quoted literal, even when its values look numeric: MySQL comparing a string column against a
bare number converts both sides to a double, which would filter numerically while `ORDER BY` sorts by
collation. A cursor picked from a collation-ordered batch and fed back into a numeric filter strands
every row below the numeric high-water mark permanently, and the implicit conversion also stops the
column's index from being used.

**On upgrade:** a deployment already tracking a text column that holds digit-like values has a stored
offset that was chosen under the old numeric comparison. The first poll after upgrading filters by
collation instead, so rows between the collation position of that offset and the numeric high-water
mark are read again and republished once. That is duplication, not loss, and it is within the
[at-least-once](#delivery-semantics) contract the connector already provides, but expect the burst.

Example:

```sql
SELECT * FROM $table
WHERE id > $offset
  AND (scheduled_at IS NULL OR scheduled_at <= '$now')
ORDER BY id
LIMIT $limit
```

## Delete After Read / Mark as Processed

### Delete After Read

Deletes rows from the source table after successful processing. At-most-once — see [Delivery semantics](#delivery-semantics) below.

```toml
[plugin_config]
delete_after_read = true
primary_key_column = "id"
```

### Mark as Processed

Updates a boolean column instead of deleting. At-most-once — see [Delivery semantics](#delivery-semantics) below.

```toml
[plugin_config]
processed_column = "is_processed"
primary_key_column = "id"
```

Your table needs the boolean column:

```sql
ALTER TABLE users ADD COLUMN is_processed BOOLEAN DEFAULT false;
```

When `processed_column` is set, the connector automatically adds a `WHERE is_processed = FALSE` filter to the polling query, so only unprocessed rows are fetched.

### Delivery semantics

Both `delete_after_read` and `processed_column` mutate MySQL during the poll,
before the batch is actually sent to Iggy. Only one mutation runs per row: with
`delete_after_read = true` the row is deleted, otherwise (`processed_column`
set) it is marked processed — the two are mutually exclusive, and
`delete_after_read` wins if both are configured. If the connector crashes or the
send fails after that mutation but before the batch reaches Iggy, the row is
already deleted or already marked and won't be retried. So both options are
at-most-once, not at-least-once.

Plain offset tracking (no `delete_after_read` / `processed_column`) has the same commit-before-send ordering: the offset advances in memory before the batch is sent. A failed send after that point loses the batch; a crash after the batch reaches Iggy but before the offset is persisted replays it on restart. It is best-effort, closer to at-least-once, and the deterministic message ids (see [Message IDs and Idempotent Replay](#message-ids-and-idempotent-replay)) let downstream drop the replays.

If you can't tolerate losing rows, use `processed_column` instead of `delete_after_read` — at least the data's still there if something goes wrong.

## Supported Column Types

The connector handles these MySQL types in JSON mode:

| MySQL Type | JSON Output |
| ---------- | ----------- |
| `BOOLEAN` | boolean |
| `TINYINT` | number (i8 → i64) |
| `SMALLINT` | number (i16 → i64) |
| `MEDIUMINT`, `INT` | number (i32 → i64) |
| `BIGINT` | number (i64) |
| `TINYINT UNSIGNED` | number (u8 → u64) |
| `SMALLINT UNSIGNED` | number (u16 → u64) |
| `MEDIUMINT UNSIGNED`, `INT UNSIGNED` | number (u32 → u64) |
| `BIGINT UNSIGNED` | number (u64) |
| `FLOAT` | number (f32 → f64) |
| `DOUBLE` | number (f64) |
| `DECIMAL` | string (exact value preserved) |
| `BIT` | number (u64) |
| `YEAR` | number (u16 → u64) |
| `DATE` | string (YYYY-MM-DD) |
| `TIME` | string (HH:MM:SS) |
| `DATETIME`, `TIMESTAMP` | string (YYYY-MM-DD HH:MM:SS) |
| `CHAR`, `VARCHAR`, `TINYTEXT`, `TEXT`, `MEDIUMTEXT`, `LONGTEXT` | string |
| `ENUM`, `SET` | string |
| `BINARY`, `VARBINARY`, `TINYBLOB`, `BLOB`, `MEDIUMBLOB`, `LONGBLOB` | base64 string |
| `JSON` | object |
| `NULL` | null |
| Unknown | string (text fallback), or base64 string (binary fallback) |

> **DECIMAL precision:** MySQL `DECIMAL` is an arbitrary-precision decimal type. The connector emits it as a JSON string verbatim (e.g. `"99999999999999999"`), preserving the exact value. Parse it as a decimal in the consumer if you need to do arithmetic; do not assume it arrives as a JSON number.

## Reliability Features

### Automatic Retries

The connector automatically retries transient database errors with linear backoff (`retry_delay * attempt_number`). Transient errors include:

| MySQL Error | Meaning |
| ----------- | ------- |
| `1213` | Deadlock, retry candidate |
| `1205` | Lock wait timeout |
| `1053` | Server shutdown in progress |
| `1152` | Connection aborted |
| `1080` | Forced connection close |
| `1158` | Network read error |
| `1159` | Network read interrupted |
| `1160` | Network write error |
| `1161` | Network write interrupted |
| `1040` | Too many connections |
| `1041` | Out of memory |

These are server-side error codes carried in the MySQL error packet. A dropped or lost connection (server gone away, connection reset) surfaces as an I/O error rather than one of these codes, and is retried as well.

Non-transient errors (syntax errors, constraint violations, missing tables) fail immediately without retry.

Configure with `max_retries` (default: `3`) and `retry_delay` (default: `1s`).

### Undecodable Rows

If a row cannot be decoded — invalid UTF-8 under `text`, non-JSON under `json_direct`, or a column value that doesn't fit its declared type — the connector does **not** skip it. That table's poll fails for the cycle, its offset does not advance, and the same rows are retried on the next poll. Other tables are unaffected and keep flowing.

This is deliberate. Skipping the row would advance the offset past it and drop it permanently with no way to recover, so the connector stalls the one table rather than silently lose data. The trade-off is that a single undecodable row blocks every row behind it in that table until an operator fixes or removes it.

The per-cycle error log identifies the table, the resume offset, the offending column, the primary-key value (best effort), and the underlying reason, so the row can be located and corrected. Once fixed, the table resumes from where it stalled with nothing lost.

### Message IDs and Idempotent Replay

Each message id is deterministic — a UUID v5 derived from the table name and the row's stable key (its primary key, or the unique/monotonic tracking value). The same source row therefore always produces the same message id.

This matters because offset tracking narrows but does not fully close the duplicate window: if the connector crashes after a batch reaches Iggy but before its offset is persisted, those rows are re-polled and re-emitted on restart. Because the replayed messages carry the *same* ids as the originals, a downstream consumer or an idempotent sink can recognize and drop them. A random id per message would make those replays indistinguishable from new data.

Idempotent replay therefore depends on the row having a stable key, which the [unique, monotonic tracking column](#tracking-column-requirements) already guarantees. A row with no extractable key falls back to a random id and logs a warning; under the tracking-column contract that does not happen.

### SQL Injection Protection

In the connector-built query, all table names and column names are quoted with MySQL backtick syntax, and NUL bytes in identifiers are rejected. The `$offset` value is escaped the same way whether it's substituted into the `built_polling_query` or into a `custom_query` (single quotes doubled, backslashes escaped, NUL bytes stripped).

`custom_query` itself is raw, operator-supplied SQL — the connector only escapes the values it substitutes in (`$offset`), not the query text itself.

## Example Configs

### Basic Polling (JSON Mode)

```toml
[[streams]]
stream = "user_events"
topic = "users"
schema = "json"
batch_length = 100

[plugin_config]
connection_string = "mysql://user:pass@localhost:3306/mydb"
tables = ["users"]
poll_interval = "1s"
tracking_column = "id"
```

### Raw Payload Passthrough

```toml
[[streams]]
stream = "messages"
topic = "queue"
schema = "raw"
batch_length = 100

[plugin_config]
connection_string = "mysql://user:pass@localhost:3306/mydb"
tables = ["message_queue"]
poll_interval = "100ms"
tracking_column = "id"
payload_column = "payload"
payload_format = "bytea"
delete_after_read = true
```

### JSON Direct Extraction

```toml
[[streams]]
stream = "events"
topic = "user_events"
schema = "json"
batch_length = 100

[plugin_config]
connection_string = "mysql://user:pass@localhost:3306/mydb"
tables = ["events"]
poll_interval = "1s"
tracking_column = "id"
payload_column = "data"
payload_format = "json_direct"
```

### Multiple Tables

```toml
[[streams]]
stream = "audit"
topic = "changes"
schema = "json"
batch_length = 100

[plugin_config]
connection_string = "mysql://user:pass@localhost:3306/mydb"
tables = ["users", "orders", "products"]
poll_interval = "5s"
tracking_column = "id"
initial_offset = "0"
include_metadata = true
```

### Flat-Schema Sinks (Iceberg, Delta)

In the default JSON mode, each row is wrapped in a `DatabaseRecord` envelope. Sinks like Iceberg and Delta expect flat JSON matching the target schema, so the envelope must be unwrapped.

**Option A — use the `unwrap_envelope` transform** on the sink side to extract the `data` field:

```toml
[transforms.unwrap_envelope]
enabled = true
field = "data"
```

**Option B — bypass the envelope** by setting `include_metadata = false` on the source, which serializes columns directly without the wrapper.
