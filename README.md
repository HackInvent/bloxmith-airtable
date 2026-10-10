# Airtable

List, create, update and upsert records in an Airtable base. The block reads
records as plain JSON (`id`, `created_time`, `fields`) so they travel through a
workflow without extra conversion, and writes JSON records back in batches that
respect Airtable's limits. The personal access token never leaves the BloxSmith
wallet, remote access stays off until you enable it, nothing is written until you
also enable writes, and deleting records needs a third, separate switch.

<!-- block-metadata:start -->
[![Block version: 0.1.0](https://img.shields.io/badge/block-0.1.0-blue)](model.json)
[![BloxSmith compatibility: 1.0.9](https://img.shields.io/badge/BloxSmith-1.0.9-brightgreen)](compatibility.json)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Verified BloxSmith versions: **1.0.9** (bundled-block tests; see [test evidence](compatibility.json)).
<!-- block-metadata:end -->

[![Airtable — list and write table records](media/thumbnail.webp)](media/cover.png)

## When to use it

- Load leads, orders, tasks or an inventory from an Airtable view into a workflow.
- Log workflow results, form answers or detected events as new records.
- Keep a table in sync with another system: upsert by a unique field such as `Email`
  or `SKU`, so a repeated run updates instead of duplicating.
- Mark records as processed by updating their status field.
- Remove records by ID, with the explicit `allow_delete` switch.

Each node works on **one base**, set in its properties. A request may choose
another table of that base, never another base.

## Ports

| Direction | Port | Value |
| --- | --- | --- |
| Input | `request` | A JSON object with the operation parameters, a JSON array of records, a record ID, or any other text as a plain trigger |
| Output | `records` | List, get, create, update and upsert: a JSON array of records `{"id", "created_time", "fields"}` |
| Output | `result` | Operation receipt: counts, record IDs, continuation offset, batches, request ID |
| Output | `error` | Machine `code`, English `message`, `uncertain` flag, provider details and batch progress |

Each fresh request runs exactly one operation. A retained value is never sent
again, so a replayed or forced activation does not repeat a write. Business
failures are published on `error` and the run continues; they never stop an
Active Runtime worker.

### `request` input

| Field | Operations | Meaning |
| --- | --- | --- |
| `operation` | all | `list_records`, `get_record`, `create_records`, `update_records`, `upsert_records` or `delete_records`; defaults to the `operation` setting and must be allowed |
| `request_id` | all | Optional correlation ID (1–128 of `A-Za-z0-9._:-`), echoed in `result` and `error` |
| `table` | all | Table name or `tbl…` ID; defaults to the `table` setting |
| `view` | `list_records` | View name or `viw…` ID; defaults to the `view` setting; `""` reads without a view |
| `filter_by_formula` | `list_records` | Airtable formula (up to 16,000 characters), sent as `filterByFormula` |
| `fields` | `list_records` | Array of field names or IDs to return (`fields[]`) |
| `sort` | `list_records` | Array of `{"field": "Name", "direction": "asc"}` (`asc` or `desc`), at most 20 |
| `max_records` | `list_records` | Records to return, 1 to `read_limit` |
| `page_size` | `list_records` | Records per page, 1–100; defaults to the `page_size` setting |
| `offset` | `list_records` | The `offset` of a previous result, to continue reading |
| `record_id` | `get_record`, `delete_records` | One record ID (`rec…`) |
| `record_ids` | `delete_records` | Array of record IDs |
| `records` / `record` | `create_records`, `update_records`, `upsert_records` | Array of records, or one record (see below) |
| `typecast` | writes | `true` lets Airtable convert values; defaults to the `typecast` setting |
| `fields_to_merge_on` | `upsert_records` | 1–3 field names that identify a record; defaults to the setting |

Unknown fields are refused. A JSON array is a shortcut for `{"records": [...]}`.
A bare record ID such as `recA1b2C3d4E5f6G7` is a shortcut for `{"record_id": ...}`
when the default operation is `get_record` or `delete_records`. Other text (for
example `go` from a trigger) runs the default operation without parameters, which
is useful for `list_records`.

Records to write keep Airtable's shape:

```json
{"operation": "create_records", "records": [{"fields": {"Name": "Ada", "Email": "ada@example.com", "Tags": ["vip"]}}]}
{"operation": "update_records", "records": [{"id": "recA1b2C3d4E5f6G7", "fields": {"Status": "contacted"}}]}
{"operation": "upsert_records", "fields_to_merge_on": ["Email"], "records": [{"fields": {"Email": "ada@example.com", "Status": "active"}}]}
{"operation": "delete_records", "record_ids": ["recA1b2C3d4E5f6G7"]}
```

Field values are any JSON accepted by the field type: text, numbers, booleans,
arrays of linked record IDs or select options, attachment objects with a `url`,
collaborators as `{"email": ...}`. `null` clears a field. `create_records` and
`upsert_records` refuse an `id`; `update_records` needs one per record and refuses
duplicates; `upsert_records` needs a value for every merge field in every record.

### `records` output

```json
[
  {"id": "recA1b2C3d4E5f6G7", "created_time": "2026-01-02T10:00:00.000Z",
   "fields": {"Name": "Ada", "Email": "ada@example.com", "Status": "new"}}
]
```

Airtable leaves empty fields out of `fields` (an empty text, an unchecked box, an
empty list). Writes return the records as Airtable stored them, with their IDs.

### `result` output

- `list_records`: `{"request_id", "operation", "base_id", "table", "view", "count",
  "truncated", "stop_reason", "offset", "pages", "post_list_requests", "requests"}`.
  `stop_reason` is `end`, `limit` (`max_records`), `max_pages` or `output_limit`
  (16 MiB of records). `truncated` is true for the last three; pass `offset` back
  to continue exactly where the read stopped: no record is skipped or repeated.
- `get_record`: `record_id` and `count` (1).
- `create_records`, `update_records`: `count`, `record_ids`, `batches`, `typecast`.
- `upsert_records`: the same plus `fields_to_merge_on`, `created_record_ids` and
  `updated_record_ids`.
- `delete_records`: `count`, `deleted_record_ids`, `batches`.

All receipts carry `request_id`, `operation`, `base_id`, `table` and `requests`
(HTTP requests used, retries included). They never contain the token.

### `error` output

`{"code", "message", "uncertain", "operation", "request_id", "base_id", "table"}`
plus, when Airtable provides them, `http_status`, `retry_after`, `provider_type`
(for example `INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND`), `provider_message`
(bounded, credentials removed) and `attempts`. Writes add `completed_batches`,
`total_batches` and `committed_record_ids`: batches before the failing one **are
written** and are listed there. `uncertain: true` means the failing batch itself
may have been applied: read the table before sending it again.

| Code | Meaning |
| --- | --- |
| `config`, `disabled`, `secret`, `invalid_input`, `request_too_large` | Refused locally; nothing sent |
| `operation_not_allowed`, `writes_disabled`, `delete_disabled` | The operation is not enabled in the properties |
| `too_many_records` | More records than `max_write_records`; nothing written |
| `bad_request` (400), `unauthorized` (401), `quota_exceeded` (402), `forbidden` (403), `not_found` (404), `provider_too_large` (413), `invalid_request` (422), `rate_limited` (429), `provider_error` (other) | Airtable refused the request |
| `provider_unavailable` (5xx), `network`, `timeout`, `cancelled`, `protocol` | Transport outcome; `uncertain` tells whether a write may have been applied |
| `output_too_large` | One page alone exceeds the 16 MiB output limit; lower `page_size` or select fewer fields |
| `tls` | Certificate verification failed; nothing sent |

## Settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Remote access switch. While off, nothing reaches Airtable and the wallet is not read |
| `secret_ref` | empty | Wallet reference such as `secret://workspace/airtable` (see Authentication) |
| `base_id` | empty | Base ID from the base URL (`https://airtable.com/app…/…`): `app` and 14 letters or digits |
| `table` | empty | Default table name or `tbl…` ID |
| `view` | empty | Optional default view for `list_records` |
| `operation` | `list_records` | Operation used when a request names none; always allowed |
| `allowed_operations` | `["list_records"]` | Other operations a request may select |
| `read_limit` | 1000 | Maximum records returned by one `list_records`, 1–10000 |
| `allow_writes` | `false` | Required when any write operation is selectable |
| `typecast` | `false` | Let Airtable convert text to field types and create missing select options |
| `fields_to_merge_on` | `[]` | Default merge fields for `upsert_records`, 1–3 names |
| `max_write_records` | 100 | Records per write request, 1–1000, sent in batches of 10 |
| `allow_delete` | `false` | Additional switch required for `delete_records` |
| `page_size` | 100 | `pageSize` per list request, 1–100 |
| `max_pages` | 20 | Pages per `list_records`, 1–100 |
| `return_fields_by_field_id` | `false` | Key fields by `fld…` IDs instead of names (reads and write receipts) |
| `timeout_sec` | 60 | Deadline for the whole operation, pages, batches, pacing and retries included, 5–300 |
| `max_requests_per_sec` | 5 | Pacing per base in this process, 1–5 |
| `max_retries` | 2 | Retries, 0–5 (see Limits) |
| `max_retry_wait_sec` | 35 | Longest wait before a retry, 1–120 |
| `max_request_bytes` | 1048576 | Bound for the incoming request and each JSON body sent, 1 KiB–8 MiB |
| `max_response_bytes` | 4194304 | Bound for each Airtable response, 64 KiB–32 MiB |
| `api_base_url` | `https://api.airtable.com` | API origin; plain HTTP only for a loopback test host |
| `ca_file` | empty | Optional absolute PEM file for a privately signed HTTPS origin |

The settings are validated on Apply and again before a Run starts. A write
operation listed in `operation` or `allowed_operations` is refused unless
`allow_writes` is on, and `delete_records` also needs `allow_delete`. Restart a
loaded Run to use changed settings.

## Authentication

Create a personal access token in Airtable's developer hub with the scopes the
node needs (`data.records:read` for reading, plus `data.records:write` for writes)
and access to the base. Store it in an unlocked wallet secret, then configure only
its reference, for example `secret://workspace/airtable`. The wallet value is the
bare token text, `{"token": "YOUR_TOKEN"}` or `{"access_token": "YOUR_OAUTH_ACCESS_TOKEN"}`;
these are placeholders, not credentials. An OAuth access token works as long as it
is valid; the block does not refresh it. API keys from the retired legacy scheme
and environment-variable credentials are not supported.

## Example

1. Create a personal access token with `data.records:read` and `data.records:write`
   on your base, and store it in the wallet as `airtable`.
2. Set `enabled = true`, `secret_ref = secret://workspace/airtable`, `base_id` from
   the base URL and `table = Leads`.
3. Wire a Text block containing `go` to `request` and a Display to `records`:
   One Shot returns the records of the `Leads` table as JSON.
4. To log results, enable `allow_writes`, add `create_records` to
   `allowed_operations`, and send
   `{"operation": "create_records", "record": {"fields": {"Name": "Ada", "Status": "new"}}}`.
5. To keep contacts in sync, add `upsert_records`, set `fields_to_merge_on = ["Email"]`
   and send records that carry an `Email` value.

## How the operations work

- `list_records` calls `GET /v0/{base}/{table}` with `pageSize`, `offset`, `view`,
  `filterByFormula`, `fields[]` and `sort[n][field|direction]`, page after page.
  The last page asks only for the records still needed, so the returned `offset`
  continues exactly. When the URL would reach Airtable's 16,000-character limit (a
  long formula or many fields), the same parameters are sent as JSON to
  `POST /v0/{base}/{table}/listRecords`. Reading stops at the last page, at
  `max_records`, at `max_pages` or at 16 MiB of records, and always keeps whole pages.
- `get_record` calls `GET /v0/{base}/{table}/{recordId}`.
- `create_records` sends `POST /v0/{base}/{table}` with up to 10 records per request.
- `update_records` sends `PATCH /v0/{base}/{table}`: only the given fields change.
  `PUT`, which clears the fields that are not given, is never used.
- `upsert_records` sends `PATCH /v0/{base}/{table}` with
  `performUpsert.fieldsToMergeOn`; Airtable creates records without a match and
  updates the matching ones, and the result lists both.
- `delete_records` sends `DELETE /v0/{base}/{table}?records[]=…` with up to 10 IDs.

Every write request is validated and sized before the first batch is sent.

## Limits and warnings

- **Batches are not atomic as a whole.** A request with more than 10 records is
  sent as several requests; if one fails, earlier batches stay written and the
  error lists them in `committed_record_ids`. Each single batch is atomic on
  Airtable's side.
- Requests to one base are paced to `max_requests_per_sec` (Airtable allows 5 per
  second per base) within this process, across nodes. Several Active Runtime workers
  each pace themselves; Airtable then answers 429, which is honoured.
- Reads are retried after HTTP 429, 5xx or a lost connection. Writes are retried
  only after HTTP 429 (Airtable did not process them); a write that fails after it was
  sent is reported with `uncertain: true` and never replayed. `Retry-After` is
  honoured up to `max_retry_wait_sec`; without it, Airtable's documented 30-second
  penalty is used. A longer wait returns `rate_limited` with `retry_after`.
- Airtable list offsets expire after a while; an expired `offset` returns
  `invalid_request` (`LIST_RECORDS_ITERATOR_NOT_AVAILABLE`): restart the read.
- `filter_by_formula` is sent as written. Do not build formulas by pasting untrusted
  text into them: a crafted value can widen the filter to other records of the table.
- `typecast` creates missing select options and converts text; keep it off for data
  that comes from users, chats or web pages unless that is intended.
- Treat `delete_records` as final. Whether Airtable keeps a restorable copy depends on
  the plan and has not been checked by these tests.
- No HTTP proxy support; the host needs direct outbound HTTPS to `api.airtable.com`.
- Field values read from a table are untrusted data. Do not feed them to tools that
  execute commands.

## Dependencies

Python standard library only (`http.client`, `ssl`, `json`); no `requirements.txt`.

## Security

- The wallet value is read at run time, only after settings and the request are
  validated. The token never appears in settings, outputs, logs, run metadata or
  error messages; provider messages are bounded and credentials are removed.
- TLS certificates and host names are always verified; there is no insecure mode.
- Redirects, cookies, proxies and compressed responses are not accepted.
- An `api_base_url` other than the official origin receives the token: change it
  only for a trusted gateway or a local test double.
- The block opens no listening port; it only makes outbound requests.

## Airtable API usage

Airtable Web API **v0** on `https://api.airtable.com`, bearer authentication:

- `GET /v0/{baseId}/{tableIdOrName}` (list records) and
  `POST /v0/{baseId}/{tableIdOrName}/listRecords` (same parameters as JSON)
- `GET /v0/{baseId}/{tableIdOrName}/{recordId}`
- `POST /v0/{baseId}/{tableIdOrName}` (create, `typecast`, `returnFieldsByFieldId`)
- `PATCH /v0/{baseId}/{tableIdOrName}` (update, and upsert with `performUpsert`)
- `DELETE /v0/{baseId}/{tableIdOrName}?records[]=…`

Errors are read from `{"error": {"type", "message"}}`, `{"error": "NOT_FOUND"}` and
`{"errors": [{"error", "message"}]}`; the type is passed through as `provider_type`.

**Real-provider qualification is pending.** All tests use a loopback double that
reproduces these documented request and response shapes; no Airtable account,
token or base was contacted. In particular, the shorter last page (a `pageSize`
change between pages of one read) and the exact error types returned for unknown
tables and records follow the documentation and have not been checked against the
live service.

## Framing decisions

- One base per node, fixed in the properties: a graph cannot redirect writes to
  another base the token can reach. Tables remain selectable per request.
- Three gates for writes: `enabled`, `allow_writes` and the operation allowlist;
  `delete_records` needs a fourth switch, `allow_delete`.
- Records keep Airtable's own shape (`{"fields": {...}}`, `{"id", "fields"}`) for
  writes, so there is no ambiguity between a field named `id` or `fields` and the
  record envelope. Read records are normalized to `id`, `created_time`, `fields`.
- Request field names are snake_case (`filter_by_formula`, `max_records`) like the
  other BloxSmith connectors; the README maps each one to Airtable's parameter.
- Updates use `PATCH` only, so fields that are not given are never cleared.
- `typecast` is off by default, so values stay strict unless conversion is wanted.
- Pagination returns Airtable's own `offset`; `max_records` is applied by shrinking
  the last page rather than by `maxRecords`, which would end the cursor.
- Pacing is process-wide per base, as Airtable's limit is per base.
- No formula builder: formulas are passed through as written and documented as
  such, rather than escaped by rules that have not been qualified on the live API.
- The API origin is fixed but overridable for tests; plain HTTP is accepted only for
  loopback hosts.

## Tests

`tests/F5.01_airtable.py` checks settings, credentials, request contracts, query
encoding, batch sizing, provider error mapping, pacing windows and block hooks
without a socket. `tests/F5.02_airtable_runtime.py` runs the real client against the
loopback double (`tests/airtable_fixture.py`, HTTP and HTTPS): views, formulas, field
selection, sort, pages and exact continuation offsets, POST listRecords, get record,
batched create, update, upsert and delete, partial batch failures, pacing across
sessions, 400/401/402/403/404/413/422/429/5xx, lost connections, oversized and
compressed bodies, cancellation and the deadline, then real graphs in One Shot and
Active Runtime for bundled, managed and linked packages, including Stop while a
request is pending. `tests/F8.01_airtable_ui.py` covers the English and French forms
at desktop and mobile widths. They run in the private BloxSmith workspace against a
pinned framework; the initial development version does not imply a release.

## License

Block code: Apache-2.0. Airtable is an external service governed by its own terms.
No Airtable SDK, logo, account data or credential is bundled.
