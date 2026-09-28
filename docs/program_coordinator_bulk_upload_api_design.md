# Program Coordinator — Bulk Upload API Design

## 1. Purpose

Program Coordinators can currently only be added one at a time via
`PUT /program/coordinator/{programId}`. This design adds a **bulk add** capability: a Program
Coordinator uploads a CSV/Excel file of `(Registered Name, Registered Email ID, Trainer Type)`
rows, and for every row that is a genuinely-registered user, the system:

1. Assigns the `BP_PROGRAM_TRAINER` role to the user.
2. Sets `profileDetails.bpCoTrainer = <Trainer Type>` on the user's profile, without disturbing
   any other `profileDetails` field.
3. Adds the user as a Program Coordinator on the target programme, in a single batched action.

Two new APIs are added to `ProgramCoordinatorController`; per-row processing runs asynchronously
via Kafka, reusing the existing single-user `upsert()` API for the actual coordinator-add step.

## 2. Architecture

```
Client                Controller              Sync Service                Cassandra          Kafka
  |  POST file            |                        |                          |                |
  |----------------------->  bulkUploadCoordinators |                          |                |
  |                       |----------------------->  bulkUpload()             |                |
  |                       |                        |-- role/in-progress check |                |
  |                       |                        |-- validate file struct   |                |
  |                       |                        |-- upload raw file        |                |
  |                       |                        |-- insert tracking row -->|                |
  |                       |                        |-- publish event -------------------------->|
  |  <---- 200 INITIATED --------------------------|                          |                |
                                                                                                 |
                                                                                    Kafka Consumer|
                                                                                    (async, off  |
                                                                                     the request  |
                                                                                     thread)      |
                                                                                                 v
                                                                          ProgramCoordinatorBulkUploadProcessingServiceImpl
                                                                          -- download file, parse rows
                                                                          -- per row: validate, register-check,
                                                                             assign role, update profile
                                                                          -- batch all valid rows into ONE
                                                                             ProgramCoordinatorService.upsert() call
                                                                          -- write annotated result file
                                                                          -- update tracking row (terminal status)

  |  GET status/{identifier}                        |
  |----------------------->  getBulkUploadStatus     |
  |                       |----------------------->  getStatus() -- reads tracking row from Cassandra
  |  <---- 200 {status, ...} ------------------------|
```

**Sync phase** (request/response): validates access + file structure, uploads the raw file to
storage, inserts a tracking record, publishes a Kafka event, and returns immediately with
`status = INITIATED`. No row-level processing happens on the request thread.

**Async phase** (Kafka consumer): downloads the file, parses rows, validates and processes each
row independently (one bad row does not abort the file), batches every valid row into a single
call to the existing `ProgramCoordinatorService.upsert()`, writes back an annotated result file,
and finalizes the tracking record's status.

## 3. APIs

### 3.1 `POST /program/coordinator/bulk-upload/{programId}`

Triggers a bulk upload job.

**Path variable**
| Name | Type | Description |
|---|---|---|
| `programId` | string | Target programme id |

**Headers**
| Name | Required | Description |
|---|---|---|
| `x-authenticated-user-token` | Yes | Caller's auth token. Must belong to a user holding the `PROGRAM_COORDINATOR` role (`program.coordinator.allowed.roles`) — same requirement as the existing single-user upsert API. The caller's user id (recorded as `createdBy` on the tracking record) is derived from this token via `AccessTokenValidator.fetchUserIdFromAccessToken()`, the same way `ProgramCoordinatorServiceImpl.upsert()` derives its actor id — no separate id header is needed. |

**Body** — `multipart/form-data`
| Part | Type | Description |
|---|---|---|
| `file` | file | CSV or `.xlsx`. Header row must contain `Registered Name`, `Registered Email ID`, `Trainer Type` (case-insensitive; an optional `(mandatory)` suffix on any header is also accepted). |

**Processing (synchronous) steps**
1. Reject with `403` if the caller's token doesn't carry `PROGRAM_COORDINATOR`.
2. Reject with `429` if a bulk upload for this `programId` is already `INITIATED`/`IN-PROGRESS`.
3. Reject with `400` if the file is missing/empty, an unsupported type, missing a mandatory
   column, or has no data rows.
4. Reject with `400` if the row count exceeds `program.coordinator.bulk.upload.max.rows`
   (default `500`) — checked again in the async phase against the same limit as a safety net.
5. Upload the raw file to blob storage.
6. Insert a tracking record into `program_coordinator_bulk_upload` (see §4) with
   `status = INITIATED`.
7. Publish an event to the `program.coordinator.bulk.upload.final` Kafka topic (see §5) carrying
   the tracking record plus the caller's auth token, so the async consumer can act on behalf of
   the same user.

**Success response — `200 OK`**
```json
{
  "id": "api.program.coordinator.bulk.upload",
  "ver": "v1",
  "ts": "...",
  "params": { "status": "Successful" },
  "responseCode": "OK",
  "result": {
    "programId": "prog-123",
    "identifier": "b6f8...-uuid",
    "fileName": "1732512345678_coordinators.csv",
    "filePath": "https://<storage>/.../1732512345678_coordinators.csv",
    "dateCreatedOn": "2026-09-24T10:15:30.000+00:00",
    "status": "INITIATED",
    "comment": "",
    "createdBy": "<callerUserId>"
  }
}
```

**Error responses**
| HTTP status | Condition |
|---|---|
| `403 FORBIDDEN` | Caller lacks the `PROGRAM_COORDINATOR` role |
| `429 TOO_MANY_REQUESTS` | A bulk upload for this `programId` is already in progress |
| `400 BAD_REQUEST` | File missing/empty, unsupported type, missing mandatory columns, no data rows |
| `500 INTERNAL_SERVER_ERROR` | Storage upload failure, tracking-record insert failure, or unexpected exception |

### 3.2 `GET /program/coordinator/bulk-upload/{programId}/status/{identifier}`

Reads back the tracking record for a previously submitted job — used to poll for completion.

**Path variables**: `programId`, `identifier` (returned by the POST above).

**Headers**: `x-authenticated-user-token` (no specific role required beyond a valid token).

**Success response — `200 OK`** (raw tracking-record columns, as persisted in Cassandra —
note the column-name casing differs from the POST response above; see §4):
```json
{
  "params": { "status": "Successful" },
  "responseCode": "OK",
  "result": {
    "programid": "prog-123",
    "identifier": "b6f8...-uuid",
    "filename": "1732512345678_coordinators.csv",
    "filepath": "https://<storage>/.../1732512345678_coordinators.csv",
    "resultfilepath": "https://<storage>/.../1732512345678_coordinators.csv",
    "datecreatedon": "2026-09-24T10:15:30.000Z",
    "dateupdatedon": "2026-09-24T10:15:48.000Z",
    "status": "PARTIALLY-COMPLETED",
    "comment": "",
    "createdby": "<callerUserId>",
    "totalrecords": 10,
    "successfulrecordscount": 8,
    "failedrecordscount": 2
  }
}
```

**Error responses**
| HTTP status | Condition |
|---|---|
| `404 NOT_FOUND` | No tracking record for the given `programId` + `identifier` |
| `500 INTERNAL_SERVER_ERROR` | Unexpected exception |

**Status lifecycle**: `INITIATED` → `IN-PROGRESS` → one of `SUCCESSFUL` (all rows succeeded) /
`PARTIALLY-COMPLETED` (some rows failed) / `FAILED` (structural failure or every row failed).
`resultfilepath` is populated once the async processor writes the annotated result file back
(input columns + `Status` + `Error Details`); it stays absent while a job is in flight.

## 4. Data model changes

### New table: `sunbird.program_coordinator_bulk_upload`

Written/read purely via `CassandraOperation` (no JPA entity) — the same pattern used by every
other bulk-upload feature in this codebase (`user_bulk_upload`, `calendar_event_bulk_upload`,
`org_designation_mapping_bulk_upload`), as opposed to the JPA-backed `program_coordinator` /
`program_coordinator_role` tables used for actual coordinator membership.

```cql
CREATE TABLE IF NOT EXISTS sunbird.program_coordinator_bulk_upload (
    programid              text,
    identifier             text,
    filename               text,
    filepath               text,
    resultfilepath         text,
    datecreatedon          timestamp,
    dateupdatedon          timestamp,
    status                 text,
    comment                text,
    createdby              text,
    totalrecords           int,
    successfulrecordscount int,
    failedrecordscount     int,
    PRIMARY KEY (programid, identifier)
) WITH comment = 'Tracking records for the Program Coordinator bulk-upload (CSV/Excel) flow';
```

- **Partition key**: `programid` — supports the in-progress guard (query by programme alone, no
  `ALLOW FILTERING`).
- **Clustering key**: `identifier` — supports the exact-key status lookup.
- Column names are lowercase because the application writes/reads via unquoted CQL identifiers
  (`Constants.PROGRAM_ID = "programId"`, etc.), which Cassandra folds to lowercase automatically —
  this is also why the POST response (built directly from the Java tracking-record map, before any
  DB round-trip) shows camelCase keys while the GET status response (read straight back from
  Cassandra) shows the folded lowercase column names.

### No changes to existing tables

`program_coordinator_role` already seeds the three trainer types this feature depends on
(`NATIONAL_LEAD_TRAINER`, `STATE_LEAD_TRAINER`, `STATE_MASTER_TRAINER`) — the "Trainer Type"
column value is validated against this table's `role_code` (still queried directly, purely to
reject an invalid value against the correct row instead of failing the whole batch — see §7) and
is passed straight through as `ProgramCoordinatorUpsertRequest.roleName`, which
`ProgramCoordinatorServiceImpl.upsert()` now resolves to a `roleId` itself. It also doubles as the
literal value written to `profileDetails.bpCoTrainer`. No new rows or schema changes were needed
here.

`program_coordinator.is_co_trainer` (added upstream alongside the `roleName`-resolution support)
is always set to `true` on every batched request this feature builds — bulk-adding co-trainers as
coordinators is this feature's entire purpose.

## 5. Kafka changes

**New topic**
| Property | Value |
|---|---|
| `kafka.topics.program.coordinator.bulk.upload` | `program.coordinator.bulk.upload.final` |
| `kafka.topics.program.coordinator.bulk.upload.group` | `programCoordinatorBulkUpload` |

**Producer**: `ProgramCoordinatorBulkUploadServiceImpl.triggerBulkUploadKafkaEvent()`, keyed by
`programId`. Payload is the tracking record plus `x-authenticated-user-token` (the caller's
token, carried through so the async consumer can call downstream APIs and the existing
`upsert()` on the caller's behalf).

**Consumer**: `ProgramCoordinatorBulkUploadConsumer` (`@KafkaListener`), hands off to
`ProgramCoordinatorBulkUploadProcessingServiceImpl.initiateProgramCoordinatorBulkUploadProcess()`
on a separate async thread.

This is independent of the existing `program.coordinator.sync.topic`
(`cb.program.coordinator.sync`), which `ProgramCoordinatorService.upsert()` still publishes to
after the batched add, for Elasticsearch coordinator-lookup sync — unchanged by this feature.

## 6. External / downstream APIs used

All under the existing `sb.service.url` base, reusing existing config properties — no new
downstream endpoints were introduced.

| Call | Property | Path | Auth header sent | Purpose |
|---|---|---|---|---|
| User lookup by email | `sb.service.user.lookup.path` | `/private/user/v1/lookup` | none | Registration check for a row's email |
| User read | `lms.user.read.path` | `/private/user/v1/read/{userId}` | `x-authenticated-user-token` | Fetch current `roles`, `profileDetails`, name for a user — called fresh immediately before each of the next two calls, not from one shared snapshot |
| Assign role | `sb.service.assign.role.path` | `/v1/user/assign/role` | none | Append `BP_PROGRAM_TRAINER` to the user's existing role list and re-submit the full list |
| Profile update | `lms.user.update.private.path` | `/private/user/v1/update` | none | Merge `profileDetails.bpCoTrainer` into the user's existing `profileDetails` and PATCH the whole object back |

The assign-role and profile-update calls are both `/private/...` / system-level LMS endpoints and
deliberately don't carry the caller's `x-authenticated-user-token` — only `Content-Type` is sent.
The user-read call still needs the token, since it's what identifies which user's data is being
fetched via the token's own auth context.

**In-process call (not HTTP)**: `ProgramCoordinatorService.upsert(programId, batch, token)` —
the async processor calls this directly as a Java method once per file, with one
`ProgramCoordinatorUpsertRequest` per row that passed validation. This reuses all of `upsert()`'s
existing logic unchanged (access check, per-row DB upsert, Kafka sync-event publish), so the
bulk flow is authorized by the exact same `program.coordinator.allowed.roles` check as the
single-user API.

## 7. Row validation rules (async phase)

Applied in order; first failure wins and the row is marked `Failed` in the result file with the
corresponding message, while the rest of the file continues processing:

1. Mandatory fields present (`Registered Name`, `Registered Email ID`, `Trainer Type`).
2. Email format valid.
3. Email not a duplicate within the same file.
4. `Trainer Type` matches an active `program_coordinator_role.role_code` (case-insensitive).
5. Email resolves to a registered user (via user lookup).
6. File's `Registered Name` matches `profileDetails.personalDetails.firstname` on the registered
   user's record (case-insensitive, trimmed) — a **hard** validation failure on mismatch, not
   merely informational.
7. Role assignment succeeds.
8. Profile update succeeds.

Only rows passing all eight checks are included in the batched `upsert()` call.

## 8. Configuration summary

| Property | Default | Purpose |
|---|---|---|
| `program.coordinator.bulk.upload.max.rows` | `500` | Row-count ceiling per file |
| `program.coordinator.bulk.upload.result.headers` | `Registered Name,Registered Email ID,Trainer Type,Status,Error Details` | Column order for the annotated result file |
| `kafka.topics.program.coordinator.bulk.upload` | `program.coordinator.bulk.upload.final` | Kafka topic |
| `kafka.topics.program.coordinator.bulk.upload.group` | `programCoordinatorBulkUpload` | Consumer group id |
| `program.coordinator.allowed.roles` | `PROGRAM_COORDINATOR` | Reused, unchanged — gates both the bulk POST and the underlying `upsert()` |

## 9. New source files

| File | Responsibility |
|---|---|
| `service/ProgramCoordinatorBulkUploadService.java` / `Impl` | Sync phase: access check, in-progress guard, file validation, upload, tracking-record insert, Kafka publish, status read |
| `service/ProgramCoordinatorBulkUploadProcessingService.java` / `Impl` | Async phase: parse rows, validate, register-check, role assignment, profile update, batched `upsert()`, result file, final status |
| `consumer/ProgramCoordinatorBulkUploadConsumer.java` | Kafka listener bridging the two phases |
| `model/ProgramCoordinatorBulkUploadRowSummary.java` | Aggregates per-row outcomes into total/success/fail counts plus the annotated rows |
| `controller/ProgramCoordinatorController.java` (extended) | The two new endpoints described in §3 |
