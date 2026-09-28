# Program Coordinator bulk upload — tracking table

Tracking table for the bulk "Program Coordinator" upload flow
(`ProgramCoordinatorBulkUploadServiceImpl` / `ProgramCoordinatorBulkUploadProcessingServiceImpl`).
Written and read purely via `CassandraOperation` (no JPA entity), matching how the other
bulk-upload features in this repo track their jobs (`user_bulk_upload`,
`calendar_event_bulk_upload`, `org_designation_mapping_bulk_upload`) — as opposed to the
JPA-backed `program_coordinator` / `program_coordinator_role` tables used for actual coordinator
membership data (see `program_coordinator_ddl.sql`).

No migration tool (Flyway/Liquibase) is wired into this project, so this script is meant to be
run manually against the target Cassandra keyspace (`sunbird`) before the application code that
depends on it is deployed — same caveat as `program_coordinator_ddl.sql`.

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

Column names are written lowercase deliberately: `CassandraOperationImpl`/`CassandraUtil` build every
INSERT/UPDATE with **unquoted** identifiers (the raw `Constants.*` Java strings, e.g. `"programId"`,
`"fileName"`), and Cassandra folds unquoted identifiers to lowercase when parsing CQL. So the
application's camelCase constants and this table's lowercase columns resolve to the exact same
names (`programId` -> `programid`, `fileName` -> `filename`, etc.) - this only holds as long as the
column names here stay unquoted too.

- Partition key `programid` + clustering key `identifier` lets `isPreviousUploadInProgress`
  (query by `programid` alone, no `ALLOW FILTERING`) and `getStatus` / the async processor's status
  updates (query by the full `programid` + `identifier` key) both work off a single table.
- `identifier` is `text`, not `uuid`/`timeuuid` - the app writes `UUID.randomUUID().toString()`, a
  plain string, so the column type must match.
- `status` moves `INITIATED` → `IN-PROGRESS` → one of `SUCCESSFUL` / `PARTIALLY-COMPLETED` /
  `FAILED`.
- `resultfilepath` is populated once the async processor writes back the annotated CSV/XLSX
  (input columns + `Status` + `Error Details`); it stays null while a job is in flight or if it
  fails before producing a result file.
