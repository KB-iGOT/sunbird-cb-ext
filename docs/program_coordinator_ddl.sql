-- Program Coordinator — schema for the two supported APIs:
--   1. Upsert (add / soft-remove a coordinator on a programme)
--   2. Paginated, optionally-sorted list of coordinators for a programme
--
-- Scoped down from the full design doc: only the role master and the
-- membership table + its read index are included. The audit table and the
-- read-only view from the design doc are NOT created here, since neither
-- is used by the two APIs currently being built.
--
-- No migration tool (Flyway/Liquibase) is wired into this project, so this
-- script is meant to be run manually against the target Postgres database
-- before the application code that depends on it is deployed.

-- ---------------------------------------------------------------------
-- Role master. Loaded once and cached in application memory. IDs are
-- assigned in display order with gaps of ten so a new role can be slotted
-- in later without renumbering existing rows.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS program_coordinator_role (
    id          SMALLINT     PRIMARY KEY,
    role_code   VARCHAR(64)  NOT NULL UNIQUE,
    role_name   VARCHAR(128) NOT NULL,
    is_active   BOOLEAN      NOT NULL DEFAULT TRUE
);

INSERT INTO program_coordinator_role (id, role_code, role_name, is_active) VALUES
  (10, 'NATIONAL_LEAD_TRAINER', 'National Lead Trainer', TRUE),
  (20, 'STATE_LEAD_TRAINER',    'State Lead Trainer',    TRUE),
  (30, 'STATE_MASTER_TRAINER',  'State Master Trainer',  TRUE)
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------------------
-- Membership table. Stores membership only — no name/email; those are
-- resolved per page from Cassandra/Elasticsearch by the caller.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS program_coordinator (
    program_id   VARCHAR(64) NOT NULL,
    user_id      UUID        NOT NULL,
    role_id      SMALLINT    NOT NULL REFERENCES program_coordinator_role(id),
    status       SMALLINT    NOT NULL DEFAULT 1,   -- 1 = active, 0 = removed
    created_by   UUID        NOT NULL,
    created_on   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by   UUID,
    updated_on   TIMESTAMPTZ,
    CONSTRAINT pk_program_coordinator PRIMARY KEY (program_id, user_id),
    CONSTRAINT ck_pc_status CHECK (status IN (0, 1))
);

-- Serves the list API's WHERE (program_id, status) predicate and, when the
-- caller asks to sort, the ORDER BY on role_id/user_id — all from one index.
CREATE INDEX IF NOT EXISTS idx_pc_program_role_active
    ON program_coordinator (program_id, role_id, user_id)
    WHERE status = 1;
