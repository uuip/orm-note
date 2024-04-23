-- ========================================
-- CREATE TABLE
-- ========================================

CREATE TABLE IF NOT EXISTS indextest
(
    ID1       text PRIMARY KEY,
    T1        int4,
    T2        int4,
    IND       text,
    PRO       int4,
    DJJG      float8,
    XZQH      float8,
    F11       float8,
    F12       float8,
    F13       float8,
    F14       float8,
    P1        float8,
    P2        float8,
    P3        float8,
    D11       float8,
    D12       float8,
    D13       float8,
    M11       float8,
    M21       float8,
    M22       float8,
    M31       float8,
    M32       float8,
    C11       float8,
    C12       float8,
    C13       float8,
    C21       float8,
    C22       float8,
    C31       float8,
    C32       float8,
    C33       float8,
    R11       float8,
    R12       float8,
    F21       float8,
    F22       float8,
    F23       float8,
    F24       float8,
    F25       float8,
    F26       float8,
    F11_score float8,
    F12_score float8,
    F13_score float8,
    F14_score float8,
    F22_score float8,
    F23_score float8,
    F24_score float8,
    F25_score float8,
    F26_score float8,
    F21_score float8,
    F1        float8,
    F2        float8,
    F         float8,
    M1        float8,
    M2        float8,
    M3        float8,
    M         float8,
    C14       float8,
    C1        float8,
    C2        float8,
    C3        float8,
    C         float8,
    D1        float8,
    D21       float8,
    D22       float8,
    D2        float8,
    D         float8,
    R21       float8,
    R22       float8,
    R1        float8,
    R23       float8,
    R2        float8,
    R         float8,
    normal    int4,
    OCI       float8,
    OCI2      float8,
    OCI3      float8
);

-- ========================================
-- ALTER TABLE - ADD/DROP/RENAME COLUMN
-- ========================================

ALTER TABLE some_table
    ADD COLUMN blocknumber INT8 DEFAULT 0;

ALTER TABLE some_table
    DROP COLUMN total_reward;

ALTER TABLE some_table
    RENAME COLUMN old_name TO new_name;

-- ========================================
-- ALTER TABLE - MODIFY COLUMN TYPE/DEFAULT
-- ========================================

ALTER TABLE some_table
    ALTER COLUMN blocknumber TYPE INT8;

ALTER TABLE some_table
    ALTER COLUMN is_destroyed SET DEFAULT FALSE;

-- ========================================
-- ALTER TABLE - CONSTRAINT
-- ========================================

ALTER TABLE some_table
    ADD CONSTRAINT "unique_some_table" UNIQUE ("transactionHash", "logIndex");

-- ========================================
-- ALTER TABLE - OWNER
-- ========================================

ALTER TABLE some_table
    OWNER TO prjbusama;

-- ========================================
-- CREATE INDEX
-- ========================================

CREATE [UNIQUE] INDEX index_name ON TABLE_NAME (column1_name, column2_name);

-- ========================================
-- RESET ID SEQUENCE
-- ========================================

UPDATE some_table
SET id = DEFAULT;
