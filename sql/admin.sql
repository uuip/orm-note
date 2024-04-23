-- ========================================
-- QUERY TABLES
-- ========================================

SELECT tablename
FROM pg_tables
WHERE schemaname = 'public';

-- ========================================
-- QUERY INDEXES
-- ========================================

SELECT tablename,
       indexname,
       indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'pindex_1'
ORDER BY indexname;

-- ========================================
-- QUERY / MANAGE SEQUENCES
-- ========================================

SELECT PG_GET_SERIAL_SEQUENCE('network', 'id');

SELECT *
FROM pg_sequences;

-- reset sequence after truncate
-- ALTER SEQUENCE seq_name RESTART WITH 1;
-- SELECT SETVAL('seq_name', 1, false);
-- ALTER TABLE tbl ALTER COLUMN id RESTART 1;  -- for IDENTITY columns

-- ========================================
-- CONNECTIONS
-- ========================================

SELECT COUNT(*)
FROM pg_stat_activity;

SELECT PG_TERMINATE_BACKEND(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'website'
  AND pid <> PG_BACKEND_PID();

-- ========================================
-- SERVER METADATA
-- ========================================

SHOW ALL;
SHOW config_file;
SELECT VERSION();
SELECT CURRENT_SETTING('server_version_num')::integer;
SHOW SERVER_VERSION;

SELECT * FROM pg_get_keywords();
SELECT PG_SIZE_PRETTY(PG_DATABASE_SIZE('jp_170'));
SELECT PG_SIZE_PRETTY(PG_TOTAL_RELATION_SIZE('transactions_20231011'));
