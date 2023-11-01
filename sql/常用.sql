SELECT *
FROM pg_timezone_names
ORDER BY utc_offset;
--Asia/Tokyo
--Asia/Shanghai
--America/New_York
--America/Los_Angeles
--Pacific/Samoa

SHOW TIMEZONE;
RESET TIMEZONE;
SET SESSION TIME ZONE 'Asia/Tokyo';

SELECT TO_CHAR(TO_TIMESTAMP(start_time) AT TIME ZONE 'utc', 'YYYY-MM-DD  HH24:MI:SS utc');
SELECT TO_TIMESTAMP(gen_time, 'YYYY-MM-DD"T"HH24:MI:SS.MSTZH') AS event_time;
-- 更简单写法，支持各种常规输入
SELECT gen_time::timestamptz;

BEGIN;
SET LOCAL TIME ZONE 'Asia/Tokyo';
SELECT TO_CHAR('yesterday'::date, 'YYYYMMDD');
END;

SELECT DATE_TRUNC('day', updated_at)::date
FROM transactions_20230923
LIMIT 5;

SELECT CURRENT_TIMESTAMP(0);
SELECT CURRENT_DATE AT TIME ZONE 'America/Los_Angeles' - INTERVAL '1days';

--查询连接数
SELECT COUNT(*)
FROM pg_stat_activity;

--删除连接
SELECT PG_TERMINATE_BACKEND(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'website'
  AND pid <> PG_BACKEND_PID();

--查询元数据
SHOW ALL;
SHOW config_file;
SELECT VERSION();
SELECT CURRENT_SETTING('server_version_num')::integer;
SHOW SERVER_VERSION;

SELECT tablename
FROM pg_catalog.pg_tables
WHERE schemaname = 'public'
ORDER BY tablename;

SELECT PG_SIZE_PRETTY(PG_DATABASE_SIZE('jp_170'));
SELECT PG_SIZE_PRETTY(PG_TOTAL_RELATION_SIZE('transactions_20231011'));
