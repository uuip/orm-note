-- ========================================
-- BASIC SELECT / WHERE / ORDER BY / LIMIT
-- ========================================

SELECT NAME
FROM track
LIMIT 5;

SELECT NAME
FROM track
WHERE Name NOT LIKE 'A%';

SELECT NAME, GENREID
FROM track
WHERE Name IN ('Snowbal');

SELECT NAME
FROM track
WHERE Name LIKE 'A%'
   OR NAME LIKE 'B%'
ORDER BY NAME
LIMIT 10;

SELECT *
FROM employee
WHERE BirthDate BETWEEN '1962-02-18' AND '1965-03-03';

-- ========================================
-- JOIN
-- ========================================

-- INNER JOIN
SELECT column_name(s)
FROM table1
         JOIN table2 ON table1.column_name = table2.column_name
         JOIN table3 ON table3.column_name = table12.column_name;

-- CROSS JOIN
SELECT *
FROM employee
         CROSS JOIN department;

-- NATURAL JOIN (auto match columns with same name)
SELECT SNAME, DNAME, CNO, TNAME
FROM STUDENT
         NATURAL JOIN TEACHER;

-- LEFT JOIN
SELECT column_name(s)
FROM table1
         LEFT JOIN table2 ON table1.column_name = table2.column_name;

-- RIGHT JOIN
SELECT column_name(s)
FROM table1
         RIGHT JOIN table2 ON table1.column_name = table2.column_name;

-- FULL OUTER JOIN
SELECT column_name(s)
FROM table1
         FULL OUTER JOIN table2 ON table1.column_name = table2.column_name;

-- ========================================
-- SUBQUERY / AGGREGATION
-- ========================================

SELECT InvoiceId, MAX(B)
FROM (SELECT InvoiceId, SUM(UnitPrice * Quantity) B
      FROM invoiceline
      GROUP BY InvoiceId) T;

SELECT c.FirstName, c.LastName
FROM customer c
         JOIN invoice i ON c.CustomerId = i.CustomerId
         JOIN (SELECT InvoiceId, MAX(B)
               FROM (SELECT InvoiceId, SUM(UnitPrice * Quantity) B
                     FROM invoiceline
                     GROUP BY InvoiceId) T) il ON i.InvoiceId = il.InvoiceId;

SELECT count(quantity)
FROM invoiceline
GROUP BY InvoiceId;

-- ========================================
-- EXISTENCE CHECK
-- ========================================

-- ANY + ARRAY
SELECT DISTINCT address
FROM user_address
WHERE address = ANY
      (ARRAY ['3151f7e0-90c1-44c8-b497-2e6dcce092f0', '125623f2-3c42-49a5-b10e-a63e353cf389', 'b164d265-9da9-4314-b7f8-e55427736c6e']);

-- VALUES + JOIN
SELECT a.val
FROM (VALUES ('3151f7e0-90c1-44c8-b497-2e6dcce092f0'),
             ('125623f2-3c42-49a5-b10e-a63e353cf389'),
             ('b164d265-9da9-4314-b7f8-e55427736c6e')) AS a (val)
         JOIN user_address t ON t.address = a.val;

-- UNNEST + EXISTS
SELECT elem
FROM unnest(ARRAY ['3151f7e0-90c1-44c8-b497-2e6dcce092f0', '125623f2-3c42-49a5-b10e-a63e353cf389', 'b164d265-9da9-4314-b7f8-e55427736c6e']) AS elem
WHERE EXISTS (SELECT 1
              FROM user_address
              WHERE address = elem);

-- UNNEST + JOIN
SELECT elem
FROM unnest(ARRAY ['3151f7e0-90c1-44c8-b497-2e6dcce092f0', '125623f2-3c42-49a5-b10e-a63e353cf389', 'b164d265-9da9-4314-b7f8-e55427736c6e']) elem
         JOIN user_address t ON t.address = elem
GROUP BY elem;

-- ========================================
-- WINDOW FUNCTIONS
-- ========================================

SELECT *, RANK() OVER (PARTITION BY from_user_id ORDER BY updated_at DESC)
FROM transactions_20231020
LIMIT 100;

-- CTE + ROW_NUMBER
WITH row_rank AS (SELECT *,
                         ROW_NUMBER() OVER win AS row_seq
                  FROM transactions_20231020
                  WINDOW win AS (PARTITION BY from_user_id ORDER BY updated_at DESC))
SELECT *
FROM row_rank
WHERE row_seq <= 5;

-- ========================================
-- JSONB
-- ========================================

-- jsonb #>> text[] -> text
-- extract JSON sub-object at the specified path as text
SELECT t1.ID,
       ARRAY_TO_STRING(ARRAY_APPEND(
                               ARRAY(
                                       SELECT s1.NAME
                                       FROM business_tag s1
                                       WHERE s1.ID IN (SELECT CAST(s2.level_id #>> '{}' AS INTEGER)
                                                       FROM (SELECT JSONB_ARRAY_ELEMENTS(t1.tag_path) AS level_id) s2)
                                       ORDER BY s1.tag_level
                               ),
                               t1.NAME
                       ), '/') AS "full_name"
FROM business_tag t1
WHERE t1.tag_path != '{}'
ORDER BY t1.ID;

-- <@ containment operator
SELECT t1.ID,
       t1.tag_path,
       t1.full_name,
       ARRAY_TO_STRING(ARRAY_APPEND(
                               ARRAY(
                                       SELECT s1.NAME
                                       FROM business_tag s1
                                       WHERE TO_JSONB(s1.ID) <@ t1.tag_path
                                          OR TO_JSONB(s1.ID::TEXT) <@ t1.tag_path
                                       ORDER BY s1.tag_level
                               ),
                               t1.NAME
                       ), '/') AS "full_name_new"
FROM business_tag t1
ORDER BY t1.id;

-- ========================================
-- TIMEZONE / DATE / TIME
-- ========================================

SELECT *
FROM pg_timezone_names
ORDER BY utc_offset;

SET SESSION TIME ZONE 'Asia/Tokyo';
SHOW TIMEZONE;
RESET TIMEZONE;

-- use SET LOCAL in transaction
BEGIN;
SET LOCAL TIME ZONE 'Asia/Tokyo';
SELECT 'yesterday'::DATE;
SELECT TO_CHAR('yesterday'::date, 'YYYYMMDD');
END;

SELECT CURRENT_TIMESTAMP(0);
SELECT (CURRENT_DATE AT TIME ZONE 'Asia/Tokyo' - INTERVAL '1days')::date;

SELECT TO_CHAR(TO_TIMESTAMP(start_time) AT TIME ZONE 'utc', 'YYYY-MM-DD  HH24:MI:SS utc');
-- shorthand, accepts various common inputs
SELECT gen_time::timestamptz;
SELECT TO_TIMESTAMP(1698141991);

-- EXTRACT
SELECT EXTRACT(HOUR FROM gen_time::timestamptz) AS event_time, COUNT(*)
FROM transactions_pool
GROUP BY event_time
ORDER BY event_time;

-- DATE_TRUNC
SELECT DATE_TRUNC('day', stats_at AT TIME ZONE 'Asia/Tokyo')::date, stats_at, stats_date
FROM consumer_log_stats;

-- ========================================
-- COUNT FILTER / GENERATE_SERIES / CTE AGGREGATION
-- ========================================

SELECT COUNT(*),
       COUNT(*) FILTER (WHERE point > 10000)
FROM transactions_20231020;

SELECT *
FROM GENERATE_SERIES('2008-03-01 00:00'::timestamp,
                     '2008-03-04 12:00', '10 hours');

WITH cte AS (SELECT gen_time::timestamptz AS event_time FROM transactions_pool ORDER BY event_time)
SELECT MAX(event_time), MIN(event_time)
FROM cte;

-- ========================================
-- SELECT FOR UPDATE (row locking)
-- ========================================

SELECT id, user_id
FROM userinfo
WHERE user_id LIKE 'HAP%'
ORDER BY id
LIMIT 1 FOR UPDATE SKIP LOCKED;
