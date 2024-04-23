-- ========================================
-- BASIC INSERT / UPDATE / DELETE
-- ========================================

INSERT INTO Album (TITLE, ARTISTID)
VALUES ('LAYDGAGA', 100);

INSERT INTO Album
VALUES (100, 'LAYDGAGA', 100);

UPDATE Album
SET ArtistID=200, TITLE='LADYGA'
WHERE TITLE = 'LAYDGAGA';

DELETE
FROM Album
WHERE TITLE = 'LADYGA';

-- ========================================
-- TRUNCATE
-- ========================================

TRUNCATE some_table RESTART IDENTITY CASCADE;

-- ========================================
-- UPSERT (INSERT ON CONFLICT)
-- ========================================

INSERT INTO userinfo(user_id, address)
VALUES ($1, $2)
ON CONFLICT DO NOTHING;

-- based on another table
INSERT INTO newuser(user_id, user_type)
SELECT $1, $2
WHERE NOT EXISTS(SELECT id FROM userinfo WHERE user_id = $1)
ON CONFLICT DO NOTHING;

-- ========================================
-- BATCH UPDATE - VALUES
-- ========================================

UPDATE tablename t
SET column_a = v.column_a,
    column_b = v.column_b
FROM (VALUES (1, 'FINISH', 1234),
             (2, 'UNFINISH', 3124)) v(id, column_a, column_b)
WHERE v.id = t.id;

-- ========================================
-- BATCH UPDATE - CTE
-- ========================================

WITH cte(id, column_a, column_b) AS (VALUES (1, 'FINISH', 1234),
                                            (2, 'UNFINISH', 3124))
UPDATE table_to_update
SET column_from_table_to_update = cte.column_a
FROM cte
WHERE table_to_update.id = cte.id;

-- ========================================
-- INSERT / UPDATE with UNNEST
-- ========================================

INSERT INTO tablename("parse_date", "log_num")
SELECT UNNEST('{"2023-09-23 18:09:04+08","2023-09-23 18:09:04+08"}'::timestamptz[]),
       UNNEST(ARRAY [11,22]);

UPDATE tablename
SET log_date=bulk_query.log_date
FROM UNNEST('{"2023-09-23 18:09:04+08","2023-09-23 18:09:04+08"}'::timestamptz[],
            ARRAY [11,22]) AS bulk_query(log_date, log_num)
WHERE tablename.log_num = bulk_query.log_num;
