--批量更新
UPDATE t
SET column_a = v.column_a,
    column_b = v.column_b
FROM (VALUES (1, 'FINISH', 1234),
             (2, 'UNFINISH', 3124)) v(id, column_a, column_b)
WHERE v.id = t.id;

--批量更新 cte
WITH cte(id, column_a, column_b) AS (VALUES (1, 'FINISH', 1234),
                                            (2, 'UNFINISH', 3124))
UPDATE table_to_update
SET column_from_table_to_update = cte.column_a
FROM cte
WHERE table_to_update.id = cte.id;

-- 窗口函数
SELECT *, RANK() OVER (PARTITION BY from_user_id ORDER BY updated_at DESC)
FROM transactions_20231020
LIMIT 100;

-- cte + 窗口函数
WITH row_rank AS (SELECT *,
                         ROW_NUMBER() OVER win AS row_seq
                  FROM transactions_20231020
                  WINDOW win AS (PARTITION BY from_user_id ORDER BY updated_at DESC))
SELECT *
FROM row_rank
WHERE row_seq <= 5;


--json and select in
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