-- ========================================
-- BASIC FUNCTION
-- ========================================

-- generate random Chinese characters
CREATE OR REPLACE FUNCTION gen_hanzi(int) RETURNS text AS
$$
DECLARE
    res text;
BEGIN
    IF $1 >= 1 THEN
        SELECT STRING_AGG(CHR(19968 + (RANDOM() * 20901)::int), '') INTO res FROM GENERATE_SERIES(1, $1);
        RETURN res;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- batch drop tables by name pattern
CREATE OR REPLACE FUNCTION footgun(IN _schema TEXT, IN _parttionbase TEXT) RETURNS VOID AS
$$
DECLARE
    ROW record;
BEGIN
    FOR ROW IN SELECT table_schema,
                      TABLE_NAME
               FROM information_schema.tables
               WHERE table_type = 'BASE TABLE'
                 AND table_schema = _schema
                 AND TABLE_NAME ILIKE (_parttionbase || '%')
        LOOP
            EXECUTE 'DROP TABLE ' || QUOTE_IDENT(ROW.table_schema) || '.' ||
                    QUOTE_IDENT(ROW.TABLE_NAME);
            RAISE INFO 'Dropped table: %',
                QUOTE_IDENT(ROW.table_schema) || '.' || QUOTE_IDENT(ROW.TABLE_NAME);

        END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT footgun('public', 'larkle');

-- ========================================
-- FUNCTION with OUT PARAMETERS
-- ========================================

DROP FUNCTION IF EXISTS last_day_stat;

CREATE OR REPLACE FUNCTION last_day_stat(OUT yesterday text, OUT amount int8) AS
$$
DECLARE
BEGIN
    SET LOCAL TIME ZONE 'Asia/Tokyo';
    yesterday := (SELECT TO_CHAR('yesterday'::DATE, 'YYYYMMDD'));
    EXECUTE 'select count(*) from transactions_' || yesterday INTO amount;
EXCEPTION
    WHEN undefined_table THEN amount := NULL;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- FUNCTION with RETURNS TABLE
-- ========================================

CREATE OR REPLACE FUNCTION somenote()
    RETURNS Table
            (
                yesterday text,
                amount    int8
            )
AS
$$
DECLARE
BEGIN
    SET LOCAL TIME ZONE 'Asia/Tokyo';
    yesterday := (SELECT TO_CHAR('yesterday'::DATE, 'YYYYMMDD'));
    EXECUTE 'select count(*) from transactions_' || yesterday INTO amount;
    -- IF statement
    IF (SELECT TO_REGCLASS(yesterday_table)) IS NULL THEN
        amount := NULL;
    ELSE
        EXECUTE 'select count(*) from ' || yesterday_table INTO amount;
    END IF;
    RETURN NEXT;
    -- equivalent to: RETURN QUERY VALUES (yesterday, amount)
EXCEPTION
    WHEN undefined_table THEN amount := NULL;
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- FUNCTION - DELETE related records
-- ========================================

DROP FUNCTION IF EXISTS remove_user;

CREATE FUNCTION remove_user(user_address text) RETURNS VOID AS
$$
DECLARE
    uid int;
BEGIN
    uid := (SELECT id
            FROM "user"
            WHERE address = LOWER(user_address));
    DELETE
    FROM user_function
    WHERE user_id = uid;
    DELETE
    FROM collection
    WHERE user_id = uid;
    DELETE
    FROM "user"
    WHERE id = uid;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- ANONYMOUS DO BLOCK - loop insert
-- ========================================

DO
$$
    BEGIN
        FOR counter IN 1..20
            LOOP
                INSERT INTO indextest
                SELECT MD5(id::text),
                       (RANDOM() * 10000)::int,
                       (RANDOM() * 12)::int,
                       gen_hanzi(4),
                       (RANDOM() * 10000000)::int,
                       RANDOM() * 10000000,
                       RANDOM() * 10000000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       1,
                       RANDOM() * 1000,
                       RANDOM() * 1000,
                       RANDOM() * 1000
                FROM GENERATE_SERIES((counter - 1) * 100 + 1, counter * 100) AS t(id);
                RAISE NOTICE 'counter: %', counter;
            END LOOP;
    END
$$;

-- ========================================
-- ANONYMOUS DO BLOCK - variables + upsert
-- ========================================

DO
$$
    DECLARE
        var_serial_no TEXT := 'host_xx';
        var_endpoint  TEXT := 'http://1.2.3.4:8007';
        var_group_id  plat_groupmodel."id" % TYPE;
        var_node_id   plat_nodemodel."id" % TYPE;
    BEGIN
        INSERT INTO plat_nodemodel (NAME, serial_no, is_approved, node_endpoint, register_time,
                                    beat_time, ct, mt)
        VALUES ('节点名_demo', var_serial_no, TRUE, var_endpoint, NOW(), NOW(), NOW(), NOW())
        ON CONFLICT ( serial_no ) DO UPDATE SET node_endpoint = var_endpoint
        RETURNING ID INTO var_node_id;

        INSERT INTO plat_groupmodel (NAME, created_by, is_deleted, ct, mt)
        VALUES ('联盟名_demo', '创建者_demo', FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT ( name ) DO UPDATE SET name = EXCLUDED.name
        RETURNING ID INTO var_group_id;
    END
$$;
