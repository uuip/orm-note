CREATE OR REPLACE FUNCTION last_day_stat()
    RETURNS Table
            (
                yesterday text,
                amount    int8
            )
AS
$$
DECLARE
    yesterday_table text;
BEGIN
    SET LOCAL TIME ZONE 'Asia/Tokyo';
    yesterday := (SELECT TO_CHAR('yesterday'::DATE, 'YYYYMMDD'));
    yesterday_table := 'transactions_' || yesterday;
    -- 记录if语句使用
    IF (SELECT TO_REGCLASS(yesterday_table)) IS NULL THEN
        amount := NULL;
    ELSE
        EXECUTE 'select count(*) from ' || yesterday_table INTO amount;
    END IF;
    RETURN QUERY VALUES (yesterday, amount);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION last_day_stat()
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
    RETURN NEXT;
    -- RETURN NEXT 等价
    --RETURN QUERY VALUES (yesterday, amount)
EXCEPTION
    WHEN undefined_table THEN amount := NULL;
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- 最终版本
DROP FUNCTION IF EXISTS last_day_stat;
CREATE OR REPLACE FUNCTION last_day_stat(OUT yesterday text, OUT amount int8)
AS
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


--创建并运行匿名函数
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