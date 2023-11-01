--批量删除表
CREATE
    OR REPLACE FUNCTION footgun(IN _schema TEXT, IN _parttionbase TEXT) RETURNS void
    LANGUAGE plpgsql AS
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
            EXECUTE 'DROP TABLE ' || QUOTE_IDENT(ROW.table_schema) || '.' || QUOTE_IDENT(ROW.TABLE_NAME);
            RAISE INFO 'Dropped table: %',
                QUOTE_IDENT(ROW.table_schema) || '.' || QUOTE_IDENT(ROW.TABLE_NAME);

        END LOOP;
END;
$$;

--执行函数
SELECT footgun('public', 'larkle');

