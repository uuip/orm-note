--清空表
TRUNCATE some_table RESTART IDENTITY CASCADE;

--查询表
select tablename from pg_tables where schemaname='public';

--查询序列
SELECT PG_GET_SERIAL_SEQUENCE('network', 'id');
SELECT *
FROM pg_sequences;

--重新排列已有数据id：
UPDATE some_table t
SET id =t."id" + (SELECT MAX("id") FROM some_table);
ALTER SEQUENCE some_table_id_seq RESTART WITH 1;
UPDATE some_table
SET id = DEFAULT;

-- 下一个 max("id")+1, 与ALTER SEQUENCE 等价
SELECT SETVAL('some_table_id_seq', MAX("id"))
FROM some_table;

--添加约束
ALTER TABLE some_table
    ADD CONSTRAINT "unique_some_table" UNIQUE ("transactionHash", "logIndex");

--添加索引
CREATE
[UNIQUE] INDEX index_name ON TABLE_NAME (column1_name, column2_name);

--修改属主
ALTER TABLE some_table
    OWNER TO prjbusama;

--删除字段
ALTER TABLE some_table
    DROP
        COLUMN total_reward,
    DROP
        COLUMN break_num;

--添加字段
ALTER TABLE some_table
    ADD COLUMN blocknumber INT8 DEFAULT 0;

--修改字段类型
ALTER TABLE some_table
    ALTER COLUMN blocknumber TYPE INT8;

--列重命名
ALTER TABLE some_table
    RENAME COLUMN old_name TO new_name;

--修改字段默认值
ALTER TABLE some_table
    ALTER COLUMN is_destroyed SET DEFAULT FALSE;


