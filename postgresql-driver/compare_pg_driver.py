import asyncio
import inspect as std_inspect
import pickle
import random
import re
import sys
import time
from datetime import datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

import asyncpg
import orjson
import pandas as pd
import psycopg
import psycopg2
from loguru import logger
from psycopg2.extras import execute_values, execute_batch
from sqlalchemy import *
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.orm import *

sys.path.append("..")
from conf import settings
from models.model import StatusChoice


class Base(DeclarativeBase):
    pass


class TransactionsPool(Base):
    __tablename__ = "transactions_pool"

    created_at = mapped_column(DateTime(timezone=True), server_default=func.current_timestamp())
    updated_at = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), onupdate=func.current_timestamp()
    )
    request_time = mapped_column(DateTime(timezone=True), nullable=True)
    success_time = mapped_column(DateTime(timezone=True), nullable=True)
    block_number = mapped_column(BigInteger, nullable=True)
    status = mapped_column(Enum(StatusChoice, native_enum=False), server_default=StatusChoice.pending)
    status_code = mapped_column(Integer, server_default=text("0"), index=True)
    fail_reason = mapped_column(Text, nullable=True, index=True)
    nonce = mapped_column(BigInteger, nullable=True)
    gas = mapped_column(BigInteger, nullable=True)
    tx_hash = mapped_column(VARCHAR(255), nullable=True)

    from_user_id = mapped_column(VARCHAR(255), index=True)
    to_user_id = mapped_column(VARCHAR(255))
    coin_code = mapped_column(Text)
    point = mapped_column(Float)
    tag_id = mapped_column(VARCHAR(255), primary_key=True)
    store_id = mapped_column(VARCHAR(255), nullable=True)
    gen_time = mapped_column(VARCHAR(128))
    ext_json = mapped_column(Text)


def make_data():
    t = datetime.now(tz=ZoneInfo("Asia/Tokyo"))
    data = {
        "coin_code": "JPY",
        "pay_type": "xxPay",
        "trxn_result": "SUCCESS",
        "trxn_type": "general",
        "store_id": "devtest",
        "point": 10.0,
        "from_user_id": "CPM1696751455",
        "to_user_id": "920MH0OFY6c",
        "tag_id": str(uuid4()),
        "gen_time": t.isoformat(),
    }
    data["ext_json"] = orjson.dumps(data).decode()
    data2 = {
        "tx_hash": "0x77babc8124b64c6556976c847a16590600135307f1ba4cc0d2d1a7e98a55b230",
        "gas": 50000,
        "nonce": 50,
        "fail_reason": None,
        "status_code": 200,
        "block_number": 1007334,
        "success_time": t,
        "request_time": t,
    }
    data.pop("trxn_result")
    data.pop("trxn_type")
    data.pop("pay_type")
    return data | data2


def make_update_data():
    t = datetime.now(tz=ZoneInfo("Asia/Tokyo"))
    data = {
        "coin_code": "JPYa",
        "pay_type": "xxPaya",
        "trxn_result": "SUCCESSa",
        "trxn_type": "generala",
        "store_id": "devtesta",
        "point": 110.0,
        "from_user_id": "aCPM1696751455",
        "to_user_id": "a920MH0OFY6c",
        "tag_id": str(uuid4()),
        "gen_time": t.isoformat(),
    }
    data["ext_json"] = orjson.dumps(data).decode()
    data2 = {
        "tx_hash": "0x6677abc8124b64c6556976c847a16590600135307f1ba4cc0d2d1a7e98a55b230",
        "gas": 150000,
        "nonce": 150,
        "fail_reason": None,
        "status_code": 1200,
        "block_number": 11007334,
        "success_time": t,
        "request_time": t,
    }
    data.pop("trxn_result")
    data.pop("trxn_type")
    data.pop("pay_type")
    return data | data2


def remove_pk(item: dict):
    del item["tag_id"]
    return item


# data = [make_data() for _ in range(1000000)]
# with open("data.bin","wb+") as f:
#     pickle.dump(data,f)
# exit()
with open("data.bin", "rb") as f:
    data = pickle.load(f)
fields = list(data[0].keys())
to_updated_pk = [x["tag_id"] for x in random.sample(data, k=100000)]
to_updated_data = []
for i in range(100000):
    item = make_update_data()
    item["tag_id"] = to_updated_pk[i]
    to_updated_data.append(item)
del to_updated_pk
updated_fields = list(to_updated_data[0].keys())
updated_fields.remove("tag_id")


async def asyncpg_many_values_once():
    # query arguments cannot exceed 32767

    sql = insert(TransactionsPool).values(data[0]).compile(dialect=PGDialect_asyncpg())
    args = []
    for x in data:
        item = []
        for field in sql.positiontup:
            if x[field] is None:
                item.append("null")
            elif isinstance(x[field], datetime):
                item.append(f"'{x[field].isoformat()}'")
            elif isinstance(x[field], str):
                item.append(f"'{x[field]}'")
            else:
                item.append(x[field])
        args.append(f"({','.join(map(str, item))})")
    sql_part = sql.string.split("VALUES")
    sql_str = f'{sql_part[0]} VALUES {",".join(args)}'

    async with asyncpg.create_pool(settings.db_standard, min_size=1) as pool:
        await pool.execute("TRUNCATE transactions_pool RESTART IDENTITY")
        async with pool.acquire() as conn:  # type: asyncpg.Connection
            async with conn.transaction():
                logger.info("stmt ok, execute start")
                t1 = time.perf_counter()
                await conn.execute(sql_str)
                total = time.perf_counter() - t1
            logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
            return total


async def asyncpg_many_values_group():
    sql = insert(TransactionsPool).values(data[0]).compile(dialect=PGDialect_asyncpg())
    sqls = []
    for i in range(500):
        args = []
        for x in data[i * 2000 : (i + 1) * 2000]:
            item = []
            for field in sql.positiontup:
                if x[field] is None:
                    item.append("null")
                elif isinstance(x[field], datetime):
                    item.append(f"'{x[field].isoformat()}'")
                elif isinstance(x[field], str):
                    item.append(f"'{x[field]}'")
                else:
                    item.append(x[field])
            args.append(f"({','.join(map(str, item))})")
        sql_part = sql.string.split("VALUES")
        sql_str = f'{sql_part[0]} VALUES {",".join(args)}'
        sqls.append(sql_str)
    async with asyncpg.create_pool(settings.db_standard, min_size=1) as pool:
        await pool.execute("TRUNCATE transactions_pool RESTART IDENTITY")
        async with pool.acquire() as conn:  # type: asyncpg.Connection
            async with conn.transaction():
                logger.info("stmt ok, execute start")
                t1 = time.perf_counter()
                for sql in sqls:
                    await conn.execute(sql)
                total = time.perf_counter() - t1
            logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
            return total


async def asyncpg_executemany():
    sql = insert(TransactionsPool).values(data[0]).compile(dialect=PGDialect_asyncpg())
    args = [[x[field] for field in sql.positiontup] for x in data]
    async with asyncpg.create_pool(settings.db_standard, min_size=1, max_size=20) as pool:
        await pool.execute("TRUNCATE transactions_pool RESTART IDENTITY")
        async with pool.acquire() as conn:  # type: asyncpg.Connection
            async with conn.transaction():
                logger.info("stmt ok, execute start")
                t1 = time.perf_counter()
                await conn.executemany(sql.string, args)
                total = time.perf_counter() - t1
            logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
            return total


async def asyncpg_execute_one_by_one():
    sql = insert(TransactionsPool).values(data[0]).compile(dialect=PGDialect_asyncpg())
    async with asyncpg.create_pool(settings.db_standard, min_size=1, max_size=20) as pool:
        await pool.execute("TRUNCATE transactions_pool RESTART IDENTITY")
        async with pool.acquire() as conn:  # type: asyncpg.Connection
            async with conn.transaction():
                logger.info("stmt ok, execute start")
                t1 = time.perf_counter()
                for x in data:
                    await conn.execute(sql.string, *[x[field] for field in sql.positiontup])
                total = time.perf_counter() - t1
            logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
            return total


async def asyncpg_execute_unnest():
    sql = insert(TransactionsPool).values(data[0]).compile(dialect=PGDialect_asyncpg())
    sql_part = sql.string.split("VALUES")
    sql_part2 = [f"UNNEST({x}[])" for x in map(str.strip, re.search(r"\((.*)\)", sql_part[-1])[1].split(","))]
    sql_str = f'{sql_part[0]} SELECT {",".join(sql_part2)}'
    args = [[x[field] for x in data] for field in sql.positiontup]
    async with asyncpg.create_pool(settings.db_standard, min_size=1, max_size=20) as pool:
        await pool.execute("TRUNCATE transactions_pool RESTART IDENTITY")
        async with pool.acquire() as conn:  # type: asyncpg.Connection
            async with conn.transaction():
                logger.info("stmt ok, execute start")
                t1 = time.perf_counter()
                await conn.execute(sql_str, *args)
                total = time.perf_counter() - t1
            logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
            return total


async def asyncpg_copy():
    sql = insert(TransactionsPool).values(data[0]).compile(dialect=PGDialect_asyncpg())
    async with asyncpg.create_pool(settings.db_standard, min_size=1, max_size=20) as pool:
        await pool.execute("TRUNCATE transactions_pool RESTART IDENTITY")
        async with pool.acquire() as conn:  # type: asyncpg.Connection
            async with conn.transaction():
                t1 = time.perf_counter()
                await conn.copy_records_to_table(
                    "transactions_pool",
                    records=([x[field] for field in sql.positiontup] for x in data),
                    columns=sql.positiontup,
                )
                total = time.perf_counter() - t1
            logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
            return total


def psycopg2_many_values_once():
    sql = f"INSERT INTO transactions_pool ({','.join(fields)})"
    args = []
    for x in data:
        item = []
        for field in fields:
            if x[field] is None:
                item.append("null")
            elif isinstance(x[field], datetime):
                item.append(f"'{x[field].isoformat()}'")
            elif isinstance(x[field], str):
                item.append(f"'{x[field]}'")
            else:
                item.append(x[field])
        args.append(f"({','.join(map(str, item))})")
    sql_str = f'{sql} VALUES {",".join(args)}'
    with psycopg2.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            curs.execute(sql_str)
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


def psycopg2_many_values_group():
    sql = f"INSERT INTO transactions_pool ({','.join(fields)}) "
    sqls = []
    for i in range(500):
        args = []
        for x in data[i * 2000 : (i + 1) * 2000]:
            item = []
            for field in fields:
                if x[field] is None:
                    item.append("null")
                elif isinstance(x[field], datetime):
                    item.append(f"'{x[field].isoformat()}'")
                elif isinstance(x[field], str):
                    item.append(f"'{x[field]}'")
                else:
                    item.append(x[field])
            args.append(f"({','.join(map(str, item))})")
        if args:
            sql_str = f'{sql} VALUES {",".join(args)}'
            sqls.append(sql_str)
    with psycopg2.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            for sql in sqls:
                curs.execute(sql)
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


def psycopg2_executemany():
    sql = f"INSERT INTO transactions_pool ({','.join(fields)}) VALUES ({','.join(['%s'] * len(fields))})"
    args = [[x[field] for field in fields] for x in data]
    with psycopg2.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            curs.executemany(sql, args)
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


def psycopg2_execute_one_by_one():
    sql = f"INSERT INTO transactions_pool ({','.join(fields)}) VALUES ({','.join(['%s'] * len(fields))})"
    with psycopg2.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            for x in data:
                curs.execute(sql, [x[field] for field in fields])
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


def psycopg2_execute_unnest():
    sql = insert(TransactionsPool).values(data[0]).compile(dialect=PGDialect_asyncpg())
    sql_part = sql.string.split("VALUES")
    sql_part2 = [f"UNNEST({x}[])" for x in map(str.strip, re.search(r"\((.*)\)", sql_part[-1])[1].split(","))]
    sql_part2 = re.sub(r"\$\d+", "%s", ",".join(sql_part2))
    sql_str = f"{sql_part[0]} SELECT {sql_part2}"
    args = [[x[field] for x in data] for field in sql.positiontup]
    with psycopg2.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            curs.execute(sql_str, args)
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


def psycopg2_execute_batch():
    values_params = [f"${i}" for i in range(1, len(fields) + 1)]
    sql = f"INSERT INTO transactions_pool ({','.join(fields)}) VALUES ({','.join(values_params)})"
    args = [[x[field] for field in fields] for x in data]
    with psycopg2.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            curs.execute(f"PREPARE stmt AS {sql}")
            execute_batch(curs, f"EXECUTE stmt ({','.join(['%s'] * len(fields))})", args)
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


def psycopg2_execute_values():
    sql = f"INSERT INTO transactions_pool ({','.join(fields)}) VALUES %s"
    args = [[x[field] for field in fields] for x in data]
    with psycopg2.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            execute_values(curs, sql, args)
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


def psycopg_many_values_once():
    sql = f"INSERT INTO transactions_pool ({','.join(fields)})"
    args = []
    for x in data:
        item = []
        for field in fields:
            if x[field] is None:
                item.append("null")
            elif isinstance(x[field], datetime):
                item.append(f"'{x[field].isoformat()}'")
            elif isinstance(x[field], str):
                item.append(f"'{x[field]}'")
            else:
                item.append(x[field])
        args.append(f"({','.join(map(str, item))})")
    sql_str = f'{sql} VALUES {",".join(args)}'
    with psycopg.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            curs.execute(sql_str)
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


def psycopg_many_values_group():
    sql = f"INSERT INTO transactions_pool ({','.join(fields)}) "
    sqls = []
    for i in range(500):
        args = []
        for x in data[i * 2000 : (i + 1) * 2000]:
            item = []
            for field in fields:
                if x[field] is None:
                    item.append("null")
                elif isinstance(x[field], datetime):
                    item.append(f"'{x[field].isoformat()}'")
                elif isinstance(x[field], str):
                    item.append(f"'{x[field]}'")
                else:
                    item.append(x[field])
            args.append(f"({','.join(map(str, item))})")
        if args:
            sql_str = f'{sql} VALUES {",".join(args)}'
            sqls.append(sql_str)
    with psycopg.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            with conn.transaction():
                logger.info("stmt ok, execute start")
                t1 = time.perf_counter()
                for sql in sqls:
                    curs.execute(sql)
                total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


def psycopg_executemany():
    sql = f"INSERT INTO transactions_pool ({','.join(fields)}) VALUES ({','.join(['%s'] * len(fields))})"
    args = [[x[field] for field in fields] for x in data]
    with psycopg.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            curs.executemany(sql, args)
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


def psycopg_execute_one_by_one():
    sql = f"INSERT INTO transactions_pool ({','.join(fields)}) VALUES ({','.join(['%s'] * len(fields))})"
    with psycopg.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            with conn.transaction():
                t1 = time.perf_counter()
                for x in data:
                    curs.execute(sql, [x[field] for field in fields])
                total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


def psycopg_execute_unnest():
    sql = insert(TransactionsPool).values(data[0]).compile(dialect=PGDialect_asyncpg())
    sql_part = sql.string.split("VALUES")
    sql_part2 = [f"UNNEST({x}[])" for x in map(str.strip, re.search(r"\((.*)\)", sql_part[-1])[1].split(","))]
    sql_part2 = re.sub(r"\$\d+", "%s", ",".join(sql_part2))
    sql_str = f"{sql_part[0]} SELECT {sql_part2}"
    args = [[x[field] for x in data] for field in sql.positiontup]
    with psycopg.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            curs.execute("set session jit='off'")
            t1 = time.perf_counter()
            curs.execute(sql_str, args)
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


def psycopg_copy():
    args = [[x[field] for field in fields] for x in data]
    with psycopg.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            with curs.copy(f"COPY transactions_pool ({','.join(fields)}) FROM STDIN") as copy:
                for record in args:
                    copy.write_row(record)
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


async def psycopg_async_many_values_once():
    sql = f"INSERT INTO transactions_pool ({','.join(fields)})"
    args = []
    for x in data:
        item = []
        for field in fields:
            if x[field] is None:
                item.append("null")
            elif isinstance(x[field], datetime):
                item.append(f"'{x[field].isoformat()}'")
            elif isinstance(x[field], str):
                item.append(f"'{x[field]}'")
            else:
                item.append(x[field])
        args.append(f"({','.join(map(str, item))})")
    sql_str = f'{sql} VALUES {",".join(args)}'
    async with await psycopg.AsyncConnection.connect(settings.db_standard) as conn:
        async with conn.cursor() as curs:
            await curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            await curs.execute(sql_str)
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


async def psycopg_async_many_values_group():
    sql = f"INSERT INTO transactions_pool ({','.join(fields)}) "
    sqls = []
    for i in range(500):
        args = []
        for x in data[i * 2000 : (i + 1) * 2000]:
            item = []
            for field in fields:
                if x[field] is None:
                    item.append("null")
                elif isinstance(x[field], datetime):
                    item.append(f"'{x[field].isoformat()}'")
                elif isinstance(x[field], str):
                    item.append(f"'{x[field]}'")
                else:
                    item.append(x[field])
            args.append(f"({','.join(map(str, item))})")
        if args:
            sql_str = f'{sql} VALUES {",".join(args)}'
            sqls.append(sql_str)
    async with await psycopg.AsyncConnection.connect(settings.db_standard) as conn:
        async with conn.cursor() as curs:
            await curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            async with conn.transaction():
                t1 = time.perf_counter()
                for sql in sqls:
                    await curs.execute(sql)
            total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


async def psycopg_async_executemany():
    sql = f"INSERT INTO transactions_pool ({','.join(fields)}) VALUES ({','.join(['%s'] * len(fields))})"
    args = [[x[field] for field in fields] for x in data]
    async with await psycopg.AsyncConnection.connect(settings.db_standard) as conn:
        async with conn.cursor() as curs:
            await curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            await curs.executemany(sql, args)
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


async def psycopg_async_execute_one_by_one():
    sql = f"INSERT INTO transactions_pool ({','.join(fields)}) VALUES ({','.join(['%s'] * len(fields))})"
    async with await psycopg.AsyncConnection.connect(settings.db_standard) as conn:
        async with conn.pipeline():
            async with conn.cursor() as curs:
                await curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
                logger.info("stmt ok, execute start")
                async with conn.transaction():
                    t1 = time.perf_counter()
                    for x in data:
                        await curs.execute(sql, [x[field] for field in fields])
                total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


async def psycopg_async_execute_unnest():
    sql = insert(TransactionsPool).values(data[0]).compile(dialect=PGDialect_asyncpg())
    sql_part = sql.string.split("VALUES")
    sql_part2 = [f"UNNEST({x}[])" for x in map(str.strip, re.search(r"\((.*)\)", sql_part[-1])[1].split(","))]
    sql_part2 = re.sub(r"\$\d+", "%s", ",".join(sql_part2))
    sql_str = f"{sql_part[0]} SELECT {sql_part2}"
    args = [[x[field] for x in data] for field in sql.positiontup]
    async with await psycopg.AsyncConnection.connect(settings.db_standard) as conn:
        async with conn.cursor() as curs:
            await curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            await curs.execute(sql_str, args)
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


async def psycopg_async_copy():
    args = [[x[field] for field in fields] for x in data]
    async with await psycopg.AsyncConnection.connect(settings.db_standard) as conn:
        async with conn.cursor() as curs:
            await curs.execute("TRUNCATE transactions_pool RESTART IDENTITY")
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            async with curs.copy(f"COPY transactions_pool ({','.join(fields)}) FROM STDIN") as copy:
                for record in args:
                    await copy.write_row(record)
        total = time.perf_counter() - t1
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


async def asyncpg_update_executemany():
    import asyncpg.transaction

    sql_fields = ",".join((f"{key}=${i + 1}" for i, key in enumerate(updated_fields)))
    sql_where = f"tag_id=${len(fields)}"
    sql = f"update transactions_pool set {sql_fields} where {sql_where}"
    args = [[x[field] for field in updated_fields + ["tag_id"]] for x in to_updated_data]
    async with asyncpg.create_pool(settings.db_standard, min_size=1) as pool:
        async with pool.acquire() as conn:  # type: asyncpg.Connection
            tr = conn.transaction()
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            await tr.start()
            await conn.executemany(sql, args)
            total = time.perf_counter() - t1
            await tr.rollback()
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        return total


async def psycopg_async_update_executemany():
    sql_set = ",".join(f"{key}=%s" for key in updated_fields)
    sql_where = f"tag_id=%s"
    sql = f"update transactions_pool set {sql_set} where {sql_where}"
    args = [[x[field] for field in updated_fields + ["tag_id"]] for x in to_updated_data]
    async with await psycopg.AsyncConnection.connect(settings.db_standard) as conn:
        async with conn.cursor() as curs:
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            await curs.executemany(sql, args)
            rst = curs.rowcount
        total = time.perf_counter() - t1
        await conn.rollback()
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        logger.warning(f"updated rows: {rst}")
        return total


def psycopg_update_executemany():
    sql_set = ",".join(f"{key}=%s" for key in updated_fields)
    sql = f"update transactions_pool set {sql_set} where tag_id=%s"
    args = [[x[field] for field in updated_fields + ["tag_id"]] for x in to_updated_data]
    with psycopg.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            curs.executemany(sql, args)
            rst = curs.rowcount
        total = time.perf_counter() - t1
        conn.rollback()
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        logger.warning(f"updated rows: {rst}")
        return total


def psycopg2_update_executemany():
    sql_fields = ",".join(f"{key}=%s" for key in updated_fields)
    sql = f"update transactions_pool set {sql_fields} where tag_id=%s"
    args = [[x[field] for field in updated_fields + ["tag_id"]] for x in to_updated_data]
    with psycopg2.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            curs.executemany(sql, args)
            rst = curs.rowcount
        total = time.perf_counter() - t1
        conn.rollback()
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        logger.warning(f"updated rows: {rst}")
        return total


async def asyncpg_update_unnest():
    sql_set = ",".join(f"{x}=vst.{x}" for x in updated_fields)
    sql_unnest_type = ",".join(
        f"${i + 1}::{TransactionsPool.__table__.columns[x].type.compile(PGDialect())}[]" for i, x in enumerate(fields)
    )
    sql_unnest_as = ",".join(fields)
    sql = f"UPDATE transactions_pool SET updated_at=CURRENT_TIMESTAMP, {sql_set} FROM unnest({sql_unnest_type}) AS vst ({sql_unnest_as}) WHERE transactions_pool.tag_id = vst.tag_id"
    args = [[x[field] for x in to_updated_data] for field in fields]
    async with asyncpg.create_pool(settings.db_standard, min_size=1) as pool:
        async with pool.acquire() as conn:  # type: asyncpg.Connection
            tr = conn.transaction()
            await tr.start()
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            rst = await conn.execute(sql, *args)
            total = time.perf_counter() - t1
            await tr.rollback()
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        logger.warning(f"updated rows: {rst}")
        return total


def psycopg2_update_unnest():
    place_hold = "%s"
    sql_set = ",".join(f"{x}=vst.{x}" for x in updated_fields)
    sql_unnest_type = ",".join(
        f"{place_hold}::{TransactionsPool.__table__.columns[x].type.compile(PGDialect())}[]" for x in fields
    )
    sql_unnest_as = ",".join(fields)
    sql = f"UPDATE transactions_pool SET updated_at=CURRENT_TIMESTAMP, {sql_set} FROM unnest({sql_unnest_type}) AS vst ({sql_unnest_as}) WHERE transactions_pool.tag_id = vst.tag_id"
    args = [[x[field] for x in to_updated_data] for field in fields]
    with psycopg2.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            curs.execute(sql, args)
            rst = curs.rowcount
        total = time.perf_counter() - t1
        conn.rollback()
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        logger.warning(f"updated rows: {rst}")
        return total


def psycopg_update_unnest():
    place_hold = "%s"
    sql_set = ",".join(f"{x}=vst.{x}" for x in updated_fields)
    sql_unnest_type = ",".join(
        f"{place_hold}::{TransactionsPool.__table__.columns[x].type.compile(PGDialect())}[]" for x in fields
    )
    sql_unnest_as = ",".join(fields)
    sql = f"UPDATE transactions_pool SET updated_at=CURRENT_TIMESTAMP, {sql_set} FROM unnest({sql_unnest_type}) AS vst ({sql_unnest_as}) WHERE transactions_pool.tag_id = vst.tag_id"
    args = [[x[field] for x in to_updated_data] for field in fields]
    with psycopg.connect(settings.db_standard) as conn:
        with conn.cursor() as curs:
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            curs.execute(sql, args)
            rst = curs.rowcount
        total = time.perf_counter() - t1
        conn.rollback()
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        logger.warning(f"updated rows: {rst}")
        return total


async def psycopg_async_update_unnest():
    place_hold = "%s"
    sql_set = ",".join(f"{x}=vst.{x}" for x in updated_fields)
    sql_unnest_type = ",".join(
        f"{place_hold}::{TransactionsPool.__table__.columns[x].type.compile(PGDialect())}[]" for x in fields
    )
    sql_unnest_as = ",".join(fields)
    sql = f"UPDATE transactions_pool SET updated_at=CURRENT_TIMESTAMP, {sql_set} FROM unnest({sql_unnest_type}) AS vst ({sql_unnest_as}) WHERE transactions_pool.tag_id = vst.tag_id"
    args = [[x[field] for x in to_updated_data] for field in fields]
    async with await psycopg.AsyncConnection.connect(settings.db_standard) as conn:
        async with conn.cursor() as curs:
            logger.info("stmt ok, execute start")
            t1 = time.perf_counter()
            await curs.execute(sql, args)
            rst = curs.rowcount
        total = time.perf_counter() - t1
        await conn.rollback()
        logger.warning(f"{std_inspect.stack()[0][3]} run time: {total:.2f}s")
        logger.warning(f"updated rows: {rst}")
        return total


# 百分位法
def filter_extreme_percentile(series: pd.Series, min=0, max=0.95):
    series = series.sort_values()
    q = series.quantile([min, max])
    return series[(series >= q.iloc[0]) & (series <= q.iloc[1])]


# 3σ法
def filter_extreme_3sigma(series: pd.Series, n=3):
    mean = series.mean()
    std = series.std()
    max_range = mean + n * std
    min_range = mean - n * std
    return series[(series >= min_range) & (series <= max_range)]


if __name__ == "__main__":
    # for x in [
    #     asyncpg_many_values_once,
    #     asyncpg_many_values_group,
    #     asyncpg_executemany,
    #     asyncpg_execute_one_by_one,
    #     asyncpg_execute_unnest,
    #     asyncpg_copy,
    # ]:
    #     stats = []
    #     for i in range(5):
    #         stats.append(asyncio.run(x()))
    #     s = pd.Series(stats)
    #     s = filter_extreme_percentile(s)
    #     print(s)
    #     print(x.__qualname__, "average", s.mean())
    #
    # for x in [
    #     psycopg_async_many_values_once,
    #     psycopg_async_many_values_group,
    #     psycopg_async_executemany,
    #     psycopg_async_execute_one_by_one,
    #     psycopg_async_execute_unnest,
    #     psycopg_async_copy,
    # ]:
    #     stats = []
    #     for i in range(5):
    #         stats.append(asyncio.run(x()))
    #     s = pd.Series(stats)
    #     s = filter_extreme_percentile(s)
    #     print(s)
    #     print(x.__qualname__, "average", s.mean())
    #
    # for x in [
    #     psycopg2_many_values_once,
    #     psycopg2_many_values_group,
    #     psycopg2_executemany,
    #     psycopg2_execute_one_by_one,
    #     psycopg2_execute_unnest,
    #     psycopg2_execute_batch,
    #     psycopg2_execute_values,
    # ]:
    #     stats = []
    #     for i in range(5):
    #         stats.append(x())
    #     s = pd.Series(stats)
    #     s = filter_extreme_percentile(s)
    #     print(s)
    #     print(x.__qualname__, "average", s.mean())

    for x in [
        # psycopg_many_values_once,
        # psycopg_many_values_group,
        # psycopg_executemany,
        # psycopg_execute_one_by_one,
        psycopg_execute_unnest,
        psycopg_copy,
    ]:
        stats = []
        for i in range(5):
            stats.append(x())
        s = pd.Series(stats)
        s = filter_extreme_percentile(s)
        print(s)
        print(x.__qualname__, "average", s.mean())

    asyncio.run(asyncpg_copy())
    for x in [
        psycopg_update_executemany,
        psycopg_update_unnest,
        psycopg2_update_executemany,
        psycopg2_update_unnest,
    ]:
        stats = []
        for i in range(5):
            stats.append(x())
        s = pd.Series(stats)
        s = filter_extreme_percentile(s)
        print(s)
        print(x.__qualname__, "average", s.mean())

    asyncio.run(asyncpg_copy())
    for x in [
        asyncpg_update_executemany,
        asyncpg_update_unnest,
        psycopg_async_update_executemany,
        psycopg_async_update_unnest,
    ]:
        stats = []
        for i in range(5):
            stats.append(asyncio.run(x()))
        s = pd.Series(stats)
        s = filter_extreme_percentile(s)
        print(s)
        print(x.__qualname__, "average", s.mean())
