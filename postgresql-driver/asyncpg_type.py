from datetime import datetime, UTC

import asyncpg


def timestamptz_endocder(v):
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, tz=UTC).isoformat()
    if isinstance(v, datetime):
        return v.astimezone(UTC).isoformat()
    if isinstance(v, str):
        return datetime.fromisoformat(v).astimezone(UTC).isoformat()
    raise ValueError


async def transform(conn: asyncpg.Connection):
    await conn.set_type_codec(
        schema="pg_catalog", typename="timestamptz", encoder=timestamptz_endocder, decoder=lambda x: x
    )


async def main():
    async with asyncpg.create_pool(..., min_size=1, command_timeout=60, init=transform) as pool:
        ...
