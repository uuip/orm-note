import psycopg
from sqlalchemy import Engine

from conf import settings


def psycopg2_copy_from(db: Engine):
    with db.raw_connection().driver_connection as conn:
        with conn.cursor() as cursor:
            cursor.copy_from(open(r"data.dump", "rb"), "table")
            conn.commit()


def psycopg2_copy_to(db: Engine):
    with db.raw_connection().driver_connection as conn:
        with conn.cursor() as cursor:
            cursor.copy_to(open(r"data.dump", "wb+"), "table")
            conn.commit()


def psycopg_copy_from():
    src_file = "/Users/sharp/Downloads/GeoLite2-City-Blocks-IPv4.csv"
    with psycopg.connect(...) as conn, open(src_file, "rb") as f:
        with conn.cursor().copy("COPY geoip2_network FROM STDIN with (format csv, header)") as copy:
            while data := f.read(20 * 1024**2):
                copy.write(data)
    src_file = "data.dump"

    with psycopg.connect(settings.db_psycopg) as conn, open(src_file, "rb") as f:
        with conn.cursor().copy("COPY address(bsc_addr,tron_addr) FROM STDIN") as copy:
            while data := f.read():
                copy.write(data)


def psycopg_copy_to():
    dst_file = "data.dump"
    with psycopg.connect(...) as conn, open(dst_file, "wb+") as f:
        with conn.cursor().copy("COPY geoip2_network TO STDOUT") as copy:
            for data in copy:
                f.write(data)
    dst_file = "data.dump"
    with psycopg.connect(settings.db_psycopg) as conn, open(dst_file, "wb+") as f:
        with conn.cursor().copy("COPY (select bsc_addr,tron_addr from address) TO STDOUT") as copy:
            for data in copy:
                f.write(data)
