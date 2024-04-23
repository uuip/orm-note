import re

from sqlalchemy.dialects.postgresql.psycopg import PGDialectAsync_psycopg, PGDialect_psycopg


class KingBaseDialect(PGDialect_psycopg):
    name = "kingbase"
    driver = "psycopg"

    supports_statement_cache = True
    supports_server_side_cursors = True

    @classmethod
    def get_async_dialect_cls(cls, url):
        return KingBaseDialectAsync

    def _get_server_version_info(self, connection):
        v = connection.exec_driver_sql("select pg_catalog.version()").scalar()
        m = re.match(
            r"KingbaseES\s+V(\d{3})R(\d{3})C(\d{3})B(\d{4})", v, re.IGNORECASE  # 匹配人大金仓的版本号格式  # 忽略大小写
        )

        if not m:
            raise AssertionError("Could not determine version from string '%s'" % v)

        major = int(m.group(1))
        minor = int(m.group(2))
        patch = int(m.group(3))
        build = int(m.group(4))
        return major, minor, patch, build


class KingBaseDialectAsync(PGDialectAsync_psycopg):
    name = "kingbase"
    driver = "psycopg"
    is_async = True
    supports_statement_cache = True

    _get_server_version_info = KingBaseDialect._get_server_version_info
