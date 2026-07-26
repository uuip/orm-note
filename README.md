# orm-note

SQLAlchemy 使用笔记和小型兼容性示例。

## SQLAlchemy 笔记

### PostgreSQL 和 MySQL 下的 Boolean 取值

以下取值已经在本项目中验证过，适用于 PostgreSQL 和 MySQL 下的 SQLAlchemy
`Boolean` 字段示例。

| 方言 | 场景 | 取值 |
| --- | --- | --- |
| PostgreSQL | ORM insert | `True`, `1`, `text("true")` |
| PostgreSQL | Raw SQL | `True`, `"1"`, `"true"` |
| PostgreSQL | ORM default | `"true"`, `text("true")`, `"1"` |
| MySQL | ORM insert | `True`, `text("true")`, `1`, `text("1")` |
| MySQL | Raw SQL | `True`, `1`, `"1"`, `text("1")` |
| MySQL | ORM default | `"1"`, `text("1")`, `text("true")` |

### INSERT 和 WHERE 中 DateTime 字符串的绑定差异

`INSERT` 时，参数绑定在明确的目标列上：

```python
insert(ReviewType).values(created_at="2026-04-29 02:52:04")
```

SQLAlchemy 知道 `created_at` 是 `DateTime` 列，因此 PostgreSQL 可以收到带时间类型的绑定参数：

```sql
VALUES (%(created_at)s::TIMESTAMP WITHOUT TIME ZONE)
```

数据库随后可以把字符串按时间解析。

但在 `WHERE` 比较中：

```python
ReviewType.created_at == "2026-04-29 02:52:04"
```

右侧 Python 值本身会参与类型推断。因为右侧值是 `str`，PostgreSQL 可能收到被标为
`VARCHAR` 的绑定参数：

```sql
%(created_at_1)s::VARCHAR
```

### 连接时区参数

PostgreSQL 时区示例：

```python
os.environ["PGTZ"] = "Asia/Tokyo"
```

对 `psycopg` 来说，生效优先级为：

```text
PGTZ > connect_args > URL parameters
```

URL 参数可选转义：

```python
f"?options={quote_plus('-c timezone=Asia/Tokyo')}"
```

`connect_args` 的值会直接传给驱动，不需要 URL 转义：

```python
connect_args={"options": "-c TimeZone=Asia/Tokyo"}
```

PyMySQL 时区示例：

URL 参数需要转义：

```python
f'?init_command={quote_plus("SET time_zone='+08:00'")}'
```

`connect_args` 的值会直接传给驱动，不需要 URL 转义：

```python
connect_args={"init_command": "SET time_zone='+09:00'"}
```

```
aiomysql+uvloop:
https://github.com/aio-libs/aiomysql/issues/966
asyncpg:
cannot use Connection.transaction() in a manually started transaction
```