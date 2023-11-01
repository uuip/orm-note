export PGHOST=127.0.0.1
export PGPORT=5432
export PGUSER=postgres
export PGPASSWORD=postgres
export PGDATABASE=trees
export dsn=

pg_dump  -d trees -t network -f tx.dump -a 只数据
-s 只结构
pg_dump $dsn --if-exists -c -Ox -t transactions -f tx.dump
psql -f tx.dump

pg_dump $dsn -Ft -Ox -t transactions -f tx.dump
pg_restore --if-exists -c -Ox -d trees tx.dump