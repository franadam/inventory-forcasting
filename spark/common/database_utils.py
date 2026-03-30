import psycopg2
from psycopg2.extras import RealDictCursor

from spark.common.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, SQL_ROOT

def get_conn_cursor():
    conn = psycopg2.connect(
        host='inventory-postgres',
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()

def create_tables():
    conn, cur = get_conn_cursor()

    sql_path = f"{SQL_ROOT}/tables/bronze_tables.sql"
    
    with open(sql_path, "r") as f:
        sql = f.read()

    for statement in sql.split(";"):
        stmt = statement.strip()
        if stmt:
            cur.execute(stmt)

    conn.commit()
    close_conn_cursor(conn, cur)