import pytest
import snowflake.connector
import snowflake.connector.cursor
import snowflake.connector.errors
import sqlglot

from fakesnow.transforms.ddl import alter_table_add_multiple_columns, alter_table_strip_cluster_by


def test_alter_table_add_multiple_columns() -> None:
    # Test multiple columns are split into separate statements
    sql = "alter table tab1 add column col1 int, col2 varchar(50), col3 boolean"
    result = alter_table_add_multiple_columns(sqlglot.parse_one(sql, dialect="snowflake"))

    assert len(result) == 3
    assert result[0].sql() == "ALTER TABLE tab1 ADD COLUMN col1 INT"
    assert result[1].sql() == "ALTER TABLE tab1 ADD COLUMN col2 VARCHAR(50)"
    assert result[2].sql() == "ALTER TABLE tab1 ADD COLUMN col3 BOOLEAN"

    # Test multiple columns with mix with one column having IF NOT EXISTS
    sql_if_not_exists = "alter table tab1 add column if not exists col1 int, col2 varchar(50)"
    result_if_not_exists = alter_table_add_multiple_columns(sqlglot.parse_one(sql_if_not_exists, dialect="snowflake"))

    assert len(result_if_not_exists) == 2
    assert result_if_not_exists[0].sql() == "ALTER TABLE tab1 ADD COLUMN IF NOT EXISTS col1 INT"
    assert result_if_not_exists[1].sql() == "ALTER TABLE tab1 ADD COLUMN col2 VARCHAR(50)"


def test_alter_table_strip_cluster_by() -> None:
    success_sql = "SELECT 'Statement executed successfully.' AS status"

    # ALTER TABLE ... CLUSTER BY
    assert (
        sqlglot.parse_one("alter table table1 cluster by (name)").transform(alter_table_strip_cluster_by).sql()
        == success_sql
    )

    # ALTER TABLE ... CLUSTER BY with multiple columns
    assert (
        sqlglot.parse_one("alter table table1 cluster by (name, id)").transform(alter_table_strip_cluster_by).sql()
        == success_sql
    )

    # ALTER TABLE ... DROP CLUSTERING KEY
    assert (
        sqlglot.parse_one("alter table table1 drop clustering key", read="snowflake")
        .transform(alter_table_strip_cluster_by)
        .sql()
        == success_sql
    )

    # ALTER TABLE ... RESUME RECLUSTER
    assert (
        sqlglot.parse_one("alter table table1 resume recluster", read="snowflake")
        .transform(alter_table_strip_cluster_by)
        .sql()
        == success_sql
    )

    # ALTER TABLE ... SUSPEND RECLUSTER
    assert (
        sqlglot.parse_one("alter table table1 suspend recluster", read="snowflake")
        .transform(alter_table_strip_cluster_by)
        .sql()
        == success_sql
    )

    # Regular ALTER TABLE should not be affected
    regular_alter = sqlglot.parse_one("alter table table1 add column name varchar(20)")
    assert regular_alter.transform(alter_table_strip_cluster_by).sql() != success_sql


def test_qualified_temporary_view(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create or replace temporary view db1.schema1.tv as select 1 as a")
    assert cur.fetchall() == [("View TV successfully created.",)]
    cur.execute("select a from db1.schema1.tv")
    assert cur.fetchall() == [(1,)]


def test_qualified_temporary_table(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create or replace temporary table "DB1"."SCHEMA1"."TT" as select 1 as a')
    assert cur.fetchall() == [("Table TT successfully created.",)]
    cur.execute("insert into db1.schema1.tt values (2)")
    cur.execute("delete from db1.schema1.tt where a = 1")
    cur.execute("select a from schema1.tt")
    assert cur.fetchall() == [(2,)]


def test_qualified_temporary_table_describe_and_drop(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create temporary table db1.schema1.tt (a int)")
    cur.execute("describe table db1.schema1.tt")
    assert [r[0] for r in cur.fetchall()] == ["A"]
    cur.execute("drop table if exists db1.schema1.tt")
    assert cur.fetchall() == [("TT successfully dropped.",)]


def test_qualified_temporary_table_shadows_permanent(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table db1.schema1.t (a int)")
    cur.execute("insert into db1.schema1.t values (1)")
    cur.execute("create temporary table db1.schema1.t (a int)")
    cur.execute("select count(*) from db1.schema1.t")
    assert cur.fetchall() == [(0,)]
    cur.execute("drop table db1.schema1.t")
    cur.execute("select a from db1.schema1.t")
    assert cur.fetchall() == [(1,)]


def test_qualified_temporary_table_not_visible_from_other_schema(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema db1.other")
    cur.execute("create temporary table db1.schema1.tt (a int)")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not exist"):
        cur.execute("select * from db1.other.tt")


def test_qualified_temporary_table_is_session_isolated(conn: snowflake.connector.SnowflakeConnection):
    conn.cursor().execute("create temporary table db1.schema1.tt (a int)")
    with (
        snowflake.connector.connect(database="db1", schema="schema1") as other,
        pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not exist"),
    ):
        other.cursor().execute("select * from db1.schema1.tt")
