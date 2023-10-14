''' LLM prompt
Create a python class that enables operations on a postgres database using psycopg2. Use two space indent. I need the following FUNCTION support.

FUNCTION

__init__()
__enter__()
__exit__()
connect_with_url(url) 
upsert(table_name, _dict) - insert or update a row in a table if id is present in the _dict
delete(table_name, id) - delete a row in the database based on id
get(table_name, id) - get a row in the database based on id
get_all(table_name, page, limit) - get all rows in a table based on page and limit
run_sql(sql) - run a raw sql statement

get_table_definition(table_name) - get table definitions in a 'create table' format directly from postgres, that doesnt use pg_dump
get_all_table_names() - get all table names in the database as a string list
get_table_definitions_for_prompt() - combine get_table_definition() and get_all_table_names() to get a list of table definitons in a 'create table' format for all tables in the database as a string that can be used for llm prompt
'''
import psycopg2
from psycopg2 import sql

class PostgresDBManager:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
      if self.cursor:
        self.cursor.close()
      if self.conn:
        self.conn.close()

    def connect_with_url(self, url):
        self.conn = psycopg2.connect(url)
        self.cursor = self.conn.cursor()

    def upsert(self, table_name, _dict):
        columns = ", ".join(_dict.keys())
        values = [str(value) for value in _dict.values()]
        updates = ", ".join(f"{col} = EXCLUDED.{col}" for col in _dict.keys())
        # Create SQL for INSERT
        insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(map(sql.Placeholder, columns))
        )
        # Create SQL for UPDATE
        update_sql = sql.SQL("ON CONFLICT (id) DO UPDATE SET {}").format(
            sql.SQL(', ').join(
                sql.SQL('{} = EXCLUDED.{}').format(
                    sql.Identifier(column),
                    sql.Identifier(column)
                ) for column in columns if column != 'id'
            )
        )
        # Combine INSERT and UPDATE for UPSERT
        upsert_sql = sql.SQL("{} {}").format(insert_sql, update_sql)
        # Execute UPSERT
        self.cursor.execute(upsert_sql, _dict)
        #self.cursor.execute(f"INSERT INTO {table_name} ({cols}) VALUES {(values,)} ON CONFLICT (id) DO UPDATE SET {updates};")
        self.conn.commit()

    def delete(self, table_name, id):
        self.cursor.execute(f"DELETE FROM {table_name} WHERE id = %s;", (id,))
        self.conn.commit()

    def get(self, table_name, id):
        self.cursor.execute(f"SELECT * FROM {table_name} WHERE id = %s;", (id,))
        return self.cursor.fetchone()

    def get_all(self, table_name, page, limit):
        offset = (page - 1) * limit
        self.cursor.execute(f"SELECT * FROM {table_name} LIMIT %s OFFSET %s;", (limit, offset))
        return self.cursor.fetchall()

    def run_sql(self, sql):
        self.cursor.execute(sql)
        self.conn.commit()

    def get_table_definition(self, table_name):
      self.cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';")
      return self.cursor.fetchone()

    def get_all_table_names(self):
        self.cursor.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")
        return [row[0] for row in self.cursor.fetchall()]

    def get_table_definitions_for_prompt(self):
        table_names = self.get_all_table_names()
        return "\n".join([self.get_table_definition(name) for name in table_names])
