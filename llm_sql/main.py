import os 
from dotenv import load_dotenv
from llm_sql.modules.db import PostgresDBManager

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def main(): 

  # parse prompt using arg parse

  # connect to db
  with PostgresDBManager() as db:
    db.connect_with_url(DATABASE_URL)
    print(db.get_all_table_names())
    print(db.get_table_definition('users'))
    print(db.get_table_definitions_for_prompt())

    # db.upsert("users", {"id": 1, "name": "Alice"})
    # print(db.get("users", 1))
    # db.delete("users", 1)
  pass

if __name__ == 'main':
  main()