''' LLM prompt
Create an sql statement that operates on a postgres database. Use two space indent. 

Create a table with the name <name> that has the following fields. 
id: integer primary index 
created_on: datetime defaults to current time
updated_on: datetime defaults to current time
name: string 

'''
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  created_on TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  updated_on TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  name TEXT
);
