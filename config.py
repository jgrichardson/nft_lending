import pandas as pd
from pathlib import Path
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import inspect

# Load .env environment variables
load_dotenv()

# Scheme: "postgres+psycopg2://<USERNAME>:<PASSWORD>@<IP_ADDRESS>:<PORT>/<DATABASE_NAME>"
database_connection_string = os.getenv("DATABASE_URI")

engine = create_engine(database_connection_string)

inspector = inspect(engine)
print(inspector.get_table_names("public"))

sql_query = """
SELECT *
FROM "Customer"
"""

customer_df = pd.read_sql_query(sql_query, con = engine)

print(customer_df.head(10))

sql_insert = """
INSERT INTO "Customer" (customer_id, first_name, last_name)
VALUES (10, 'Test2', 'Test2')
"""

engine.execute(sql_insert)


customer_df = pd.read_sql_query(sql_query, con = engine)

print(customer_df.head(10))

