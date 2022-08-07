import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import inspect
import requests
import hvplot.pandas




load_dotenv()

database_connection_string = os.getenv("DATABASE_URI")

database_schema = os.getenv("DATABASE_SCHEMA")

engine = create_engine(database_connection_string, echo=False)

sql_query = """
SELECT *
FROM collection
"""

df = pd.read_sql_query(sql_query, con = engine)

df.head()