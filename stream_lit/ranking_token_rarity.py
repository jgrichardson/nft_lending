import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import inspect
import requests
import hvplot.pandas
import plost 

load_dotenv()

database_connection_string = os.getenv("DATABASE_URI")

database_schema = os.getenv("DATABASE_SCHEMA")

engine = create_engine(database_connection_string, echo=False)


sql_query = """
SELECT rarity_percentage
FROM token_attribute
ORDER BY rarity_percentage DESC
"""

df = pd.read_sql_query(sql_query, con = engine)

print(df.head(10))
print(df.tail(10))

#df.plot(title="Ranking Token Rarity")
plost.bar_chart(
    data=df['rarity_percentage'],
    bar='percentage',
    value='%')

    