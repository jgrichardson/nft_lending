import pandas as pd
import os
from dotenv import load_dotenv
import hvplot.pandas
from sqlalchemy import create_engine
from sqlalchemy import inspect
import altair as alt
import holoviews as hv
hv.extension('bokeh')

# Libraries needed for streamlit and integrating plotting with plost
import streamlit as st
import plost


# Support bokeh extension
hv.extension('bokeh', logo=False)

# Set the streamlit page layout to wide
st.set_page_config(layout='wide')

load_dotenv()

rarify_api_key = os.getenv("RARIFY_API_KEY")
database_uri = os.getenv("DATABASE_URL")
database_schema = os.getenv("DATABASE_SCHEMA")

engine = create_engine(database_uri, echo = False)
inspector = inspect(engine)

# Define the base time-series chart.
def get_chart(data):
	hover = alt.selection_single(
		fields=["year_month_day"],
		nearest=True,
		on="mouseover",
		empty="none",
	)

	lines = (
		alt.Chart(data, title="Average Price (ETH)")
		.mark_line()
		.encode(
			x=alt.X("yearmonthdate(year_month_day)", axis=alt.Axis(title="Month/Year")),
			y=alt.Y("avg_price", axis=alt.Axis(title="ETH")),
			color="collection",
		)
	)

	# Draw points on the line, and highlight based on selection
	points = lines.transform_filter(hover).mark_circle(size=65)

	# Draw a rule at the location of the selection
	tooltips = (
		alt.Chart(data)
		.mark_rule()
		.encode(
			x="year_month_day",
			y="avg_price",
			opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
			tooltip=[
				alt.Tooltip("year_month_day", title="Month/Year"),
				alt.Tooltip("avg_price", title="Price (ETH)"),
			]
		)
		.add_selection(hover)
	)

	return (lines + points + tooltips).interactive()



sql_query = f"""
SELECT c.name as collection,
	   DATE_TRUNC('month', t.timestamp) as year_month_day,
	   AVG(t.avg_price) as avg_price
FROM {database_schema}.trade t
INNER JOIN {database_schema}.collection c ON c.contract_id = t.contract_id
INNER JOIN {database_schema}.network n ON n.network_id = c.network_id
WHERE n.network_id = 'ethereum' 
AND c.name IN ('CryptoPunks', 'BoredApeYachtClub', 'MutantApeYachtClub', 'Otherdeed', 'Azuki', 'CloneX', 'Moonbirds', 'Doodles', 'Meebits', 'Cool Cats', 'BoredApeKennelClub')
AND DATE_TRUNC('month', t.timestamp) > '2020-12-31'
GROUP BY c.name, DATE_TRUNC('month', t.timestamp)
HAVING MIN(avg_price) > 0.0
ORDER BY SUM(t.volume)  DESC    
"""

collections_df = pd.read_sql_query(sql_query, con = engine) 
#print(collections_df)   

chart = get_chart(collections_df)

# Add first annotation
ANNOTATION1 = [
	("Sept 30, 2021", "Best month ever for CryptoPunks! 🥳"),
]

annotations1_df = pd.DataFrame(ANNOTATION1, columns=["date", "event"])
annotations1_df.date = pd.to_datetime(annotations1_df.date)
annotations1_df["y"] = 420

annotation1_layer = (
	alt.Chart(annotations1_df)
	.mark_text(size=20, text="⬇", dx=-8, dy=-10, align="left")
	.encode(
		x="date:T",
		y=alt.Y("y:Q"),
		tooltip=["event"],
	)
	.interactive()
)

# Add second annotation
ANNOTATION2 = [
	("Jan 31, 2022", "Highest average price for BoredApeYachtClub!"),
]

annotations2_df = pd.DataFrame(ANNOTATION2, columns=["date", "event"])
annotations2_df.date = pd.to_datetime(annotations2_df.date)
annotations2_df["y"] = 400

annotation2_layer = (
	alt.Chart(annotations2_df)
	.mark_text(size=20, text="⬇", dx=-8, dy=-10, align="left")
	.encode(
		x="date:T",
		y=alt.Y("y:Q"),
		tooltip=["event"],
	)
	.interactive()
)

# Add third annotation
ANNOTATION3 = [
	("April 30, 2022", "NFT Market took a nose dive! 😰"),
]

annotations3_df = pd.DataFrame(ANNOTATION3, columns=["date", "event"])
annotations3_df.date = pd.to_datetime(annotations3_df.date)
annotations3_df["y"] = 80

annotation3_layer = (
	alt.Chart(annotations3_df)
	.mark_text(size=20, text="⬇", dx=-8, dy=-10, align="left")
	.encode(
		x="date:T",
		y=alt.Y("y:Q"),
		tooltip=["event"],
	)
	.interactive()
)

st.altair_chart(
	(chart + annotation1_layer + annotation2_layer + annotation3_layer).interactive(),
	use_container_width=True
)


