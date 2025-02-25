import streamlit as st
import pandas as pd
import snowflake.connector
import altair as alt

# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title='Engine AI Code Challenge',
    page_icon=':chart_with_upwards_trend:', 
)

# -----------------------------------------------------------------------------

# Function to establish a connection to Snowflake
def _get_snowflake_connection():
    credentials = {
    "user": "guest_THI44K287NGA",
    "password": "Y}[xk;LXz2ZmVPU<bp&_TA",
    "account": "ui76830.west-europe.azure",
    "database": "CODE_CHALLENGE_THI44K287NGA",
    "schema": "source",
    "warehouse": "guest_code_challenge_THI44K287NGA",
    "role": "guest_code_challenge_THI44K287NGA",
    }
    return snowflake.connector.connect(**credentials)

st.title("Engine AI Code Challenge by Joana Nunes")

################### Q1 ###################
# Show the top 10 sectors by position at the most recent date.

connection = _get_snowflake_connection()  # Establishing connection to Snowflake


# Query Snowflake View built on the Data Exercise, that returns the daily sector position in USD for every day.
query_q1 = '''
        SELECT 
            sector_name, 
            position_usd, 
            date
            FROM CODE_CHALLENGE_THI44K287NGA.SOURCE.sector_position 
        WHERE date = (select max(date) from CODE_CHALLENGE_THI44K287NGA.SOURCE.sector_position )
        ORDER BY position_usd DESC 
        LIMIT 10
        ''' 

# Read data into Pandas DataFrame
df_sector_position = pd.read_sql(query_q1, connection)

# Ensure column names have no spaces or special characters and lower case
df_sector_position.columns = df_sector_position.columns.str.replace(" ", "_")
df_sector_position.columns = df_sector_position.columns.str.lower()


# Define latest date in analisys 
latest_date = df_sector_position['date'].max()

# Sort DataFrame by position_usd
df_sector_position = df_sector_position[["sector_name", "position_usd"]].sort_values(by="position_usd", ascending=True)

# Convert position_usd to millions
df_sector_position["position_usd_abbr"] = df_sector_position["position_usd"].apply(lambda x: x / 1000000)

# Create Altair horizontal bar chart
chart = alt.Chart(df_sector_position).mark_bar().encode(
    x=alt.X("position_usd_abbr:Q", title="Position USD in Millions"),
    y=alt.Y("sector_name:N", sort="-x", title="Sector Name", axis=alt.Axis(labelLimit=300, titlePadding=20)),
    color=alt.Color("sector_name:N", legend=None)
).properties(title=f"Top 10 Sectors by Position USD at {latest_date}")

# Streamlit UI
st.subheader(f"Top 10 Sectors by Position USD at {latest_date}")
st.altair_chart(chart, use_container_width=True)

################### Q2 ###################
# Show a table containing thetop 25% companies found in Data Exercise

# Query Snowflake View built on the Data Exercise, that returns the the top 25% companies with the largest average position (USD) in the last year
query_top_avg_position = '''
        SELECT 
            company_id, 
            ticker, 
            sector_name, 
            avg_position_usd
            FROM CODE_CHALLENGE_THI44K287NGA.SOURCE.top_avg_position
        '''

df_top_avg_position = pd.read_sql(query_top_avg_position, connection)

# Query Snowflake table position and create a row number variable to then pick the latest record
query_latest_shares = '''
        SELECT 
            company_id, 
            shares,
            ROW_NUMBER() OVER(PARTITION BY COMPANY_ID ORDER BY DATE DESC) as row_num
            FROM CODE_CHALLENGE_THI44K287NGA.SOURCE.position
        '''

df_latest_shares = pd.read_sql(query_latest_shares, connection)

# Query Snowflake table price and create a row number variable to then pick the latest record
query_latest_close_price_usd = '''
        SELECT 
            company_id, 
            close_usd,
            ROW_NUMBER() OVER(PARTITION BY COMPANY_ID ORDER BY DATE DESC) as row_num
            FROM CODE_CHALLENGE_THI44K287NGA.SOURCE.price
        '''

df_latest_close_price_usd = pd.read_sql(query_latest_close_price_usd, connection)

# Pick the latest record for postion and price dataframes
df_latest_shares_filtered = df_latest_shares[df_latest_shares['ROW_NUM'] == 1]
df_latest_close_price_usd_filtered = df_latest_close_price_usd[df_latest_close_price_usd['ROW_NUM'] == 1]

# Convert company_id from bytes to hex
for df in [df_top_avg_position, df_latest_shares_filtered, df_latest_close_price_usd_filtered]:
    df["COMPANY_ID"] = df["COMPANY_ID"].apply(lambda x: x.hex())

# Join the DataFrames on company_id
joined_df  = df_top_avg_position.merge(df_latest_shares_filtered, on="COMPANY_ID").merge(df_latest_close_price_usd_filtered, on="COMPANY_ID")

# Convert column names to lowercase
joined_df.columns = joined_df.columns.str.lower()

# Streamlit UI
st.subheader("TOP 25% of Companies by Average Position USD in the last year")

# Show table
st.table(joined_df[['ticker', 'sector_name', 'shares', 'close_usd', 'avg_position_usd']]) 

################## Q3 ###################
# Show select box with all the companies available on Data Exercise that allows to choose one company and show a timeseries line chart with the daily close price.

# Query Snowflake View built on the Data Exercise that returns the daily closing price (USD) for every company
query_companies = '''
        SELECT 
            ticker, 
            close_usd,
            date
            FROM CODE_CHALLENGE_THI44K287NGA.SOURCE.position_usd
        '''

# Read data into Pandas DataFrame
df_companies = pd.read_sql(query_companies, connection)

# Convert column names to lowercase
df_companies.columns = df_companies.columns.str.lower()

# Streamlit UI
st.subheader("Company Stock Price Viewer")

# Select a company
selected_company = st.selectbox("Choose a Company:", df_companies["ticker"].unique())

# Filter data for selected company
filtered_data = df_companies[df_companies["ticker"] == selected_company]

# Create Altair Line Chart with Labels
chart = alt.Chart(filtered_data).mark_line().encode(
    x=alt.X("date:T", title="Date"),  # X-axis label
    y=alt.Y("close_usd:Q", title="Closing Price (USD)"),  # Y-axis label
    tooltip=["date", "close_usd"]
).properties(
    title=f"Daily Closing Price for {selected_company}"
)

# Show chart in Streamlit
st.altair_chart(chart, use_container_width=True)

#
# Close the connection
connection.close()