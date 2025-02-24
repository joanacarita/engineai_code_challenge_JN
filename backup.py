import streamlit as st
import pandas as pd
import math
from pathlib import Path
import snowflake.connector

# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title='GDP dashboard',
    page_icon=':earth_americas:', # This is an emoji shortcode. Could be a URL too.
)

# -----------------------------------------------------------------------------
# Declare some useful functions.

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

def test_connection(conn):
    try:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT 
            sector_name, 
            position_usd 
            FROM CODE_CHALLENGE_THI44K287NGA.SOURCE.sector_position 
            where date = (select max(date) from CODE_CHALLENGE_THI44K287NGA.SOURCE.sector_position )
            ORDER BY date, position_usd DESC 
        LIMIT 10
        ''')
        version = cursor.fetchall()
        cursor.close()
        return version
    except Exception as e:
        return str(e)

@st.cache_data
def get_gdp_data():
    """Grab GDP data from a CSV file.

    This uses caching to avoid having to read the file every time. If we were
    reading from an HTTP endpoint instead of a file, it's a good idea to set
    a maximum age to the cache with the TTL argument: @st.cache_data(ttl='1d')
    """

    # Instead of a CSV on disk, you could read from an HTTP endpoint here too.
    DATA_FILENAME = Path(__file__).parent/'data/gdp_data.csv'
    raw_gdp_df = pd.read_csv(DATA_FILENAME)

    MIN_YEAR = 1960
    MAX_YEAR = 2022

    # The data above has columns like:
    # - Country Name
    # - Country Code
    # - [Stuff I don't care about]
    # - GDP for 1960
    # - GDP for 1961
    # - GDP for 1962
    # - ...
    # - GDP for 2022
    #
    # ...but I want this instead:
    # - Country Name
    # - Country Code
    # - Year
    # - GDP
    #
    # So let's pivot all those year-columns into two: Year and GDP
    gdp_df = raw_gdp_df.melt(
        ['Country Code'],
        [str(x) for x in range(MIN_YEAR, MAX_YEAR + 1)],
        'Year',
        'GDP',
    )

    # Convert years from string to integers
    gdp_df['Year'] = pd.to_numeric(gdp_df['Year'])

    return gdp_df


gdp_df = get_gdp_data()

# -----------------------------------------------------------------------------
# Draw the actual page

# Set the title that appears at the top of the page.
'''
# :earth_americas: GDP dashboard

Browse GDP data from the [World Bank Open Data](https://data.worldbank.org/) website. As you'll
notice, the data only goes to 2022 right now, and datapoints for certain years are often missing.
But it's otherwise a great (and did I mention _free_?) source of data.
'''

# Add some spacing
''
''

min_value = gdp_df['Year'].min()
max_value = gdp_df['Year'].max()

from_year, to_year = st.slider(
    'Which years are you interested in?',
    min_value=min_value,
    max_value=max_value,
    value=[min_value, max_value])

countries = gdp_df['Country Code'].unique()

if not len(countries):
    st.warning("Select at least one country")

selected_countries = st.multiselect(
    'Which countries would you like to view?',
    countries,
    ['DEU', 'FRA', 'GBR', 'BRA', 'MEX', 'JPN'])

''
''
''

# Filter the data
filtered_gdp_df = gdp_df[
    (gdp_df['Country Code'].isin(selected_countries))
    & (gdp_df['Year'] <= to_year)
    & (from_year <= gdp_df['Year'])
]

st.header('GDP over time', divider='gray')

''

st.line_chart(
    filtered_gdp_df,
    x='Year',
    y='GDP',
    color='Country Code',
)

''
''


first_year = gdp_df[gdp_df['Year'] == from_year]
last_year = gdp_df[gdp_df['Year'] == to_year]

st.header(f'GDP in {to_year}', divider='gray')

''

cols = st.columns(4)

for i, country in enumerate(selected_countries):
    col = cols[i % len(cols)]

    with col:
        first_gdp = first_year[first_year['Country Code'] == country]['GDP'].iat[0] / 1000000000
        last_gdp = last_year[last_year['Country Code'] == country]['GDP'].iat[0] / 1000000000

        if math.isnan(first_gdp):
            growth = 'n/a'
            delta_color = 'off'
        else:
            growth = f'{last_gdp / first_gdp:,.2f}x'
            delta_color = 'normal'

        st.metric(
            label=f'{country} GDP',
            value=f'{last_gdp:,.0f}B',
            delta=growth,
            delta_color=delta_color
        )

import streamlit as st
import pandas as pd
import altair as alt

# Sample Data
data = {
    "Sector": [
        "Technology", "Healthcare", "Finance", "Education", "Energy",
        "Retail", "Manufacturing", "Construction", "Entertainment", "Transportation"
    ],
    "Work Satisfaction Score": [9.5, 8.8, 8.5, 8.2, 8.0, 7.8, 7.5, 7.2, 7.0, 6.8]
}

df = pd.DataFrame(data).sort_values(by="Work Satisfaction Score", ascending=True)

connection = _get_snowflake_connection()  # Establishing connection to Snowflake
df_sector_position = test_connection(connection)  # Test the connection and fetch version info

#print(df_sector_position)

# Query Snowflake Table
query = '''
        SELECT 
            sector_name, 
            position_usd 
            FROM CODE_CHALLENGE_THI44K287NGA.SOURCE.sector_position 
            where date = (select max(date) from CODE_CHALLENGE_THI44K287NGA.SOURCE.sector_position )
            ORDER BY date, position_usd DESC 
        LIMIT 10
        ''' 

# Read data into Pandas DataFrame
df_sector_position = pd.read_sql(query, connection)

# Close the connection
connection.close()

st.write("Snowflake Version:", df_sector_position)  # Display the Snowflake version information

# Convert numeric values to float (if necessary)
#df_sector_position["position_usd"] = df_sector_position["position_usd"].astype(float)

# Create Altair horizontal bar chart
chart = alt.Chart(df_sector_position).mark_bar().encode(
    x=alt.X("position_usd:Q", title="Position USD"),
    y=alt.Y("sector_name:N", sort="-x", title="Sector Name"),
    color=alt.Color("sector_name:N", legend=None)
).properties(title="Top 10 Sectors by Position USD")

# Streamlit UI
st.title("Top 10 Best Sectors to Work In")
st.altair_chart(chart, use_container_width=True)