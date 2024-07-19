import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh

# Function to fetch candle data from the API
def fetch_candle_data(pair, from_time, to_time):
    url = f"http://localhost:8080/trades/{pair}/candles?from={from_time}&to={to_time}"
    response = requests.get(url)
    data = response.json()
    return data['candles']

# Function to fetch stats data from the API
def fetch_stats_data(pair):
    url = f"http://localhost:8080/trades/{pair}/stats"
    response = requests.get(url)
    return response.json()

# Convert candle data to DataFrame
def candles_to_dataframe(candles):
    df = pd.DataFrame(candles)
    df['date'] = pd.to_datetime(df['date'])
    return df

# Initialize session state for storing candle data and selected pair
if 'candles' not in st.session_state:
    st.session_state.candles = []
if 'pair' not in st.session_state:
    st.session_state.pair = "BTCUSDT"

# Streamlit app
st.title("Binance Candlestick Visualization")

# Menu for selecting trading pair
selected_pair = st.selectbox("Select trading pair", ["BTCUSDT", "ETHUSDT"])

# Update pair and reset candles if the selected pair changes
if selected_pair != st.session_state.pair:
    st.session_state.pair = selected_pair
    st.session_state.candles = []

# Input fields for time range
from_time = (datetime.utcnow() - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
to_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# Auto-refresh every 5 seconds
count = st_autorefresh(interval=5000, limit=None, key="autorefresh")

# Fetch and display stats data
stats = fetch_stats_data(st.session_state.pair)

st.header(f"Statistics for {st.session_state.pair}")
col1, col2, col3 = st.columns(3)
col1.metric("Trades Over Last Hour", round(stats["trades_over_last_hour"],2))
col2.metric("Volume Over Last Hour", round(stats["volume_over_last_hour"],2))
col3.metric("Average Price Over Last Hour", round(stats["average_price_over_last_hour"],2))

# Fetch and display candle data
if count == 0 or count % 1 == 0:
    candles = fetch_candle_data(st.session_state.pair, from_time, to_time)
    if candles:
        df = candles_to_dataframe(candles)
        st.session_state.candles.extend(candles)
        # Remove duplicates if any
        st.session_state.candles = [dict(t) for t in {tuple(d.items()) for d in st.session_state.candles}]
        df = candles_to_dataframe(st.session_state.candles)

        # Create a collapsible section for the candle data table
        with st.expander("Candle Data Table"):
            st.write(df)
        
        # Plotting the candlestick chart
        fig = go.Figure(data=[go.Candlestick(x=df['date'],
                                             open=df['open'],
                                             high=df['highest'],
                                             low=df['lowest'],
                                             close=df['close'])])

        fig.update_layout(title=f'Candlestick chart for {st.session_state.pair}',
                          xaxis_title='Time',
                          yaxis_title='Price',
                          xaxis_rangeslider_visible=False)
        st.plotly_chart(fig)
    else:
        st.write("No data available for the specified time range.")
