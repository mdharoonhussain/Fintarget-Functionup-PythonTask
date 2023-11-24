import json
import pandas as pd
import websockets
from websockets import create_connection
from datetime import datetime, timedelta
from statistics import mean
import time

websocket_url = "wss://functionup.fintarget.in/ws?id=fintarget-functionup"

async def connect_to_websocket():
    uri = "wss://functionup.fintarget.in/ws?id=fintarget-functionup"

    async with websockets.connect(uri) as websocket:
        print("Connected to WebSocket")

        while True:
            ltp_message = await websocket.recv()
            print(f"Received LTP: {ltp_message}")


ws = create_connection(websocket_url)

# Dictionary to store OLHC data for each instrument
olhc_data = {"Nifty": pd.DataFrame(), "Banknifty": pd.DataFrame(), "Finnifty": pd.DataFrame()}

# Function to calculate moving average
def calculate_moving_average(data):
    return data['Close'].rolling(window=3).mean()

# Function to calculate exponential moving average (Bonus)
def calculate_exponential_moving_average(data):
    return data['Close'].ewm(span=3, adjust=False).mean()

# Process data received from WebSocket
def process_data(instrument, price):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if olhc_data[instrument].empty:
        # For the first minute, initialize OLHC data
        olhc_data[instrument] = pd.DataFrame(columns=["Date", "Time", "Open", "Low", "High", "Close"])

    # If there's already data for this instrument, update High, Low, and Close
    if not olhc_data[instrument].empty:
        last_data = olhc_data[instrument].iloc[-1]
        olhc_data[instrument].loc[olhc_data[instrument].index[-1], "Close"] = price
        olhc_data[instrument].loc[olhc_data[instrument].index[-1], "High"] = max(last_data["High"], price)
        olhc_data[instrument].loc[olhc_data[instrument].index[-1], "Low"] = min(last_data["Low"], price)

    # Append new data to OLHC data
    olhc_data[instrument] = olhc_data[instrument]._append(
        {"Date": timestamp.split()[0], "Time": timestamp.split()[1], "Open": price, "Low": price, "High": price, "Close": price},
        ignore_index=True
    )

    # Calculate moving average from the 3rd minute onwards
    if len(olhc_data[instrument]) > 2:
        olhc_data[instrument]['MovingAverage'] = calculate_moving_average(olhc_data[instrument])

        # Calculate exponential moving average (Bonus)
        olhc_data[instrument]['ExponentialMovingAverage'] = calculate_exponential_moving_average(olhc_data[instrument])

    # Save OLHC data to CSV once every minute
    if int(timestamp.split()[1].split(":")[2]) == 0:
        olhc_data[instrument].to_csv(f"{instrument}_olhc_data.csv", index=False)

# Time to run the WebSocket connection in seconds
run_time = 60 * 60  # 60 minutes
start_time = time.time()

try:
    while time.time() - start_time < run_time:
        message = ws.recv()
        data = json.loads(message)

        for instrument, price in data.items():
            process_data(instrument, price)

except KeyboardInterrupt:
    print("WebSocket connection closed.")
finally:
    ws.close()