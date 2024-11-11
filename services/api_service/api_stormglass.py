import arrow
import requests
import streamlit as st
API_KEY = st.secrets["GLASSTORM_API_KEY"]

start = arrow.now().floor('day')
end = arrow.now().shift(days=1).floor('day')

def get_tide_data():
    response = requests.get(
      'https://api.stormglass.io/v2/tide/sea-level/point',
      params={
        'lat': 10.529226,
        'lng': 107.00360,
        'start': start.to('UTC').timestamp(),  # Convert to UTC timestamp
        'end': end.to('UTC').timestamp(),  # Convert to UTC timestam
      },
      headers={
        'Authorization': API_KEY
      }
    )

    return response.json()

