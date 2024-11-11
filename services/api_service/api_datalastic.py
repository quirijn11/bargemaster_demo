import json

import requests
import streamlit as st
from data.service_database import load_datatable_from_db, input_data_to_db
API_KEY = st.secrets["API_KEY_DATALASTIC"]

def location_tracking(port, vessel_type):
    """
    Location Traffic Tracking API allows you to scan an area in the sea or ocean to see all the ships in your
     selected area. This API Endpoint
     lets you find and track all the vessels in the radius and monitor maritime traffic in your zone.
     The search will be performed in a radius with the center in the Port with this UNLOCODE.

    :param port:
    :return:
    """

    url = f"https://api.datalastic.com/api/v0/vessel_inradius?api-key={API_KEY}&port_unlocode={port}&vessel_type={vessel_type}"
    response = requests.get(url)

    return response.json()

def vn_barge_finder(name):

    url=f"https://api.datalastic.com/api/v0/vessel_find?api-key={API_KEY}&name={name}&fuzzy=0&length_max=90&country_iso=VN"

    response = requests.get(url)

    return response.json()


def report_historical_location_data(payload):
    """
    The historical location list of all vessels that pass through the selected zone during the chosen timeframe and is
    retrieved by selecting a specific location and timestamp.

    :param payload: (dict) {"api-key": "******", "report_type": "inradius_history", "lat": 39.05, "lon": 40.01,
    "radius": 10, "from": "2022-07-01", "to": "2022-07-05"}
    :return: (str) Report id
    """


    url = f"https://api.datalastic.com/api/v0/report"
    header = {"Content-Type": "application/json"}
    response = requests.post(url, headers=header, data=payload)

    return response.json()

def retrieve_report(report_id:str) -> dict:
    """
    Retrieve the report data by the report id
    :param report_id: (str) Report id
    :return: (json) report data
    """
    print(API_KEY)
    print(type(API_KEY))
    url = f"https://api.datalastic.com/api/v0/report?api-key={API_KEY}&report_id={report_id}"
    response = requests.get(url=url)
    return response.json()
