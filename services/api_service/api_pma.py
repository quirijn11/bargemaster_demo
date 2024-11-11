import os

import requests
import json
import streamlit as st
# from data.configuration import pma_user_name, pma_password

pma_user_name = st.secrets["PMA_USER_NAME"]
pma_password = st.secrets["PMA_PASSWORD"]
basic_auth = (pma_user_name, pma_password)

def push_pma_request(payload):
    """This function will push the request to the PMA API"""

    url = "https://pma-acc.cofanoapps.com/api/planning"
    response = requests.post(url=url, json=payload, auth=basic_auth)

    if response.status_code != 200:
        return response.status_code
    else:
        respons_id = response.json()
        return respons_id

def get_pma_result(result_id, result_type="output"):
    """This function will push the request to the PMA API"""

    url = f"https://pma-acc.cofanoapps.com/api/log/{result_id}/{result_type}"
    response = requests.get(url=url, auth=basic_auth)

    if response.status_code == 500:
        return None
    else:
        return response
