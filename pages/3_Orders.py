import streamlit as st
from dotenv import load_dotenv
import data.service_database

load_dotenv()

st.header('Orders')
st.markdown("""
       This page provides an overview of the containers in this demo environment.
       """)

df_orders = data.service_database.load_datatable_from_db('container_orders')
st.dataframe(df_orders)