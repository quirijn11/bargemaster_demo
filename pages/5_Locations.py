import streamlit as st
from dotenv import load_dotenv
import data.service_database

load_dotenv()

st.header('Locations')
st.markdown("""
       This page provides an overview of the locations in this demo environment.
       """)

df_terminals = data.service_database.load_datatable_from_db('terminals')
df_terminals.drop(columns=['id', 'unlocode', 'terminal_code', 'port_id', 'latitude', 'longitude', 'operating_times_index'], inplace=True)
st.dataframe(df_terminals)