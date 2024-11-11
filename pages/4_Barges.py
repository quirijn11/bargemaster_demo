import streamlit as st
from dotenv import load_dotenv
import data.service_database

load_dotenv()

st.header('Barges')
st.markdown("""
       This page provides an overview of the barges in this demo environment.
       """)

df_barges = data.service_database.load_datatable_from_db('barges')
df_barges.drop(columns=['country_name', 'gross_tonnage', 'deadweight', 'year_built', 'barge_id', 'operator_id'], inplace=True)
df_barges.rename(columns={'breadth': 'width'}, inplace=True)
st.dataframe(df_barges)