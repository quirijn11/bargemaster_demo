import streamlit as st
from dotenv import load_dotenv
import data.service_database
import data.generate_dataset as gen

load_dotenv()

# Header and description
st.image("data/CofanoLogo.png")
# Header
st.header(":orange[BARGEMASTER] | Barge planning algorithm")

# Introduction
introduction_string = ("Introducing BargeMaster: an automated barge planning system, revolutionizing the way "
                       "container logistics are managed on barge fleets. Barge Master can enhance human planning, "
                       "allowing barge operators to streamline their container logistics and achieve optimal "
                       "placement with speed and accuracy. With new developments in operations research, our "
                       "planning algorithm utilizes the latest technologies to take barge transportation logistics "
                       "to the next level.")
st.write(introduction_string)

st.divider()

st.header('Process description')
# st.info("You can configure the set of locations, distribution of containers, and barge fleet in the tabs below.")
# config_tab_1, config_tab_2, config_tab_3 = st.tabs(["Locations", "Containers", "Fleet"])
#
# terminals_df = data.service_database.load_datatable_from_terminal_db()
# terminals_df = terminals_df[(terminals_df['town'] == 'Rotterdam') | (terminals_df['town'] == 'Antwerpen')]
#
#
# # Step 1: Identify duplicates based on 'code' and keep the longest description
# def keep_longest_description(group):
#     return group.loc[group['description'].str.len().idxmax()]
#
#
# # Step 2: Group by 'code' and apply the function
# result = terminals_df.groupby('code', group_keys=False).apply(keep_longest_description)
# result = result.reset_index(drop=True)
# result.drop(
#     columns=['id', 'externalId', 'displayCode', 'abbreviation', 'eanNumber', 'imported', 'overridden', 'movability',
#              'type', 'encodedPolyline', 'street', 'name1', 'name2', 'zipCode'],
#     inplace=True)
#
# town_index = result.columns.get_loc("town")
# columns_to_keep = result.columns[:town_index + 1].tolist() + ['minCallSize']
# result_trimmed = result[columns_to_keep]
# result_trimmed['minCallSize'] = result_trimmed['minCallSize'].fillna(0)
# result_trimmed.rename(columns={'minCallSize': 'minimum call size'}, inplace=True)
# terminals_df_filtered = result_trimmed[(result_trimmed['latitude'] != 0) & (result_trimmed['longitude'] != 0)]
#
#
# # terminals_df_filtered.drop(
# #     columns=['latitude', 'longitude'],
# #     inplace=True)
#
# # Function to determine the value for the new column
# def create_new_column(row):
#     if row['code'][-3:].isdigit():  # Check if the last 3 characters are digits
#         # Extract only digit characters from "code" and remove leading zeros
#         digits = ''.join(filter(str.isdigit, row['code']))
#         if digits:  # Check if there are any digits extracted
#             digits = str(int(digits))  # Convert to int to remove leading zeros
#             return "K" + digits  # Concatenate "K" with the digits
#     return row['description']  # Use the value from the "description" column
#
#
# # Create the new column
# terminals_df_filtered['alternative description'] = terminals_df_filtered.apply(create_new_column, axis=1)
#
# sea_terminals = ['ECTDDE', 'K1700', 'K869', 'K913', 'K1718', 'K1742', 'Rhenus Deepsea Terminal - Maasvlakte',
#                  'APM Terminals', 'APM1 /HUTCHISON PORTS DELTA 2', 'APM Terminals - Maasvlakte 2',
#                  'RWG - Rotterdam World Gateway', 'RCT Hartelhaven', 'Euromax']
# inland_terminals = ['UCT', 'MEDREP SMIRNOFFWEG ROTTERDAM', 'Kramer Depot Maasvlakte', 'K730', 'K1610', 'K1207', 'K420']
# selected_terminals = sea_terminals[:3] + inland_terminals
#
# with config_tab_1:
#     col1, col2 = st.columns(2)
#     with col1:
#         options = st.multiselect(
#             "Which terminals should be included?",
#             terminals_df_filtered['alternative description'].tolist(),
#             selected_terminals
#         )
#     with col2:
#         terminals_df_selected = terminals_df_filtered[terminals_df_filtered['alternative description'].isin(options)]
#         terminals_df_selected.drop(
#             columns=['description', 'latitude', 'longitude', 'minimum call size'],
#             inplace=True)
#         terminals_df_selected.rename(columns={'alternative description': 'name'}, inplace=True)
#         terminals_df_edited = st.data_editor(terminals_df_selected,
#                                              disabled=('code', 'town', 'name'),
#                                              hide_index=True)
#
# locations_df = terminals_df_filtered[terminals_df_filtered['code'].isin(terminals_df_selected['code'])]
# locations_df['call_cost'] = 50
# locations_df['handling_time'] = 180
# locations_df['flex_moves'] = 0
# locations_df['call_size_fine'] = 100
# locations_df['base_stop_time'] = 900
# locations_df['operating_times_index'] = 1
# locations_df['unlocode'] = locations_df['code'].str[:5]
# locations_df['terminal_code'] = locations_df['code'].str[-5:]
# locations_df['id'] = range(1, len(locations_df) + 1)
# locations_df['port_id'] = None
# locations_df.drop(columns=['code', 'description'], inplace=True)
# locations_df.rename(columns={'alternative description': 'terminal_description', 'town': 'place',
#                              'minimum call size': 'minimum_call_size'}, inplace=True)
# new_column_order = ['id', 'unlocode', 'terminal_code', 'terminal_description', 'place', 'port_id', 'minimum_call_size',
#                     'call_cost', 'handling_time', 'flex_moves', 'call_size_fine', 'base_stop_time',	'latitude',
#                     'longitude', 'operating_times_index']
# locations_df = locations_df[new_column_order]
# data.service_database.store_dataframe_to_db(locations_df, 'terminals')
#
#
# with config_tab_2:
#     total_teu = st.number_input(
#         "How many TEU worth of containers would you like to generate?",
#         min_value=0,
#         max_value=6000,
#         value = 2000,
#     )
#     generate = st.button("Generate new container dataset")
#     if generate:
#         terminals_sea = terminals_df_edited[terminals_df_edited['name'].isin(sea_terminals)]['name'].tolist()
#         terminals_inland = terminals_df_edited[~terminals_df_edited['name'].isin(sea_terminals)]['name'].tolist()
#         gen.generate_container_data([0.475, 0.475, 0.05], total_teu, terminals_sea, terminals_inland)
#     container_data = data.service_database.load_datatable_from_db('container_orders')
#     st.dataframe(container_data)
#
# with config_tab_3:
#     barges = data.service_database.load_datatable_from_db('barges')
#     barges_copy = barges.copy()
#     barges_copy.drop(
#         columns=['mmsi', 'eni', 'call_sign', 'country_name', 'country_iso', 'type', 'type_specific', 'gross_tonnage', 'deadweight', 'length',
#                  'breadth', 'year_built', 'barge_id', 'operator_id', 'kilometer_cost', 'reefer_connections', 'capacity_dangerous_goods'],
#         inplace=True)
#     # barges_copy['selected'] = False
#     # barges_copy.loc[:4, 'selected'] = True
#     barges_edited = st.data_editor(barges_copy,
#                                    disabled=('name', 'teu'),
#                                    hide_index=True
#                                    )

