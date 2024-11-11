import datetime

import streamlit as st
from dotenv import load_dotenv
import data.service_database
import requests
import pandas as pd
import json
from services.backend.visualisation_creation import VisualizationPlanning, VisualizationContainerOrders
from services.backend.extract_planning import ExtractPmaPlanning
from services.backend.transform_orders import TransformToPMA
import services.backend.utils as utils
import data.generate_dataset as gen

load_dotenv()

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


st.header('Planning')
st.markdown("""
       This is where the :rainbow[magic] happens.
       """)

# get the min and max date (not datetime)
today = pd.to_datetime(datetime.datetime.now())


with (st.sidebar):
    planning_date = st.date_input("Enter planning date\n\n(The day the planning created at 00:00):",
                                      value=today)
    st.divider()

    planning_buffer = st.number_input("Enter buffer hours \n\n(e.g., 24 for planning tomorrow and beyond):",
                                          value=24)

    st.divider()

    penalize_distance = st.slider("Select the distance penalty",
                                  min_value=0,
                                  max_value=2,
                                  value=1)

    st.divider()

    penalize_unplanned = st.slider("Select the penalty per unplanned container",
                                   min_value=0,
                                   max_value=2,
                                   value=1)

    st.divider()

    st.write("Number of iterations")
    number_of_iterations = st.select_slider(
        "Number of results to generate from which the best one is selected",
        options=[1, 10, 100, 1000],
        value=10)

    pma_restrictions = {"penalize_distance": penalize_distance, "penalize_unplanned": penalize_unplanned,
                        "number_of_iterations": number_of_iterations}


# Barges
df_barges = data.service_database.load_datatable_from_db('barges')
df_barges_copy = df_barges.copy()
df_barges_copy.drop(columns=['country_name', 'gross_tonnage', 'deadweight', 'year_built', 'barge_id', 'operator_id'],
                    inplace=True)
df_barges_copy.rename(columns={'breadth': 'width'}, inplace=True)
df_barges_copy.drop(
    columns=['mmsi', 'eni', 'country_iso', 'type', 'type_specific', 'length', 'width'],
    inplace=True)

# Terminals
df_terminals = data.service_database.load_datatable_from_db('terminals')
df_terminals_copy = df_terminals.copy()
df_terminals_copy.drop(
    columns=['id', 'unlocode', 'terminal_code', 'port_id', 'latitude', 'longitude', 'operating_times_index'],
    inplace=True)
df_terminals_copy.drop(
    columns=['place', 'call_cost', 'flex_moves', 'call_size_fine'],
    inplace=True)
df_terminals_copy.rename(columns={'base_stop_time': 'waiting_time'}, inplace=True)

# if generate_containers:
st.info("In the tabs below, you can select **terminals and barges, and configure their features.")
config_tab_1, config_tab_2 = st.tabs(["Terminal config", "Barge config"])

terminals_df = data.service_database.load_datatable_from_terminal_db()
terminals_df_filtered, selected_terminals, sea_terminals = utils.get_demo_terminals(terminals_df)

terminals = data.service_database.load_datatable_from_db('terminals')
with config_tab_1:
    col1, col2 = st.columns(2)
    with col1:
        options = st.multiselect(
            "Which terminals should be included?",
            terminals_df_filtered['alternative description'].tolist(),
            selected_terminals
        )
    with col2:
        terminals_selected = terminals[terminals['terminal_description'].isin(options)]
        terminals_selected_copy = terminals_selected.copy()
        terminals_selected['moves_per_hour'] = 3600 / terminals_selected['handling_time']
        terminals_selected['waiting_time_hours'] = terminals_selected['base_stop_time'] / 3600

        terminals_selected.rename(columns={'terminal_description': 'name'}, inplace=True)
        terminals_selected['code'] = terminals_selected['unlocode'] + terminals_selected['terminal_code']
        terminals_selected = terminals_selected[['code', 'name', 'moves_per_hour', 'waiting_time_hours']]

        df_terminals_edited = st.data_editor(terminals_selected,
                                             disabled=('code', 'name'),
                                             hide_index=True)

    terminal_restrictions = {}
    forbidden_routes = {}
    for index, row in df_terminals_edited.iterrows():
        terminal_restrictions[row['name']] = [3600 / row['moves_per_hour'],
                                                              3600 * row['waiting_time_hours']]
        forbidden_routes[row['name']] = []

with config_tab_2:
    df_barges_copy['avg_speed'] = 12
    df_barges_copy['in_use'] = False
    df_barges_copy.loc[:4, 'in_use'] = True
    df_barges_copy.drop(
            columns=['name'],
            inplace=True)
    df_barges_edited = st.data_editor(df_barges_copy, disabled=('call_sign', 'teu'))
    barges_minimum_call_sizes = {}
    barges_speeds = {}
    home_terminals = {}
    forbidden_terminals = {}
    df_barges_edited = df_barges_edited[df_barges_edited['in_use'] == True]
    for index, row in df_barges_edited.iterrows():
        barges_minimum_call_sizes[row['call_sign']] = 0
        barges_speeds[row['call_sign']] = row['avg_speed']
        home_terminals[row['call_sign']] = []
        forbidden_terminals[row['call_sign']] = []

    df_barges = df_barges.loc[df_barges_edited.index]

total_teu = st.number_input(
        "How many TEU worth of containers would you like to generate?",
        min_value=0,
        max_value=20000,
        value=10000,
    )
generate = st.button("Generate new container dataset")
if generate:
    terminals_sea = df_terminals_edited[df_terminals_edited['name'].isin(sea_terminals)]['name'].tolist()
    terminals_inland = df_terminals_edited[~df_terminals_edited['name'].isin(sea_terminals)]['name'].tolist()
    gen.generate_container_data([0.475, 0.475, 0.05], total_teu, terminals_sea, terminals_inland)
    st.info("You can view the container dataset on the \"Orders\" page")

df_orders = data.service_database.load_datatable_from_db('container_orders')

email_address = st.text_input("Enter email_address", value="")
run_algorithm = st.button("Run algorithm")

if run_algorithm:

    # This list provides information on when planners plan (planning_date) and for which period.
    # For example, if the period is 24 hours, they plan for tomorrow and the following days.
    pln_trnsfrm = TransformToPMA(
        mailhook_emailaddress=email_address,
        webhook_url="",
        webhook_token="",
        terminals=terminals_selected_copy,
        container_orders=df_orders,
        barge_list=df_barges,
        planning_date=[planning_date, planning_buffer],
        forbidden_routes=forbidden_routes,
        forbidden_terminals=forbidden_terminals,
        home_terminals=home_terminals,
        barge_speeds=barges_speeds,
        barge_minimum_call_sizes=barges_minimum_call_sizes,
        terminal_operating_times=terminal_restrictions,
        restrictions=pma_restrictions,

    )

    pln_trnsfrm.execute_create_json()

    with open('data/payload_output.json', 'w') as f:
        f.write(pln_trnsfrm.json)
    json_payload = json.loads(pln_trnsfrm.json)
    pma_planning = push_pma_request(json_payload)

    if type(pma_planning) == int:
        st.info("The algorithm service needs to start up. Please try again after 5 minutes")
    else:
        st.success(f"**The planning is shared successfully**. \n\nThe unique code of the planning: {pma_planning}"
                   f"\n\nIf the planning isn't sent to the email address after 15 minutes please contact us.")
        planned_container_file_name = pma_planning + "_planned_containers"
        # csv_download_link(user_planning_settings.order_list,
        #                   planned_container_file_name,
        #                   "Download the containers you're planning as CSV")

st.divider()
st.header("Upload the planning")
data_retrieval_method = st.selectbox("Select the data source for the planning", ["",
                                                                                 "Upload Json file",
                                                                                 "Retrieve planning from microservice"])

if data_retrieval_method == "Upload Json file":
    json_file = st.file_uploader("Upload json file", type=["json"])
    if json_file:
        input_data = json.load(json_file)
        input_key = None

st.divider()
st.header("Analyse the planning")
plan = st.button("Analyse")
pma_planning = None
if not plan:
    st.info("Please upload the planning to analyse the data")

if plan:
    pma_planning = ExtractPmaPlanning(key=input_key, json=input_data)
    pma_planning.extract_calls()
    # pma_planning.add_voyage_numbers()
    pma_planning.extract_containers()

    calls = pd.DataFrame.from_dict(pma_planning.calls)

    # calls = drop_columns(calls)

    if calls is None:
        st.error("The columns 'voyage_number_import' and 'voyage_number_export' are not in the dataframe")
        st.stop()

    transit = pd.DataFrame.from_dict(pma_planning.transport_events)

    calls['teu_loaded'] = calls['load_20'] + 2 * calls['load_40'] + 2.25 * calls['load_45']
    calls['teu_discharged'] = calls['discharge_20'] + 2 * calls['discharge_40'] + 2.25 * calls['discharge_45']
    viz_plan = VisualizationPlanning(calls, transit, pma_planning.occupancy_timeline)
    viz_plan.add_barge_names()

    utilisation_col, planned_col = st.columns(2)
    with utilisation_col:
        st.subheader("", divider='violet')
        st.caption('Fleet capacity utilisation')
        utilisation_per_voyage = pma_planning.calculate_occupancy_per_voyage()
        st.title("100%")
        st.subheader("", divider='violet')

    with planned_col:
        st.subheader("", divider='violet')
        st.caption('Planned containers')
        percentage_planned_containers = round(pma_planning.no_planned_cargo /
                                              (pma_planning.no_planned_cargo +
                                               pma_planning.no_unplanned_cargo),
                                              2)
        st.title(f"{percentage_planned_containers * 100}%")
        st.subheader("", divider='violet')

    tab_1, tab_2 = st.tabs(["Calls", "Containers"])

    with tab_1:
        st.plotly_chart(viz_plan.calls_gantt_chart(),
                        use_container_width=True)
        calls_copy = calls.copy()
        calls_copy.drop(
            columns=['barge_id', 'time_status', 'reefer_on_board', 'dangerous_goods_on_board', 'load_orders',
                     'discharge_orders', 'fixed_stop', 'fixedAppointment', 'teu_loaded', 'teu_discharged'],
            inplace=True)
        calls_copy['stops'] = calls_copy.groupby('barge_call_sign').cumcount() + 1
        calls_copy.insert(0, 'stops', calls_copy.pop('stops'))
        st.dataframe(calls_copy)

    with tab_2:
        st.plotly_chart(viz_plan.stack_teu_occupancy(),
                        use_container_width=True)
        st.dataframe(pma_planning.containers)
