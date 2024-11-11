from data.service_database import load_query_from_db
import json
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import requests
import streamlit as st


pma_user_name = st.secrets["PMA_USER_NAME"]
pma_password = st.secrets["PMA_PASSWORD"]
basic_auth = (pma_user_name, pma_password)

def get_pma_result(result_id, result_type="output"):
    """This function will push the request to the PMA API"""

    url = f"https://pma-acc.cofanoapps.com/api/log/{result_id}/{result_type}"
    response = requests.get(url=url, auth=basic_auth)

    if response.status_code == 500:
        return None
    else:
        return response

def create_occupancy_timeline(transit_events):
    """
    Transit events is a dictionary with the depart and arrival times of barges. This function will:
    1. Filter the events per barge
    2. Sort the events per barge on time
    3. Find the occupancy of the barge at each hour


    :param transit_events:
    :return:
    """

    departures = [event for event in transit_events if event['transit_type'] == 'DEPART']

    # create a pandas dataframe of the events with 'transit_date_time as index, stransit_occupancy_teu, transit_availability_teu and barge_id as values
    df = pd.DataFrame.from_dict(departures)

    # convert the transit_date_time to datetime
    df['transit_date_time'] = pd.to_datetime(df['transit_date_time'])

    # sort the dataframe on transit_date_time
    df.sort_values(['barge_id', 'transit_date_time'], inplace=True)

    # reindex the dataframe
    df.reset_index(drop=True, inplace=True)

    for label, row in df.iterrows():
        if label == 1:
            continue
        prev_time = df.iloc[label - 1]['transit_date_time']
        current_time = pd.to_datetime(row['transit_date_time'])

        # create a list of hours between the previous and current time with datetime
        hours = pd.date_range(prev_time, current_time, freq='h')
        df_append = []
        for hour in hours[1:]:
            df_append.append({'transit_date_time': hour,
                            'barge_id': row['barge_id'],
                            'transit_occupancy_teu': row['transit_occupancy_teu'],
                            'transit_availability_teu': row['transit_availability_teu']})

        # create a dataframe of the appended list
        df_append = pd.DataFrame(df_append)
        df = pd.concat([df,df_append])

    # sort the dataframe on transit_date_time
    df.rename(columns={'transit_date_time': 'date_time',
                       'transit_occupancy_teu': 'occupancy_teu',
                       'transit_availability_teu': 'availability_teu'}, inplace=True)
    df['capacity_teu'] = df['availability_teu'] + df['occupancy_teu']
    df = df[['barge_id', 'date_time', 'occupancy_teu', 'availability_teu', 'capacity_teu']]

    # reindex the dataframe
    df.sort_values(['barge_id', 'date_time'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df

def pma_call_dict(pma_call_dict, barge_id, barge_call_sign):
    """
    Create a dictionary with the calls from the PMA API.
    """

    # if pma_call_dict['startTime'] '%Y-%m-%dT%H:%M' then add :00 to the end of the string
    if len(pma_call_dict['startTime']) == 16:
        pma_call_dict['startTime'] = pma_call_dict['startTime'] + ':00'
    if len(pma_call_dict['departureTime']) == 16:
        pma_call_dict['departureTime'] = pma_call_dict['departureTime'] + ':00'

    barge_call = {
        "terminal_id": pma_call_dict['terminalId'],
        "barge_id": barge_id,
        "barge_call_sign": barge_call_sign,
        "time_status": "PLANNED",
        "start_date_time": pma_call_dict['startTime'],
        "end_date_time": pma_call_dict['departureTime'],
        "reefer_on_board": pma_call_dict['reefersOnBoardAfterStop'],
        "dangerous_goods_on_board": pma_call_dict['dangerousGoodsOnBoardAfterStop'],
        "load_orders": pma_call_dict['loadOrders'],
        "load_20": pma_call_dict['loading20'],
        "load_40": pma_call_dict['loading40'],
        "load_45": pma_call_dict['loading45'],
        "discharge_orders": pma_call_dict['dischargeOrders'],
        "discharge_20": pma_call_dict['discharging20'],
        "discharge_40": pma_call_dict['discharging40'],
        "discharge_45": pma_call_dict['discharging45'],
        "fixed_stop": pma_call_dict['fixedStop'],
        "fixedAppointment" : pma_call_dict['fixedAppointment']}

    return barge_call

def retrieve_barge_id(barge_call_sign):
    """
    Retrieve the barge id from the barge call sign.
    """

    barge_id = load_query_from_db(f"""SELECT barge_id FROM barges WHERE call_sign = '{barge_call_sign}'""")

    return barge_id.values[0][0]

class ExtractPmaPlanning():
    """
    Extract the planning from the PMA API using the key.
    """

    def __init__(self, key=None, json=None):

        self.key = key
        if self.key is None:
            self.json = json
        else:
            self.json = self.get_planning_json()

        self.calls = []
        self.transport_events = []
        self.occupancy_timeline = []
        self.containers = {}
        self.no_unplanned_cargo = len(self.json["unplannedOrders"])
        self.no_planned_cargo  = len(self.json["orders"]) - self.no_unplanned_cargo
        self.occupancy_per_voyage = self.calculate_occupancy_per_voyage()

    def get_planning_json(self):
        """
        Get the planning json from the PMA API.
        """

        returned_object = get_pma_result(self.key, result_type="output")
        json_object = returned_object.json()

        return json_object

    def extract_calls(self):
        """
        Extract the calls from the planning json.
        """

        for barge_plan in self.json["routes"]:
            barge_call_sign = barge_plan["vessel"]
            barge_id = retrieve_barge_id(barge_call_sign)
            transit_occupancy = prev_transit_occupancy = 0.0
            transit_availability =  prev_transit_availability = barge_plan["capacityTEU"]
            transit_end_date_time = None
            transit_start_date_time = None
            transit_arrive = None
            transit_depart = None
            transit_events = []

            for i, call in enumerate(barge_plan["stops"]):
                if i > 0:
                    barge_call = pma_call_dict(call, barge_id, barge_call_sign)
                    transit_end_date_time = call["startTime"]

                    transit_start_date_time = call["departureTime"]
                    transit_occupancy += (call["loading20"] * 1 + call["loading40"] * 2 + call["loading45"] * 2.25 )
                    transit_occupancy -= (call["discharging20"] * 1 + call["discharging40"] * 2 + call["discharging45"] * 2.25)
                    transit_availability = barge_plan["capacityTEU"] - transit_occupancy
                    barge_call["teu_on_board"] = transit_occupancy
                    barge_call["occupancy"] = round(transit_occupancy / barge_plan["capacityTEU"],2)
                    self.calls.append(barge_call)

                    if transit_end_date_time is not None:
                        transit_depart = {"transit_type": "DEPART",
                                          "transit_location_id": barge_call["terminal_id"],
                                          "transit_occupancy_teu": transit_occupancy,
                                          "transit_availability_teu": transit_availability,
                                          "barge_id": barge_id,
                                          "transit_date_time": transit_start_date_time
                                          }

                    if transit_start_date_time is not None:
                        transit_arrive = {"transit_type": "ARRIVE",
                                          "transit_location_id": barge_call["terminal_id"],
                                          "transit_occupancy_teu": prev_transit_occupancy,
                                          "transit_availability_teu": prev_transit_availability,
                                          "barge_id": barge_id,
                                          "transit_date_time": transit_end_date_time
                                            }

                    self.transport_events.append(transit_arrive)
                    self.transport_events.append(transit_depart)

                    prev_transit_occupancy = transit_occupancy
                    prev_transit_availability = transit_availability

                    transit_end_date_time = None
                    transit_start_date_time = None

                    transit_arrive = None
                    transit_depart = None

        self.occupancy_timeline = create_occupancy_timeline(self.transport_events)

        return self.calls

    def extract_containers(self):
        """
        Extract the containers from the planning json.
        """

        orders_df = pd.DataFrame(self.json["orders"])
        containers = {}
        for call in self.calls:
            if len(call["load_orders"]) != 0:
                for load_order_id in call["load_orders"]:
                    order = orders_df[orders_df["orderId"] == load_order_id].iloc[0]
                    if containers.get(order["containerNumber"]) is None:
                        containers[order["containerNumber"]] = {}
                    containers[order["containerNumber"]]["container_number"] = order["containerNumber"]
                    containers[order["containerNumber"]]["load_terminal_id"] = call["terminal_id"]
                    if call.get("voyage_number_export") is not None:
                        containers[order["containerNumber"]]["voyage_number_export"] = call["voyage_number_export"]
                    if call.get("voyage_number_import") is not None:
                        containers[order["containerNumber"]]["voyage_number_import"] = call["voyage_number_import"]

            if len(call["discharge_orders"]) != 0:
                for discharge_order_id in call["discharge_orders"]:
                    order = orders_df[orders_df["orderId"] == discharge_order_id].iloc[0]
                    if containers.get(order["containerNumber"]) is None:
                        containers[order["containerNumber"]] = {}
                    containers[order["containerNumber"]]["container_number"] = order["containerNumber"]
                    containers[order["containerNumber"]]["discharge_terminal_id"] = call["terminal_id"]
                    if call.get("voyage_number_export") is not None:
                        containers[order["containerNumber"]]["voyage_number_export"] = call["voyage_number_export"]
                    if call.get("voyage_number_import") is not None:
                        containers[order["containerNumber"]]["voyage_number_import"] = call["voyage_number_import"]

        self.containers = [container for container in containers.values()]

        return self.containers


    def add_voyage_numbers(self):
        """
        Add the voyage number to the calls. The call has should be sorted by start_date_time. It should be filtered on
        barge_id. After every terminal_id "VNVUTDGML" the voyage number should change.

        The voyage number is DIRECTIONYYWW_# where YY is the year and WW is the week number and # is the voyage number that week.
        (1,2,3,4) and the direction (IMP,EXP) should be added to the voyage number.

        - If the terminal_id is "VNVUTDGML" and the call has load container:
        --> The import_voyage_number should be YYWW_1_IMP
        --> Following calls that don't have discharge container and is not equal to "VNVUTDGML" should have the same
        voyage number.
        --> Following calls that have load container should have the same voyage number. with EXPYYWW_1

        - If the terminal_id is "VNVUTDGML" and the call has discharge container:
        --> The import_voyage_number should be YYWW_1_EXP

        :return:
        """

        calls = self.calls
        imp_voyage_iteration = 0
        exp_voyage_iteration = 1
        prev_week = None
        prev_b_id = None
        exp_prev_week = False

        for i, call in enumerate(calls):
            # Get week from dt object string "2022-01-01T00:00:00"
            date_time_st = datetime.strptime(call['start_date_time'], "%Y-%m-%dT%H:%M:%S")
            b_id = call["barge_id"]
            week = date_time_st.isocalendar()[1]
            year = date_time_st.year

            exp_vo_no = "EXP" + "-" + call["barge_call_sign"] + "-" + str(year) + str(week) + "-" + str(exp_voyage_iteration)
            exp_prev_week_no = "EXP" + "-" + call["barge_call_sign"] + "-" + str(year) + str(prev_week) + "-" + str(exp_voyage_iteration)

            if call['terminal_id'] == "VNVUTDGML":
                # Export number
                if prev_b_id != b_id:
                    imp_voyage_iteration = 0
                    exp_voyage_iteration = 1
                elif prev_week != week:
                    exp_prev_week = True
                    imp_voyage_iteration = 1
                    exp_voyage_iteration = 1
                else:
                    imp_voyage_iteration += 1
                    exp_voyage_iteration += 1

                imp_vo_no = "IMP" + "-" + call["barge_call_sign"] + "-" + str(year) + str(week) + "-" + str(imp_voyage_iteration)

                if call['load_20'] + call['load_40'] + call['load_45'] > 0:
                    calls[i]['voyage_number_import'] = imp_vo_no

                if call['discharge_20'] + call['discharge_40'] + call['discharge_45'] > 0:
                    if exp_prev_week == True:
                        calls[i]['voyage_number_export'] = exp_prev_week_no
                    else:
                        calls[i]['voyage_number_export'] = exp_vo_no
                    exp_vo_no = "EXP" + "-" + call["barge_call_sign"] + "-" + str(year) + str(week) + "-" + str(exp_voyage_iteration)

            else:
                if call['discharge_20'] + call['discharge_40'] + call['discharge_45'] > 0:
                        calls[i]['voyage_number_import'] = imp_vo_no

                if call['load_20'] + call['load_40'] + call['load_45'] > 0:
                    calls[i]['voyage_number_export'] = exp_vo_no

            prev_week = week
            prev_b_id = b_id
            exp_prev_week = False

    def calculate_occupancy_per_voyage(self):
        """

        :return:
        """

        calls = self.calls
        occupancy_per_voyage = defaultdict(float)
        for call in calls:
            if call.get("voyage_number_import") is not None:
                occupancy_per_voyage[call["voyage_number_import"]] += call["occupancy"]
            elif call.get("voyage_number_export") is not None:
                occupancy_per_voyage[call["voyage_number_export"]] += call["occupancy"]

        return occupancy_per_voyage
