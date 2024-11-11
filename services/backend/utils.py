import datetime as dt
import numpy as np
import pandas as pd
from data.service_database import store_dataframe_to_db, load_datatable_from_db
import random


############################################################################
################### 1. PMA JSON        #####################################
############################################################################

def pma_random_linestops(barges, time_of_planning, calls, cargos):
    """
        Generate random line stops for barges when their locations are unknown.

        :param barges: DataFrame containing barge information.
        :param time_of_planning: String representing the time of planning.
        :param calls: DataFrame containing call information.
        :param cargos: DataFrame containing cargo information.
        :return: List of dictionaries representing line stops.
        """
    # Get a unique list of load an
    terminals = list(cargos["loadTerminal"].unique()) + list(cargos["dischargeTerminal"].unique())
    terminals = list(set(terminals))  # Remove duplicates

    # cargos["loadTimeWindowStart"] = pd.to_datetime(cargos["loadTimeWindowStart"]) previous took the loadTimeWindowStart
    # check for time of planning which is strftime ('%Y-%m-%dT%H:%M:%SZ') Change that to datetime
    planning_date = pd.to_datetime(time_of_planning)
    earliest_load_time = planning_date.tz_localize('UTC')
    # create random ofset between 6 and 12 hours to determine starttime of first visit
    random_offset = dt.timedelta(hours=random.randint(6, 12))

    # If barges are known to have locations, we should add those locations to the linestops
    barge_w_calls = [barge_id for barge_id in calls['barge_id'].to_list() if barge_id in barges['barge_id'].to_list()]
    linestops = []
    calls['end_date_time'] = pd.to_datetime(calls['end_date_time'])

    valid_calls = calls[(calls['end_date_time'] > earliest_load_time - dt.timedelta(hours=48)) &
                        (calls['barge_id'].isin(barges['barge_id'].to_list()))]

    barges_w_valid_calls = valid_calls['barge_id'].to_list()

    # We should also filter out barges that haven't been assigned a call yet
    barge_wo_calls = [barge_id for barge_id in barges['barge_id'].to_list() if barge_id not in barges_w_valid_calls]
    linestops = valid_calls.to_dict(orient='records')

    for barge_wo_call in barge_wo_calls:
        # Assign a start and end time of the first call as location for the barge
        start_date_time = earliest_load_time - random_offset
        end_date_time = start_date_time + dt.timedelta(hours=random.randint(1, 4))

        # TODO: The first stop is a terminal, but could be a river location. Should retrieve position add it as temp_t
        linestop = {"terminal_id": np.random.choice(terminals),
                    "linestop_id": np.random.randint(1000000, 9999999),
                    "barge_id": barge_wo_call,
                    "fixed_stop": True,
                    "on_board_after_stop": 0,
                    "time_status": "PLANNED",
                    "start_date_time": start_date_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "end_date_time": end_date_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "load_20": 0,
                    "load_40": 0,
                    "load_45": 0,
                    "discharge_20": 0,
                    "discharge_40": 0,
                    "discharge_45": 0,
                    "load_volume": 0,
                    "load_product_type": None,
                    "discharge_volume": None,
                    "discharge_product_type": None}
        linestops.append(linestop)

    return linestops


def pma_fill_json_orders(dictionary_list):
    """
    Fill JSON orders for PMA.

    :param dictionary_list: List of dictionaries containing order information.
    :return: List of dictionaries representing orders.
    """

    orders = []
    for dictionary_record in dictionary_list:
        orders.append(
            {"orderId": dictionary_record["orderId"],
             "containerNumber": dictionary_record["containerNumber"],
             "containerType": dictionary_record["containerType"],
             "bookingIdentifier": dictionary_record["bookingReference"],
             "TEU": int(dictionary_record["teu"]),
             "weight": round(dictionary_record["weight"], 2),
             "reefer": dictionary_record["reefer"],
             "dangerGoods": dictionary_record["dangerousGoods"],
             "loadTerminal": dictionary_record["loadTerminal"],
             "loadExternalId": dictionary_record["loadExternalId"],
             "dischargeTerminal": dictionary_record["dischargeTerminal"],
             "dischargeExternalId": dictionary_record["dischargeExternalId"],
             "loadTimeWindow": {"startDateTime": dictionary_record["loadTimeWindowStart"],
                                "endDateTime": dictionary_record["loadTimeWindowEnd"]},
             "dischargeTimeWindow": {"startDateTime": dictionary_record["dischargeTimeWindowStart"],
                                     "endDateTime": dictionary_record["dischargeTimeWindowEnd"]},
             "loadStopId": None,
             "unloadStopId": None,
             "ownRevenue": 5000,
             "otherRevenue": 5000})

    return orders


def pma_fill_json_terminals(dictionary_list_terminals, dictionary_list_opening_times, forbidden_routes,
                            terminal_operating_times):
    """
    Fill JSON terminals for PMA.

    :param dictionary_list_terminals: List of dictionaries containing terminal information.
    :param dictionary_list_opening_times: List of dictionaries containing terminal opening times.
    :param forbidden_routes: List of forbidden routes.
    :param terminal_operating_times: List of terminal operating times.
    :return: List of dictionaries representing terminals.
    """

    terminals = []

    for dictionary_record in dictionary_list_terminals:
        terminal_name = dictionary_record["terminal_description"]
        forbidden_routes_for_this_terminal = []
        if terminal_name in forbidden_routes:
            forbidden_routes_for_this_terminal = forbidden_routes[terminal_name]

        sea_terminal = False
        port_charge = 0

        waiting_time_in_seconds = terminal_operating_times[terminal_name][1]
        handling_time_in_seconds = terminal_operating_times[terminal_name][0]

        port_authority = dictionary_record["place"]

        terminals.append(
            {
                "terminalId": terminal_name,
                "seaTerminal": sea_terminal,
                "externalId": dictionary_record["id"],
                "minimumCallSize": dictionary_record["minimum_call_size"],
                "callCost": port_charge,
                "handlingCostPerTEU": 0,
                "handlingTime": handling_time_in_seconds,
                "openingTimes": pma_fill_json_terminals_opening_times(dictionary_record['operating_times_index'], dictionary_list_opening_times),
                "flexMoves": dictionary_record["flex_moves"],
                "callSizeFine": dictionary_record["call_size_fine"],
                "baseStopTime": waiting_time_in_seconds,
                "position": {
                    "latitude": dictionary_record["latitude"],
                    "longitude": dictionary_record["longitude"]
                },
                "forbiddenRoutes": forbidden_routes_for_this_terminal,
                "portAuthority": port_authority
            }
        )

    return terminals


def pma_fill_json_terminals_opening_times(operating_id, dictionary_list):
    """
    Fill JSON terminal opening times for PMA.

    :param dictionary_list: List of dictionaries containing terminal opening times.
    :return: List of dictionaries representing terminal opening times.
    """

    opening_times = []

    for dictionary_record in dictionary_list:
        if dictionary_record['index'] in list(range(operating_id, operating_id + 7)):
            opening_times.append(
                {
                    "weekDay": dictionary_record["week_day"],
                    "startTime": dictionary_record["start_time"],
                    "flexStartTime": dictionary_record["flex_start_time"],
                    "endTime": dictionary_record["end_time"],
                    "flexEndTime": dictionary_record["flex_end_time"]
                }
            )

    return opening_times


def pma_fill_json_vessel_active_times(operator_id, dictionary_list_active_times):
    """
    Fill JSON vessel active times for PMA.

    :param dictionary_list_active_times: List of dictionaries containing vessel active times.
    :return: List of dictionaries representing vessel active times.
    """

    active_times = []

    for dictionary_record in dictionary_list_active_times:
        if dictionary_record['index'] in list(range(operator_id, operator_id+7)):
            active_times.append(
                {
                    "weekDay": dictionary_record["week_day"],
                    "startTime": dictionary_record["start_time"],
                    "endTime": dictionary_record["end_time"]
                }
            )

    return active_times


def pma_fill_json_vessel_line_stops(dictionary_list_line_stops):
    """
    Fill JSON vessel line stops for PMA.

    :param dictionary_list_line_stops: List of dictionaries containing vessel line stops.
    :return: List of dictionaries representing vessel line stops.
    """

    line_stops = []

    for dictionary_record in dictionary_list_line_stops:
        line_stops.append(
            {
                "terminalId": dictionary_record["terminal_id"],
                "lineStopId": dictionary_record["linestop_id"],
                "loadOrders": [],
                "dischargeOrders": [],
                "timeWindow": {"startDateTime": dictionary_record["start_date_time"],
                               "endDateTime": dictionary_record["end_date_time"]},
                "fixedStop": True
            }
        )

    return line_stops


def pma_fill_json_vessels(dictionary_list_barges, active_times_dict, line_stops_dict, forbidden_terminals, barge_speeds,
                          barge_minimum_call_sizes, home_terminals):

    """
    Fill JSON vessels for PMA.

    :param dictionary_list_barges: List of dictionaries containing barge information.
    :param active_times_dict: Dictionary containing active times.
    :param line_stops_dict: Dictionary containing line stops.
    :param forbidden_terminals: List of forbidden terminals.
    :param barge_speeds: Dictionary containing barge speeds.
    :param barge_minimum_call_sizes: Dictionary containing barge_minimum_call_sizes.
    :param home_terminals: Dictionary containing home terminals.
    :return: List of dictionaries representing vessels.
    """

    vessels = []
    for dictionary_record in dictionary_list_barges:

        # Get vessel line stops
        vessel_line_stops = [x for x in line_stops_dict if
                             x["barge_id"] == dictionary_record["barge_id"]]

        day_cost = 250
        transit_fuel_cost = dictionary_record['kilometer_cost']
        call_fuel_cost = 0
        port_authority_cost = 0

        operating_costs = {"MONDAY": day_cost, "TUESDAY": day_cost, "WEDNESDAY": day_cost, "THURSDAY": day_cost,
                           "FRIDAY": day_cost, "SATURDAY": day_cost, "SUNDAY": day_cost}

        vessel_name = dictionary_record["call_sign"]
        forbidden_terminals_for_this_vessel = []
        if dictionary_record["call_sign"] in forbidden_terminals:
            forbidden_terminals_for_this_vessel = forbidden_terminals[vessel_name]
        home_terminals_for_this_vessel = []
        if dictionary_record["call_sign"] in home_terminals:
            home_terminals_for_this_vessel = []
            for value in home_terminals[vessel_name]:
                home_terminals_for_this_vessel.append(value)

        vessels.append(
            {
                "id": vessel_name,
                "externalId": dictionary_record["barge_id"],
                "capacityTEU": dictionary_record["teu"],
                "capacityWeight": dictionary_record["gross_tonnage"],
                "capacityReefer": dictionary_record["reefer_connections"],
                "capacityDangerGoods": dictionary_record["capacity_dangerous_goods"],
                "minimumCallSize": barge_minimum_call_sizes[vessel_name],
                "callSizeFine": 10000,
                "speed": barge_speeds[vessel_name],
                "kilometerCost": transit_fuel_cost,
                "dayCost": operating_costs,
                "portAuthorityCost": port_authority_cost,
                "terminalCallCost": call_fuel_cost,
                "activeTimes": pma_fill_json_vessel_active_times(dictionary_record['operator_id'], active_times_dict),
                "stops": pma_fill_json_vessel_line_stops(vessel_line_stops),
                "terminals": home_terminals_for_this_vessel,
                "forbiddenTerminals": forbidden_terminals_for_this_vessel,
            }
        )

    return vessels


def pma_fill_json_appointments(dictionary_list):
    """
    Fill JSON appointments for PMA.

    :param dictionary_list: List of dictionaries containing appointment information.
    :return: List of dictionaries representing appointments.
    """

    appointments = []

    if len(dictionary_list) > 0:
        for dictionary_record in dictionary_list:
            appointments.append(
                {"terminalId": dictionary_record["terminalId"],
                 "timeWindow": {"startDateTime": dictionary_record["startDateTime"],
                                "endDateTime": dictionary_record["endDateTime"]},
                 "loadOrders": dictionary_record["loadOrders"],
                 "dischargeOrders": dictionary_record["dischargeOrders"],
                 "maxMoves": dictionary_record["maxMoves"],
                 "discount": dictionary_record["discount"]})

        return appointments

    else:
        return appointments


def pma_fill_json_hubs(dictionary_list):
    """
    Fill JSON hubs for PMA.

    :param dictionary_list: List of dictionaries containing hub information.
    :return: List of dictionaries representing hubs.
    """

    hubs = []

    if len(dictionary_list) > 0:
        for dictionary_record in dictionary_list:
            hubs.append(
                {"hubId": dictionary_record["hubId"],
                 "hubName": dictionary_record["hubName"],
                 "location": {"latitude": dictionary_record["latitude"],
                              "longitude": dictionary_record["longitude"]},
                 "timeZone": dictionary_record["timeZone"],
                 "openingHours": {"startDateTime": dictionary_record["startDateTime"],
                                  "endDateTime": dictionary_record["endDateTime"]},
                 "maxMoves": dictionary_record["maxMoves"],
                 "discount": dictionary_record["discount"]})

        return hubs

    else:
        return hubs


def pma_fill_json_webhook(dictionary_record):
    """
    Fill JSON webhook for PMA.

    :param dictionary_record: Dictionary containing webhook information.
    :return: Dictionary representing the webhook.
    """

    webhook = {"url": dictionary_record["url"],
               "token": dictionary_record["token"]}

    return webhook


def pma_fill_json_mailhook(dictionary_record):
    """
    Fill JSON mailhook for PMA.

    :param dictionary_record: Dictionary containing mailhook information.
    :return: Dictionary representing the mailhook.
    """

    mailhook = {"emailAddress": dictionary_record["emailAddress"],
                "token": dictionary_record["token"]}

    return mailhook


def pma_fill_json_timestamp():
    """
    Fill JSON timestamp for PMA.

    :param dictionary_list: List of dictionaries containing timestamp information.
    :return: String representing the current timestamp in the format "YYYY-MM-DDTHH:MM:SSZ".
    """

    # return timestamp string .now() in format "2024-03-03T10:01:09Z"
    return dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


############################################################################
################### 2. Source adjustments ##################################
############################################################################


#TODO: Add logs on source adjustment
#TODO: Add print statement on source adjustment class
def source_adjustment_cma_cgm(dataframe):
    """

    :param dataframe:
    :return:
    """

    # Check if all required columns are present
    required_columns = ["20'ST", "20'RF", "40'ST", "40'HC", "45'HC", "40'RH"]

    if any(x in dataframe.columns for x in required_columns):
        # Timestamp is in format 2024-03-18 00:00:00 and should be in format 2024-03-18T00:00:00Z
        dataframe["loadTimeWindowStart"] = pd.to_datetime(dataframe['Loaddate'], format='8601')
        dataframe["loadTimeWindowEnd"] = dataframe["loadTimeWindowStart"] + dt.timedelta(days=3)
        dataframe["dischargeTimeWindowStart"] = dataframe["loadTimeWindowStart"] + dt.timedelta(days=1)
        dataframe["dischargeTimeWindowEnd"] = dataframe["dischargeTimeWindowStart"] + dt.timedelta(days=7)

        # timestamp now in format 2024-03-18
        timestamp = dt.datetime.now().strftime("%Y-%m-%d")

        ref_table = {
            "BINH DUONG": {"ExternalId": "4", "TerminalName": "VNSGNDBDT"},
            "CAT LAI GIANG NAM": {"ExternalId": "2", "TerminalName": "VNSGNDCLG"},
            "DONG NAI": {"ExternalId": "3", "TerminalName": "VNBHADDNA"},
            "GEMALINK": {"ExternalId": "1", "TerminalName": "VNVUTDGML"},
            "ICD AN SON": {"ExternalId": "16", "TerminalName": "VNBDGDASN"},
            "ICD PHUC LONG": {"ExternalId": "7", "TerminalName": "VNSGNDIPL"},
            "ICD PHUOC LONG 1": {"ExternalId": "14", "TerminalName": "VNSGNDIPT"},
            "PHUOC LONG 3": {"ExternalId": "5", "TerminalName": "VNSGNDICD"},
            "SOWATCO": {"ExternalId": "8", "TerminalName": "VNSGNDSLB"},
            "SPITC": {"ExternalId": "10", "TerminalName": "VNSGNDITC"},
            "TANAMEXCO": {"ExternalId": "6", "TerminalName": "VNSGNDTAN"},
            "TRANSIMEX": {"ExternalId": "13", "TerminalName": "VNSGNDTSM"}
        }

        # Create a log if TerminalName not in ref_table
        for terminal in list(dataframe['POL']):
            if terminal not in list(ref_table.keys()):
                print(f"Terminal '{terminal}' not in ref_table")
                raise ValueError(f"Terminal '{terminal}' not known")

        cont_type = lambda x: "20GP" if x == "20'ST" \
            else "20RF" if x == "20'RF" \
            else "40GP" if x == "40'ST" \
            else "45GP" if x == "40'HC" \
            else "45RT" if x == "40'RH" \
            else "L5GP"

        cont_teu = lambda x: 1 if x == "20'ST" \
            else 2.25 if x == "45'HC" \
            else 2
        cont_weight = lambda x: 2.350 if x == "20'ST" else 3.900 if x == "40'HC" else 4.000
        cont_reefer = lambda x: True if (x == "40'RH" or x == "20'RF") else False

        records = []
        available_columns = [column_name for column_name in dataframe.columns if column_name in required_columns]
        # Iterate through each row in the dataframe
        for _, row in dataframe.iterrows():
            # Iterate through each container type
            for container_type, quantity in row[available_columns].items():
                if pd.notnull(quantity):
                    for _ in range(int(quantity)):
                        # Create a record for each container
                        record = {
                            "bookingReference": row["SIPA REF"],
                            "bookingDateCreated": timestamp,
                            "containerNumber": "EMTY" + str(random.randint(1000000, 9999999)),
                            "containerType": cont_type(container_type),
                            "teu": cont_teu(container_type),  # Adjust TEU based on container type
                            "weight": cont_weight(container_type),
                            "reefer": cont_reefer(container_type),
                            "dangerousGoods": False,
                            "loadTerminal": ref_table[row["POL"]]["TerminalName"],
                            "loadExternalId": ref_table[row["POL"]]["ExternalId"],
                            "loadTimeWindowStart": row["loadTimeWindowStart"],
                            "loadTimeWindowEnd": row["loadTimeWindowEnd"],
                            "dischargeTerminal": ref_table[row["POD"]]["TerminalName"],
                            "dischargeExternalId": ref_table[row["POD"]]["ExternalId"],
                            "dischargeTimeWindowStart": row["dischargeTimeWindowStart"],
                            "dischargeTimeWindowEnd": row["dischargeTimeWindowEnd"]
                        }

                        # Append the record to the list
                        records.append(record)

        # records to a dataframe
        adj_dataframe = pd.DataFrame(records)

        return adj_dataframe


    else:

        dataframe['reefer'] = dataframe['reefer'].apply(lambda x: True if x == 'REEFER' else False)

        # Transform the dangerousGood column to boolean
        dataframe['dangerousGoods'] = dataframe['dangerousGoods']. \
            apply(lambda x: True if x == 'Y' else False)

        # create a uuid for self.container_input['bookingReference'], unique for each booking
        def generate_random_value():
            return str(np.random.randint(100, 1000))

        dataframe.sort_values(by='bookingReference', inplace=True)

        # Initialize the anchor variable
        anchor = None

        # Iterate over the DataFrame
        for label, row in dataframe.iterrows():
            # Check if anchor is None or if 'BKG.JOB_REFERENCE' is different from anchor
            if anchor is None or anchor != row['bookingReference']:
                counter = 1  # Reset the counter if 'BKG.JOB_REFERENCE' changes
            else:
                counter += 1  # Increment the counter if 'BKG.JOB_REFERENCE' remains the same

            # Assign the value of 'BKG.JOB_REFERENCE' appended with the counter as 'bookingReference'
            booking_ref_str = str(row['bookingReference'])
            dataframe.at[label, 'bookingReference'] = f"{row['bookingReference']}-{counter}"

            # Update the anchor for the next iteration
            anchor = row['bookingReference']

        dataframe['STATUS'] = dataframe['TKG.MOVE_STATUS_CODE'].apply(
            lambda x: "MIT" if pd.isnull(x) else x)

        def distribute_cma_cgm_disc(x):
            if x['STATUS'] == "IIT":
                return x['EXP_POD']
            elif x['STATUS'] == "MIT":
                return x['LOCATION_CODE_NEXT']
            elif x['STATUS'] == "TAF":
                return x['TKG.NEXT_POOL']
            elif x['STATUS'] == "IDF":
                return x['dischargeTerminal']

        def distribute_cma_cgm_load(x):
            if x['STATUS'] == "IIT":
                return x['TKG.POOL'][:9]
            elif x['STATUS'] == "MIT":
                return x['LOCATION_CODE']
            elif x['STATUS'] == "TAF" or x['STATUS'] == "IDF":
                return x['TKG.POOL'][:9]

        dataframe['dischargeTerminal'] = dataframe.apply(distribute_cma_cgm_disc, axis=1)
        dataframe['loadTerminal'] = dataframe.apply(distribute_cma_cgm_load, axis=1)

        # where self.container_input['dischargeTerminal'] == VNDI2DBDT change to VNSGDBDT
        terminal_code_ref_dict = {'VNDI2DBDT': 'VNSGNDBDT',
                                  'VNSGNDHPP': 'VNSGNHPPT',
                                  'VNSGNDIPT': 'VNVUTDGML',
                                  'VNVUTDTTT': 'VNVUTTCTT'}

        for terminal_code_ref, terminal_code in terminal_code_ref_dict.items():
            dataframe['loadTerminal'] = dataframe['loadTerminal'].replace(terminal_code_ref, terminal_code)
            dataframe['dischargeTerminal'] = dataframe['dischargeTerminal'].replace(terminal_code_ref, terminal_code)

        terminals = load_datatable_from_db(table='terminals')
        # Get the terminal codes, by combining the unlocode and terminal code
        terminals['code'] = terminals['unlocode'] + terminals['terminal_code']

        # It can be that the dataframe['dischargeTerminal'] has a terminal name, that should be code
        for terminal in list(dataframe['dischargeTerminal']):
            if terminal not in list(terminals['code']):
                # Get the terminal code based on the terminal name
                terminal_code = terminals[terminals['terminal_description'] == terminal]['code'].values[0]
                # Replace the terminal name with the terminal code
                dataframe['dischargeTerminal'] = dataframe['dischargeTerminal'].replace(terminal, terminal_code)

        # To get the terminal id, create a dictionary with terminal_description as key and id as value
        terminal_dict = terminals.set_index('code')['id'].to_dict()
        # TKG.POOL_NAME equals the terminal_descriptionm so get the terminal id based on the terminal_description
        loadTerminalId = [terminal_dict[ref_name] for ref_name in list(dataframe['loadTerminal'])]

        # Get the terminal id based on the new code and create a dictionary with code as key and id as value
        id_dict = terminals.set_index('code')['id'].to_dict()
        # Get the discharge terminal id based on the terminal code
        dischargeTerminalId = [id_dict[ref_code] for ref_code in list(dataframe['dischargeTerminal'])]

        # Create a new column loadTerminalId based load terminal name
        dataframe['loadExternalId'] = loadTerminalId
        # Create a new colomn discharge terminal based on discharge terminal id
        dataframe['dischargeExternalId'] = dischargeTerminalId

        # Fill container EQP.CONTAINER_NUMBER(Ctn Number) with EQP.CONTAINER_NUMBER
        dataframe['containerNumber'] = dataframe['EQP.CONTAINER_NUMBER(Ctn Number)']
        if 'EQP.CONTAINER_NUMBER' in dataframe.columns:
            dataframe['containerNumber'] = dataframe['containerNumber'].fillna(dataframe['EQP.CONTAINER_NUMBER'])

        # Fill container Weight by Ton with Weight by ton
        dataframe['weight'] = dataframe['Weight by Ton'].fillna(dataframe['Weight by ton'])

    return dataframe

def get_demo_terminals(terminals_df):
    terminals_df = terminals_df[(terminals_df['town'] == 'Rotterdam') | (terminals_df['town'] == 'Antwerpen')]

    # Step 2: Group by 'code' and apply the function
    result = terminals_df.groupby('code', group_keys=False).apply(keep_longest_description)
    result = result.reset_index(drop=True)
    result.drop(
        columns=['id', 'externalId', 'displayCode', 'abbreviation', 'eanNumber', 'imported', 'overridden', 'movability',
                 'type', 'encodedPolyline', 'street', 'name1', 'name2', 'zipCode'],
        inplace=True)

    town_index = result.columns.get_loc("town")

    columns_to_keep = result.columns[:town_index + 1].tolist() + ['minCallSize']
    result_trimmed = result[columns_to_keep]
    result_trimmed['minCallSize'] = result_trimmed['minCallSize'].fillna(0)
    result_trimmed.rename(columns={'minCallSize': 'minimum call size'}, inplace=True)
    terminals_df_filtered = result_trimmed[(result_trimmed['latitude'] != 0) & (result_trimmed['longitude'] != 0)]

    # Create the new column
    terminals_df_filtered['alternative description'] = terminals_df_filtered.apply(create_new_column, axis=1)

    # default_values = {
    #     'call_cost': 50,
    #     'handling_time': 180,
    #     'flex_moves': 0,
    #     'call_size_fine': 100,
    #     'base_stop_time': 900,
    #     'operating_times_index': 1,
    # }
    #
    # df_transformed = pd.DataFrame({
    #     'id': range(1, len(terminals_df_filtered) + 1),
    #     'unlocode': terminals_df_filtered['code'].str[:5],
    #     'terminal_code': terminals_df_filtered['code'].str[5:],
    #     'terminal_description': terminals_df_filtered['alternative description'],  # Use alternative description
    #     'place': terminals_df_filtered['town'],
    #     'port_id': '',  # Keep empty as in the example
    #     'minimum_call_size': terminals_df_filtered['minimum call size'],
    #     **default_values,
    #     'latitude': terminals_df_filtered['latitude'],
    #     'longitude': terminals_df_filtered['longitude']
    # })
    #
    # store_dataframe_to_db(df_transformed, 'terminals')

    sea_terminals = ['ECTDDE', 'K1700', 'K869', 'K913', 'K1718', 'K1742', 'Rhenus Deepsea Terminal - Maasvlakte',
                     'APM Terminals', 'APM1 /HUTCHISON PORTS DELTA 2', 'APM Terminals - Maasvlakte 2',
                     'RWG - Rotterdam World Gateway', 'RCT Hartelhaven', 'Euromax']
    inland_terminals = ['UCT', 'Waalhaven Terminal', 'Kramer Depot Maasvlakte', 'K730', 'K1610', 'K1207',
                        'K420']
    selected_terminals = sea_terminals[:3] + inland_terminals

    return terminals_df_filtered, selected_terminals, sea_terminals

def keep_longest_description(group):
    return group.loc[group['description'].str.len().idxmax()]

# Function to determine the value for the new column
def create_new_column(row):
    if row['code'][-3:].isdigit():  # Check if the last 3 characters are digits
        # Extract only digit characters from "code" and remove leading zeros
        digits = ''.join(filter(str.isdigit, row['code']))
        if digits:  # Check if there are any digits extracted
            digits = str(int(digits))  # Convert to int to remove leading zeros
            return "K" + digits  # Concatenate "K" with the digits
    return row['description']  # Use the value from the "description" column

def forbidden_routes_cma_cgm():
    """
    CMA CGM figures that routes should be restricted based on corridors and import / export flow
    :return:
    """

    # TODO: replace hard-coded forbidden routes
    # in this hard-coded version, barges are allowed to go back and forth between ICD's within a cluster, but not
    # back from a cluster to another cluster opposite of the direction of the voyage
    forbidden_terminals_list = dict()
    forbidden_terminals_list["VNVUTDGML"] = []
    forbidden_terminals_list["VNSGNDCLG"] = ["VNSGNDIPL", "VNSGNDICD", "VNSGNDSTR", "VNSGNDTSM", "VNSGNDTAN",
                                             "VNSGNDVIC", "VNBHADDNA", "VNSGNDBDT", "VNSGNDSLB"]
    forbidden_terminals_list["VNBHADDNA"] = ["VNSGNDIPL", "VNSGNDICD", "VNSGNDSTR", "VNSGNDTSM", "VNSGNDTAN",
                                             "VNSGNDVIC"]
    forbidden_terminals_list["VNSGNDBDT"] = ["VNSGNDICD", "VNSGNDIPL", "VNSGNDSTR", "VNSGNDTAN", "VNSGNDTSM",
                                             "VNSGNDVIC", "VNBHADDNA"]
    forbidden_terminals_list["VNSGNDICD"] = ["VNBHADDNA", "VNSGNDBDT", "VNSGNDSLB", "VNSGNDITC", "VNSGNDCLG",
                                             "VNSGNDCAT"]
    forbidden_terminals_list["VNSGNDTAN"] = ["VNBHADDNA", "VNSGNDBDT", "VNSGNDSLB", "VNSGNDITC", "VNSGNDCLG",
                                             "VNSGNDCAT"]
    forbidden_terminals_list["VNSGNDIPL"] = ["VNBHADDNA", "VNSGNDBDT", "VNSGNDSLB", "VNSGNDITC", "VNSGNDCLG",
                                             "VNSGNDCAT"]
    forbidden_terminals_list["VNSGNDSLB"] = ["VNSGNDIPL", "VNSGNDICD", "VNSGNDSTR", "VNSGNDTSM", "VNSGNDTAN",
                                             "VNSGNDVIC", "VNSGNDBDT"]
    forbidden_terminals_list["VNSGNDSTR"] = ["VNBHADDNA", "VNSGNDBDT", "VNSGNDSLB", "VNSGNDITC", "VNSGNDCLG",
                                             "VNSGNDCAT"]
    forbidden_terminals_list["VNSGNDITC"] = ["VNSGNDIPL", "VNSGNDICD", "VNSGNDSTR", "VNSGNDTSM", "VNSGNDTAN",
                                             "VNSGNDVIC", "VNBHADDNA", "VNSGNDBDT", "VNSGNDSLB"]
    forbidden_terminals_list["VNSGNDVIC"] = ["VNSGNDIPL", "VNSGNDICD", "VNSGNDSTR", "VNSGNDTSM", "VNSGNDTAN",
                                             "VNBHADDNA", "VNSGNDBDT", "VNSGNDSLB", "VNSGNDITC", "VNSGNDCLG",
                                             "VNSGNDCAT"]
    forbidden_terminals_list["VNSGNDCAT"] = ["VNSGNDIPL", "VNSGNDICD", "VNSGNDSTR", "VNSGNDTSM", "VNSGNDTAN",
                                             "VNSGNDVIC", "VNBHADDNA", "VNSGNDBDT", "VNSGNDSLB"]
    forbidden_terminals_list["VNSGNDTSM"] = ["VNBHADDNA", "VNSGNDBDT", "VNSGNDSLB", "VNSGNDITC", "VNSGNDCLG",
                                             "VNSGNDCAT"]

    terminals = load_datatable_from_db('terminals')
    terminals['terminal_cd'] = terminals['unlocode'] + terminals['terminal_code']
    for terminal in terminals['terminal_cd']:
        if terminal not in forbidden_terminals_list:
            forbidden_terminals_list[terminal] = []

    # corridors = load_datatable_from_db('corridors')
    # terminals = load_datatable_from_db('terminals')
    # terminals['terminal_cd'] = terminals['unlocode'] + terminals['terminal_code']
    #
    # # join the terminal codes with the corridors
    # corridors = corridors.merge(terminals[['id','terminal_cd']], left_on='loc_id', right_on='id', how='inner')
    #
    #
    # corridor_west = corridors[corridors['corridor_name'] == 'CAIMEP-HCMC-WEST'].sort_values('seq_no')
    # corridor_east = corridors[corridors['corridor_name'] == 'CAIMEP-HCMC-EAST'].sort_values('seq_no')
    #
    # non_overlap_locations_west = list(set(corridor_east['terminal_cd']).difference(set(corridor_west['terminal_cd'])))
    # non_overlap_locations_east = list(set(corridor_west['terminal_cd']).difference(set(corridor_east['terminal_cd'])))
    # forbidden_terminals_list = dict() # List of forbidden terminals
    #
    # for terminal in terminals['terminal_cd']:
    # 	if terminal in non_overlap_locations_east:
    # 		forbidden_terminals_list[terminal] = non_overlap_locations_west
    # 	elif terminal in non_overlap_locations_west:
    # 		forbidden_terminals_list[terminal] = non_overlap_locations_east
    # 	else:
    # 		forbidden_terminals_list[terminal] = []

    return forbidden_terminals_list


class EmailNotificationAttachment:
    """ An excel file to add to the email"""

    def __init__(self, filename, voyage_dataframe, financial_dataframe, container_dataframe, operator, contributor):
        self.filename = filename
        self.df_voy = voyage_dataframe
        self.df_fin = financial_dataframe
        self.df_cntr = container_dataframe
        self.operator = operator
        self.contributor = contributor

    def voyage_sheet(self):
        """ Create the voyage sheet"""
        pass

    def save(self):
        """ Save the dataframe to an Excel file"""
        pass

    def __repr__(self):
        return f"EmailNotificationAttachment({self.filename})"


if __name__ == "__main__":
    pass
