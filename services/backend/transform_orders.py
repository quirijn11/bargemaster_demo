""" Method to transform orders from a file to a dataframe """
import pandas as pd
import numpy as np
import json
import datetime as dt

from data.service_database import load_datatable_from_db, load_query_from_db, store_dataframe_to_db, \
    empty_database_table, retrieve_container_type
from services.backend.utils import *


class IncorrectInputValues():

    def __init__(self, data, data_category='container'):
        self.data = data

    def validate_booking_reference(self):
        """

        :return:
        """

        # check if the booking reference is a string
        # check if the booking doesn't contain spaces

        pass

    def validate_container_number(self):
        # check if the container number is a string
        # check if the container number is 11 characters long
        # check if the first 3 characters is a letter the forth a 'U' and the rest are numbers

        pass

    def validate_container_type(self):
        # check if the container type is a string
        # categorize the values in container_type, container_group, display_group or container_size (1984)

        pass

    def validate_container_teu(self):
        # check if the container teu is a number
        # check if the container teu is a number between 0 and 3

        pass

    def validate_container_weight(self):
        # check if the container weight is a number
        # check if the container weight is a number between 0 and 100
        # check if the container weight is within the range of the container type

        pass

    def validate_terminal(self):
        # check if the terminal is a string
        # check if the terminal is in the terminal list
        pass

    def validate_date_time_values(self):
        # check if the date time values are in the correct format
        # check if the date time values are in the correct range

        pass


class DataTypeConverter:

    def __init__(self, data, data_category='container'):
        self.data = data
        self.data_category = data_category
        self.meta_data_types = load_query_from_db(f"SELECT column_name, column_type "
                                                  f"FROM meta_data "
                                                  f"WHERE table_category = {data_category}")

    def retrieve_difference(self):
        """
        For the data check for the data types and compare with the meta data. Return a dictionary with the column names
        and the data types
        :return: dictionary with the column names and the data types {column_name: (data_type, data_type)}

        """

        column_types_map = {'int': ["int64", "int"], 'bool': ["bool", "boolean"],
                            'str': ["object", "str", "timedelta64[ns]"],
                            'float': ["float64", "float"]}

        data_types = {}

        for col in self.data.columns:
            data_type = self.data[col].dtype
            meta_data_types = column_types_map[
                self.meta_data_types[self.meta_data_types['column_name'] == col]['column_type'].values[0]]
            if str(data_type) not in meta_data_types:
                data_types[col] = (str(data_type), meta_data_types)

        return data_types

    def convert_data_types(self):

        # check if the information fields are correct
        column_types = ['int', 'str', 'float', 'bool']

        for col_type in column_types:
            check_type_list = self.meta_data_types.values
            for check_type in check_type_list:
                if check_type in self.data.columns:
                    if col_type == 'bool':
                        self.data[check_type] = self.data[check_type].astype(bool)
                    elif col_type == 'str':
                        self.data[check_type] = self.data[check_type].astype(str)
                    elif col_type == 'float':
                        self.data[check_type] = self.data[check_type].astype(float)
                    elif col_type == 'int':
                        self.data[check_type] = self.data[check_type].astype(int)
                else:
                    print(f"Column {check_type} not in container orders")


class TimeWindowFiller:
    """
    Filling timewindows, which should handle different scenarios of missing values in the timewindows.
    export_port_cut_off: Export containers with port cut off date,time == dischargeTimeWindowEnd.
    import_vessel_departure: Import containers with vessel_departure date,time == loadTimeWindowStart.
    One_time_window_filled: When one time window is filled, the other time window can be filled as well.
    no_information: When no information is available for the time windows, error message is returned.
    """

    def __init__(self, df, group_time_windows_hours=False):
        self.df = df
        self.group_time_windows_hours = group_time_windows_hours

    def fill_missing_time_windows(self):
        time_windows = [["loadTimeWindowStart", "loadTimeWindowEnd"],
                        ["dischargeTimeWindowStart", "dischargeTimeWindowEnd"]]

        for window_start, window_end in time_windows:

            if all(col in self.df.columns for col in [window_start, window_end]):

                if self.df[(self.df[window_start].notnull()) & (self.df[window_end].notnull())].shape[0] > 0:
                    self.df[window_end] = self.df.apply(
                        lambda x: x[window_start] + pd.Timedelta(hours=168) if pd.notnull(
                            x[window_start]) and pd.notnull(x[window_end]) and x[window_end] < x[window_start]
                        else x[window_end], axis=1)

                start_col = self.df[window_start]
                end_col = self.df[window_end]

                # Check if both start and end columns have all missing values
                if start_col.isnull().all() and end_col.isnull().all():
                    return "All values are missing for {} and {}".format(window_start, window_end)

                # Check if one of the columns has missing values
                if start_col.isnull().any() or end_col.isnull().any():
                    diff = self.calculate_mean_difference(start_col, end_col)

                    if start_col.isnull().any():
                        start_col = self.fill_missing_column(start_col, end_col, -diff)
                        self.df[window_start] = start_col

                    if end_col.isnull().any():
                        end_col = self.fill_missing_column(end_col, start_col, diff)
                        self.df[window_end] = end_col

        return self.df

    def calculate_mean_difference(self, start_col, end_col):

        valid_diffs = (end_col - start_col).dropna()

        if valid_diffs.empty:
            return pd.Timedelta(hours=168)  # Default difference
        else:
            valid_diffs_mean = valid_diffs.mean()
            if valid_diffs_mean < pd.Timedelta(hours=0):
                return pd.Timedelta(hours=168)
            else:
                return valid_diffs.mean()

    def fill_missing_column(self, missing_col, other_col, diff):

        return missing_col.fillna(other_col + diff)

    def group_time_windows(self):
        """
        This function will group the time windows in clusters of x hours. Meaning we will group the timestamps, per
        group we will get the max timestamp, and convert all others to the max timestamp. We will check the difference
        of the start and end window. If that is filled and smaller than 12 we will adjust to 12 hours.
        :param df: Dataframe with the time windows
        :param group_time_windows_hours: Amount of hours to group the time windows

        :return: Adjusted dataframe where the timestamps are grouped
        """

        # Create groups of loadTerminal and dischargeTerminal
        df = self.df.copy()
        df['loadTimeWindowStart'] = pd.to_datetime(df['loadTimeWindowStart'])

        freq_h = str(self.group_time_windows_hours) + 'H'
        df['group'] = df.groupby(
            ['loadTerminal', 'dischargeTerminal', pd.Grouper(key='loadTimeWindowStart', freq=freq_h)]).ngroup()

        # For each group get the max timestamp and convert all other timestamps to the max timestamp
        df['loadTimeWindowStart'] = df.groupby('group')['loadTimeWindowStart'].transform('max')

        # For each group get the max timestamp and convert all other timestamps to the max timestamp
        df['dischargeTimeWindowStart'] = df.groupby('group')['dischargeTimeWindowStart'].transform('max')

        df = df.drop(columns=['group'])
        df['loadTimeWindowStart'] = df['loadTimeWindowStart'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df['dischargeTimeWindowStart'] = df['dischargeTimeWindowStart'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        return df


class TransformContainers:

    def __init__(self, container_input, source, group_time_windows_hours=None):

        self.container_input = container_input
        self.meta_data = load_datatable_from_db(table='meta_data')
        self.source = source
        self.column_meta_data = self.retrieve_column_meta_data()
        self.group_time_windows_hours = group_time_windows_hours

    def retrieve_column_meta_data(self):
        """
        Retrieve meta data for the columns in the dataframe
        :return: dictionary containing the column names and their data types
        """

        query_column_references = f"SELECT * FROM column_references WHERE col_ref_source = '{self.source}'"
        column_references = load_query_from_db(query_column_references)

        # Merge  and create a dictionary with {column_references.col_ref_name : meta_data.column_name}
        merge_cols = pd.merge(self.meta_data, column_references, left_on="column_id", right_on="meta_data_id")
        merge_cols = merge_cols[["col_ref_name", "column_name"]]
        merge_cols = merge_cols.drop_duplicates()
        merge_cols = merge_cols.set_index("col_ref_name")
        column_meta_data = merge_cols.to_dict()

        return column_meta_data["column_name"]

    def rename_columns(self):
        """
        Rename the columns in the dataframe
        :return: None
        """
        self.container_input.rename(columns=self.column_meta_data, inplace=True)

    def analyse_cleaning_strategies(self):
        """
        Check which cleaning strategies are needed.
        1. Missing columns
        2. Missing values
        3. Incorrect data types
        4. Incorrect values

        :return: dictionary containing the cleaning strategies {"missing_columns": [],
        "missing_values": {"column_name": count},
        "incorrect_values": []}
        """

        # Check self.container_input is missing columns that are in the meta data. If so, add to missing columns
        missing_columns = [col for col in self.column_meta_data.values() if col not in self.container_input.columns]

        # Check per column if there are missing values and add to missing values
        missing_values = {col: self.container_input[col].isnull().count() for col in self.container_input.columns}

        # Check per column if there are incorrect data types
        incorrect_data_types = DataTypeConverter(self.container_input).retrieve_difference()

        # Check per column if there are incorrect values
        incorrect_values = IncorrectInputValues(self.container_input)

        cleaning_strategies = {"missing_columns": missing_columns,
                               "missing_values": missing_values,
                               "incorrect_data_types": incorrect_data_types}

        return cleaning_strategies

    def fill_missing_time_values(self):
        """
        The data has four important fields considering container time windows. Those are:
        loadTimeWindowStart, loadTimeWindowEnd and dischargeTimeWindowStart, dischargeTimeWindowEnd

        This functions could have no values or missing values, we need to fill those. If there are windows,
        get the average difference and fill the rest with those.
        If there aren't windows, but only on field, take a difference of 72 hours

        :return: dataframe with filled missing values
        """

        column_time_names = ['loadTimeWindowStart', 'loadTimeWindowEnd',
                             'dischargeTimeWindowStart', 'dischargeTimeWindowEnd']

        if all(col in self.container_input.columns for col in column_time_names):
            for column_time in column_time_names:
                self.container_input[column_time] = pd.to_datetime(self.container_input[column_time], errors='coerce')

            filler = TimeWindowFiller(self.container_input, self.group_time_windows_hours)
            result = filler.fill_missing_time_windows()

            self.container_input = result

    def fill_missing_booking_create_date(self):
        """
        BookingCreatedDate can be present. If so,
        :return:
        """

        # Check if the bookingCreateDate column is present
        if 'bookingDateCreated' not in self.container_input.columns:
            self.container_input['bookingDateCreated'] = pd.NaT

        if 'loadTimeWindowStart' in self.container_input.columns:
            self.container_input['bookingDateCreated'].fillna(self.container_input['loadTimeWindowStart'] -
                                                              pd.Timedelta(days=3), inplace=True)

        self.container_input['bookingDateCreated'] = pd.to_datetime(self.container_input['bookingDateCreated']).dt.date

    def source_adjustments(self):
        """
        Specific adjustments based on the source of the container
        :return: None
        """

        if self.source == 'CMA_CGM':
            self.container_input = source_adjustment_cma_cgm(self.container_input)

    def filter_container_columns(self):
        """
        Filter the container columns based on the meta data
        :return: None
        """
        # Check if required data
        required_columns = [col for col in
                            self.meta_data[(self.meta_data['pma_required'] == 1) &
                                           (self.meta_data['column_type'] == 'container')]['column_name']
                            if col != 'orderId']

        if all(column in self.container_input.columns for column in required_columns) is True:
            # retrieve the contrainer_orders table

            empty_database_table('container_orders')
            container_orders = load_datatable_from_db(table='container_orders')
            container_orders_columns = container_orders.columns.drop(['orderId'])

            nan_value_col = [column for column in container_orders_columns
                             if column not in self.container_input.columns]

            if len(nan_value_col) > 0:
                for column in nan_value_col:
                    self.container_input[column] = np.nan

            # makes sure the time columns are in the correct format 2024-03-10T22:00:00Z
            self.container_input['loadTimeWindowStart'] = self.container_input['loadTimeWindowStart'].dt.strftime(
                '%Y-%m-%dT%H:%M:%SZ')
            self.container_input['loadTimeWindowEnd'] = self.container_input['loadTimeWindowEnd'].dt.strftime(
                '%Y-%m-%dT%H:%M:%SZ')
            self.container_input['dischargeTimeWindowStart'] = self.container_input[
                'dischargeTimeWindowStart'].dt.strftime(
                '%Y-%m-%dT%H:%M:%SZ')
            self.container_input['dischargeTimeWindowEnd'] = self.container_input['dischargeTimeWindowEnd'].dt.strftime(
                '%Y-%m-%dT%H:%M:%SZ')

            # filter the container_input columns that are in the container_orders columns
            self.container_input = self.container_input[container_orders_columns]

        else:
            missing_columns = list(set(required_columns) - set(self.container_input.columns))

    def store_transformed_container(self):
        """
        Store the transformed container to the database
        :return: None
        """

        store_dataframe_to_db(self.container_input, 'container_orders')

    def transform_container_orders(self):
        """
        Transform the container orders to PMA format
        :return: self.container_input
        """

        self.retrieve_column_meta_data()

        self.rename_columns()

        self.fill_missing_time_values()

        self.fill_missing_booking_create_date()

        self.source_adjustments()

        self.filter_container_columns()

        if self.group_time_windows_hours:
            group_windows = TimeWindowFiller(self.container_input, self.group_time_windows_hours)
            self.container_input = group_windows.group_time_windows()

        return self.container_input


class TransformToPMA:
    """ Class to transform the container orders, locations and barges to PMA format """

    def __init__(self, webhook_url="", webhook_token="", mailhook_emailaddress="", mailhook_token="", terminals=None,
                 container_orders=None, barge_list: list = None, planning_date=None, forbidden_routes=None,
                 forbidden_terminals=None, home_terminals=None, barge_speeds=None, barge_minimum_call_sizes=None,
                 terminal_operating_times=None, restrictions=None):

        if barge_list is None:
            barge_list = []
        self.webhook_url = webhook_url
        self.webhook_token = webhook_token
        self.mailhook_emailaddress = mailhook_emailaddress
        self.mailhook_token = mailhook_token

        self.meta_data = load_datatable_from_db(table='meta_data')
        self.restrictions = restrictions

        self.container_orders = container_orders
        # create column orderId iteration 1, length of container_orders
        self.container_orders['orderId'] = range(1, len(self.container_orders) + 1)
        self.planning_date = planning_date[0]
        self.planning_after = planning_date[1]
        self.forbidden_routes = forbidden_routes
        self.forbidden_terminals = forbidden_terminals
        self.home_terminals = home_terminals
        self.barge_speeds = barge_speeds
        self.barge_minimum_call_sizes = barge_minimum_call_sizes
        self.terminal_operating_times = terminal_operating_times

        if terminals is None:
            self.terminals = load_datatable_from_db(table='terminals')
        else:
            self.terminals = terminals
        self.barges = barge_list
        self.calls = load_datatable_from_db(table='calls')
        self.operating_times = load_datatable_from_db(table='operating_times')
        self.tariffs = None
        self.json = None

    def transform_container_orders_to_pma(self):

        # check if the information fields are correct
        order_columns = self.meta_data[self.meta_data['table_category'] == 'container'].copy()
        column_types = ['int', 'str', 'float', 'bool']

        for col_type in column_types:
            check_type_list = order_columns[order_columns['column_type'] == col_type]['column_name'].values
            for check_type in check_type_list:
                if check_type in self.container_orders.columns:
                    if col_type == 'bool':
                        self.container_orders[check_type] = self.container_orders[check_type].astype(bool)
                    elif col_type == 'str':
                        self.container_orders[check_type] = self.container_orders[check_type].astype(str)
                    elif col_type == 'float':
                        self.container_orders[check_type] = self.container_orders[check_type].astype(float)
                    elif col_type == 'int':
                        self.container_orders[check_type] = self.container_orders[check_type].astype(int)
                else:
                    print(f"Column {check_type} not in container orders")

        # get the container dictionary
        container_dict = self.container_orders.to_dict(orient='records')
        container_dict = pma_fill_json_orders(container_dict)
        container_json = json.dumps(container_dict, indent=4)

        return container_dict

    def transform_terminals_to_pma(self):

        # get the terminal dictionary

        terminal_dict = self.terminals.to_dict(orient='records')
        operating_times_dict = self.operating_times.to_dict(orient='records')
        terminal_dict = pma_fill_json_terminals(terminal_dict, operating_times_dict, self.forbidden_routes,
                                                self.terminal_operating_times)
        terminal_json = json.dumps(terminal_dict, indent=4)

        return terminal_dict

    def transform_vessels_to_pma(self):
        # get the terminal dictionary
        vessels_dict = self.barges.to_dict(orient='records')
        active_times_dict = self.operating_times.to_dict(orient='records')

        line_stops_dict = pma_random_linestops(self.barges, self.planning_date, self.calls, self.container_orders)
        vessels_dict = pma_fill_json_vessels(vessels_dict, active_times_dict, line_stops_dict,
                                             self.forbidden_terminals, self.barge_speeds,
                                             self.barge_minimum_call_sizes, self.home_terminals)
        vessel_json = json.dumps(vessels_dict, indent=4)

        return vessels_dict

    def transform_hubs_to_pma(self):
        return []

    def transform_appointments_to_pma(self):
        return []

    def transform_webhooks_to_pma(self):
        return {"url": self.webhook_url,
                "token": self.webhook_token}

    def transform_mailhook_to_pma(self):
        return {"emailAddress": self.mailhook_emailaddress,
                "token": self.mailhook_token}

    def transform_timestamp(self):
        # Create a datetime object
        if self.planning_date:
            formatted_string = self.planning_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            formatted_string = dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

        return formatted_string

    def execute_create_json(self):

        self.json = json.dumps(
            {"webhook": self.transform_webhooks_to_pma(),
             "mailhook": self.transform_mailhook_to_pma(),
             "timestamp": self.transform_timestamp(),
             "appointments": self.transform_appointments_to_pma(),
             "intervalHours": 24,
             "firstHoursFixed": self.planning_after,
             "penalizeUnplanned": 1.25 * self.restrictions["penalize_unplanned"],
             "minProfitPerTEU": 0,
             "numberOfIterations": self.restrictions["number_of_iterations"],
             "orders": self.transform_container_orders_to_pma(),
             "terminals": self.transform_terminals_to_pma(),
             "hubs": self.transform_hubs_to_pma(),
             "vessels": self.transform_vessels_to_pma()
             }, indent=4)


class TransformToDave:
    """This class will convert the template data to the dave format"""

    def __init__(self, pma_planning, pma_calls=None):

        self.pmaPlanning = pma_planning
        self.pmaCalls = pma_calls
        self.timestampNow = dt.datetime.now().strftime('%Y-%m-%d %H:%M')

        self.map_coi_orderid = dict()

        self.terminals = self.retrieve_terminal_ref()

        self.daveContainerDictionary = []
        self.daveVoyageDictionary = []

    def retrieve_terminal_ref(self):
        """
        Create and store a dataframe of terminal references for later use

        :return: dataframe of terminal references
        """
        terminal_query = """SELECT t.unlocode || t.terminal_code as code,  tr.*
                                FROM terminals t
                                    JOIN terminal_references tr
                                    ON t.id = tr.terminal_id"""
        terminal_dataframe = load_query_from_db(terminal_query)

        return terminal_dataframe

    def retrieve_call_containers(self, order_id):
        """

        :return:
        """

        return [str(self.map_coi_orderid[order]) for order in order_id]

    def pma_timestamp_dave_format(self, timestamp):
        """
        Convert the timestamp from PMA format to Dave format
        :param timestamp:
        :return: str timestamp '%Y-%m-%d %H:%M'
        """

        for fmt in ('%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M', '%Y-%m-%dT%H:%M:%S'):
            try:
                return dt.datetime.strptime(timestamp, fmt).strftime('%Y-%m-%d %H:%M')
            except ValueError:
                pass

        raise ValueError(f'no valid date format found for {timestamp}')

    def create_dave_container_dictionary(self):
        """
        This method will filter the dave dictionary to the required columns.

        :return:
        """

        list_all_orders = self.pmaPlanning['orders']

        id_list = list(np.random.choice(range(1, 100000), len(list_all_orders), replace=False))
        ref_numbers = ["RIVA-" + str(no) for no in
                       list(np.random.choice(range(1, 100000), len(list_all_orders), replace=False))]

        for order in list_all_orders:
            ref_no = ref_numbers.pop()
            id_no = id_list.pop()

            cntr_type_dataframe = retrieve_container_type(order['containerType'])

            if cntr_type_dataframe.empty:
                cst_filler = "20DV"
                twgh_filler = 2200
            else:
                cst_filler = cntr_type_dataframe["display_code"].values[0]
                twgh_filler = cntr_type_dataframe["weight_kg"].values[0]

            if order['weight'] > twgh_filler:
                clv_filler = "V"
            else:
                clv_filler = "L"

            container = {
                "ts": str(self.timestampNow),
                "bkr": str(ref_no),
                "cui": 33,
                "con": str(order["containerNumber"]),
                "cur": str(ref_no),
                "coi": str(id_no),
                "cst": str(cst_filler),
                "clv": str(clv_filler),
                "cse": "",
                "cwgh": int(order['weight'] * 1000),
                "twgh": int(twgh_filler),
                "etao": str(self.pma_timestamp_dave_format(order['loadTimeWindow']['startDateTime'])),
                "oti": str(self.terminals[self.terminals['code'] == order['loadTerminal']]['source_code'].values[0]),
                "otd": str(self.terminals[self.terminals['code'] == order['loadTerminal']]['source_name'].values[0]),
                "otui": int(self.terminals[self.terminals['code'] == order['loadTerminal']]['source_id'].values[0]),
                "etad": str(self.pma_timestamp_dave_format(order['dischargeTimeWindow']['startDateTime'])),
                "dti": str(
                    self.terminals[self.terminals['code'] == order['dischargeTerminal']]['source_name'].values[0]),
                "dtd": str(
                    self.terminals[self.terminals['code'] == order['dischargeTerminal']]['source_code'].values[0]),
                "dtui": int(self.terminals[self.terminals['code'] == order['dischargeTerminal']]['source_id'].values[0])
            }

            self.map_coi_orderid[order["orderId"]] = id_no
            self.daveContainerDictionary.append(container)

    def create_dave_call_dictionary(self):
        """ Uses calls from PMA to create dictionary for Dave """

        imp_voyage_numbers = list(
            self.pmaCalls['voyage_number_import'].unique()) if 'voyage_number_import' in self.pmaCalls.columns else []
        exp_voyage_numbers = list(
            self.pmaCalls['voyage_number_export'].unique()) if 'voyage_number_export' in self.pmaCalls.columns else []
        voyage_numbers = imp_voyage_numbers + exp_voyage_numbers
        # Drop NaN values
        voyage_numbers = [voy for voy in voyage_numbers if str(voy) != 'nan']

        # Check if the start_date_time is in the past, if so move to the future
        start_date_time_list = pd.to_datetime(self.pmaCalls['start_date_time'])  # Convert to datetime
        max_diff = (dt.datetime.now() - start_date_time_list.min())
        if max_diff.days > 0:
            start_date_time_list = start_date_time_list + max_diff
            # Change timestamp to string
            self.pmaCalls['start_date_time'] = start_date_time_list.dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        direction = None
        for voyage in voyage_numbers:
            if len(imp_voyage_numbers) == 0:
                voy_calls = self.pmaCalls[self.pmaCalls['voyage_number_export'] == voyage]
            elif len(exp_voyage_numbers) == 0:
                voy_calls = self.pmaCalls[self.pmaCalls['voyage_number_import'] == voyage]
            else:
                voy_calls = self.pmaCalls[(self.pmaCalls['voyage_number_import'] == voyage) |
                                          (self.pmaCalls['voyage_number_export'] == voyage)]

            barge = voy_calls['barge_call_sign'].values[0]
            barge_map = {"TN10": 61, "SW17": 59, "SP07": 58}
            query = f"""SELECT name, mmsi, eni  FROM barges WHERE call_sign = '{barge}'"""
            barge_data = load_query_from_db(query)

            # Check if prefix voyage number is IMP or EXP
            if voyage[:3] == 'IMP':
                direction = 'import'
            elif voyage[:3] == 'EXP':
                direction = 'export'
            else:
                direction = None

            barge_voy = {
                "ts": str(self.timestampNow),
                "vui": barge_map[barge],
                "voi": voyage,
                "vnm": str(barge_data["name"].values[0]),
                "vei": str(barge_data["eni"].values[0]),
                "mmsi": str(barge_data["mmsi"].values[0]),
                "sid": 1,
                "stops": []
            }

            for index, row in voy_calls.iterrows():
                terminal_data = self.terminals[self.terminals['code'] == row['terminal_id']]
                calls_record = {
                    "tui": int(terminal_data['source_id'].values[0]),
                    "ti": str(terminal_data['source_code'].values[0]),
                    "tnm": str(terminal_data['source_name'].values[0]),
                    "pta": str(self.pma_timestamp_dave_format(row['start_date_time']))
                }

                # VNUTDGML only has load_orders when direction is import
                if len(row["load_orders"]) > 0 and ((direction == 'import' and row['terminal_id'] == 'VNVUTDGML') or
                                                    (direction == 'export' and row['terminal_id'] != 'VNVUTDGML')):
                    calls_record["lol"] = list(self.retrieve_call_containers(row["load_orders"]))
                    calls_record["loc"] = len(calls_record["lol"])

                # VNUTDGML only has discharge_orders when direction is export
                if len(row["discharge_orders"]) > 0 and (
                        (direction == 'import' and row['terminal_id'] != 'VNVUTDGML') or
                        (direction == 'export' and row['terminal_id'] == 'VNVUTDGML')):
                    calls_record["unl"] = list(self.retrieve_call_containers(row["discharge_orders"]))
                    calls_record["unc"] = len(calls_record["unl"])

                barge_voy["stops"].append(calls_record)

            self.daveVoyageDictionary.append(barge_voy)

    def create_dave_voyage_dictionary(self):
        """ For out put of voyage format"""

        barge_voy_record = []

        for route in self.pmaPlanning["routes"]:
            barge = route["vessel"]
            barge_map = {"TN10": 61, "SW17": 59, "SP07": 58}
            query = f"""SELECT name, mmsi, eni  FROM barges WHERE call_sign = '{barge}'"""
            barge_data = load_query_from_db(query)

            barge_voy = {
                "ts": str(self.timestampNow),
                "vui": barge_map[barge],
                "voi": "V-" + str(random.randint(1, 99999)),  # TODO: Should be based on import/export voy
                "vnm": str(barge_data["name"].values[0]),
                "vei": str(barge_data["eni"].values[0]),
                "mmsi": str(barge_data["mmsi"].values[0]),
                "sid": 1
            }

            calls = []
            for stop in route["stops"]:
                terminal_data = self.terminals[self.terminals['code'] == stop['terminalId']]
                calls_record = {
                    "tui": int(terminal_data['source_id'].values[0]),
                    "ti": str(terminal_data['source_code'].values[0]),
                    "tnm": str(terminal_data['source_name'].values[0]),
                    "pta": str(self.pma_timestamp_dave_format(stop["startTime"]))
                }

                if len(stop["loadOrders"]) > 0:
                    calls_record["lol"] = list(self.retrieve_call_containers(stop["loadOrders"]))
                    calls_record["loc"] = len(calls_record["lol"])

                if len(stop["dischargeOrders"]) > 0:
                    calls_record["unl"] = list(self.retrieve_call_containers(stop["dischargeOrders"]))
                    calls_record["unc"] = len(calls_record["unl"])

                calls.append(calls_record)

            barge_voy["stops"] = calls
            self.daveVoyageDictionary.append(barge_voy)

        return self.daveVoyageDictionary
