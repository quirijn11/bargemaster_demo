""" This file will be used to explore retrieved container information"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
import seaborn as sns
import openpyxl

import plotly.express as px
from datetime import datetime
import sqlite3

from itertools import groupby
# from services.backend.transform_orders import TransformContainers
from data.service_database import load_datatable_from_db
from services.backend.visualisation_creation import VizualisationContainerOrders

def most_common_string(strings):
    # Sort the strings to group identical strings together
    sorted_strings = sorted(strings)

    # Group the strings and count the occurrences of each group
    grouped_strings = ((key, len(list(group))) for key, group in groupby(sorted_strings))

    print("Grouped strings are:")
    for key, length in grouped_strings:
        print(f"Key: {key}, Length: {length}")

    # Find the group with the maximum count
    most_common_group = max(grouped_strings, key=lambda x: x[1])

    # Return the string with the maximum count
    return most_common_group[0]


class ContainerFileAnalyser:

    def __init__(self, owner, file=None, file_extension=None, file_path=""):
        self.data_steward = None
        self.created = None
        self.owner = owner

        if file is not None:
            self.df = file
        else:
            self.df = self.load_data(file_path)

        if self.df is not None:
            self.missing_values = self.check_for_missing_values()
            self.duplicates = self.check_for_duplicates()
            self.column_references = self.check_for_column_references()


    def load_data(self, file_path):
        """
        Load data from file

        :param file_path: path to the file
        :return: dataframe or None if file format is not supported
        """
        try:
            if file_path.endswith((".xlsx", ".xls")):
                wb = openpyxl.load_workbook(file_path)
                self.data_steward = wb.properties.creator
                self.created = wb.properties.created
                return pd.read_excel(file_path)

            elif file_path.endswith(".csv"):
                return pd.read_csv(file_path, sep=";")

            elif file_path.endswith(".json"):
                return pd.read_json(file_path)

            else:
                return None

        except Exception as e:
            print(f"Error loading file: {e}")
            return None

    def check_for_missing_values(self):
        """
        Check for missing values in the dataframe

        :return: Series containing the count of missing values for each column
        """
        if self.df is not None:
            return self.df.isnull().sum()
        else:
            return None

    def check_for_duplicates(self):
        """
        Check for duplicates in the dataframe

        :return: number of duplicate rows
        """
        if self.df is not None:
            return self.df.duplicated().sum()
        else:
            return None

    def describe_container_weights(self):
        """
        Check for duplicates in the dataframe

        :return: number of duplicate rows
        """

        if not 'weight' in self.df.columns:
            return "Weight column not found in the dataframe."
        else:
            return self.df['weight'].describe()

    def analyse_window_times(self):
        # create a list of differences between the start and end time windows and and the loadTimeWindowEnd and dischargeTimeWindowStart

        if not all(col in self.df.columns for col in
                   ['loadTimeWindowStart', 'loadTimeWindowEnd', 'dischargeTimeWindowStart', 'dischargeTimeWindowEnd']):
            return "Time window columns not found in the dataframe."

        time_windows = self.df[['loadTimeWindowStart', 'loadTimeWindowEnd',
                                'dischargeTimeWindowStart', 'dischargeTimeWindowEnd']].copy()

        # Time windows are strings '%Y-%m-%dT%H:%M:%SZ' so we need to convert them to datetime
        time_windows = time_windows.map(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))

        time_windows["loadTimeWindow"] = time_windows["loadTimeWindowEnd"] - time_windows["loadTimeWindowStart"]
        time_windows["dischargeTimeWindow"] = time_windows["dischargeTimeWindowEnd"] - time_windows["dischargeTimeWindowStart"]

        # get the mean, max and min of the time differences
        time_diff = time_windows[["loadTimeWindow", "dischargeTimeWindow"]].describe()


        return time_diff

    def analyse_booking_dates(self):
        """
        The function will analyse the booking dates and return the mean, max and min of the time differences
        :return:
        """

        # Create a groupby object to group the data by bookingCreateDate
        booking_dates = self.df.groupby("bookingDateCreated")['bookingDateCreated'].count()

        return booking_dates

    def check_for_column_references(self):
        """
        The function will check for references in the dataframe columns and returns a dictionary with the references
        :return: {matches: int, missing: int, references: dict}
        """

        known_references = load_datatable_from_db("column_references")

        # Get the column names of the self.df that match with known_references["col_ref_name"]
        matches = self.df.columns[self.df.columns.isin(known_references["col_ref_name"])]
        len_matches = len(matches)
        list_matches = list(matches)
        len_missing = len(self.df.columns) - len_matches
        list_missing = [col for col in self.df.columns if col not in list_matches]


        if len_matches == 0:
            return {"matches": 0, "missing": len(self.df.columns)}
        # check if the matches have a meta_data_id
        referenced_columns = known_references[known_references["col_ref_name"].isin(matches)]

        meta_data_ids = list(referenced_columns["meta_data_id"].dropna().unique())
        reference_source = list(referenced_columns["col_ref_source"].dropna().unique())
        most_common_source = referenced_columns.groupby("col_ref_source")["col_ref_source"].count().idxmax()

        return {"amount_matches": len_matches,
                "amount_missing": len_missing,
                "most_common_source": most_common_source,
                "matches": list_matches,
                "missing": list_missing}

    def transform_container_columns(self):
        """
        :return:
        """
        get_references = TransformContainers(self.df, most_common_source)

        merge_cols = get_references.column_meta_data

        get_references.source_adjustments()
        get_references.rename_columns()

        get_references.fill_missing_time_values()
        get_references.fill_missing_booking_create_date()

        self.df = get_references.container_input

        return self.df

    def summary(self):
        """

        :return:
        """

        string_divider = f"\n----------------------------------------------\n\n"


        strg_output = "##############################################\n" \
                      "###           Dataframe information        ###\n" \
                      "##############################################\n"

        strg_output += f"The owner of the data is: {self.owner}\n"
        strg_output += f"The data steward is: {self.data_steward}\n"
        strg_output += f"The file was created on: {self.created}\n"

        strg_output += string_divider

        strg_output += f"The dataframe counts {self.df.shape[0]} rows and {self.df.shape[1]} columns\n"
        strg_output += f"The column names are:\n{self.df.columns}\n"

        strg_output += string_divider

        strg_output += f"Missing values in the dataframe:\n{self.missing_values}\n"

        strg_output += string_divider

        strg_output += f"Duplicates in the dataframe:\n{self.duplicates}\n"

        strg_output += string_divider

        strg_output += f"Column references:\n{json.dumps(self.column_references, indent=4)}\n"

        strg_output += string_divider

        strg_output += string_divider


        strg_output += string_divider


        return strg_output

    def __str__(self):

        return self.summary()



if __name__ == "__main__":

    # Load the CSV file with custom date parser
    containers = pd.read_csv(
        "services/exploration/2407081700_gil_converted_containers.csv",
    )

    date_time_columns = ['loadTimeWindowStart', 'loadTimeWindowEnd', 'dischargeTimeWindowStart','dischargeTimeWindowEnd']

    for date_time in date_time_columns:
        containers[date_time] = pd.to_datetime(containers[date_time], format='%Y-%m-%dT%H:%M:%SZ')

    containers['bookingDateCreated'] = pd.to_datetime(containers['bookingDateCreated'], format='%Y-%m-%d')

    # round all date_time_columns to hour
    for date_time in date_time_columns:
        containers[date_time] = containers[date_time].dt.round('h')

    containers.sort_values(by=['loadTimeWindowStart','loadTerminal']
                           , inplace=True)

    # viz_containers = VizualisationContainerOrders(containers)
    # viz_containers.order_load_date_trend().show()

    # get minimum date of loadTimeWindowStart and create timestamp at 08:00
    min_date = containers['loadTimeWindowStart'].min()
    max_date = containers['loadTimeWindowStart'].max()
    min_date_08 = min_date.replace(hour=8, minute=0, second=0)
    max_date_08 = max_date.replace(hour=8, minute=0, second=0)
    cut_intervals = pd.date_range(start=min_date_08, end=max_date_08, freq='d')

    # Extract the weekday from the 'timestamp' column
    containers['weekday'] = containers['loadTimeWindowStart'].dt.weekday

    # Group by the 'weekday' column and apply an aggregation function
    grouped_df = containers.groupby('weekday').count()


    print(containers['loadTimeWindowStart'])
    for i, cut_moment in enumerate(cut_intervals):
        containers_cut = containers[containers['loadTimeWindowStart'] < cut_moment]
        containers.drop(containers_cut.index, inplace=True)
        file_name = f"containers_{str(cut_moment.strftime('%y%m%d'))}.csv"
        print("\n-------------\n")
        print(f"The current day is: {file_name}")
        print(f"the current shape of containers this day: {containers_cut.shape[0]}")
        print(f"the  containers in the future: {containers.shape[0]}")
        containers_cut.to_csv(f'data/{file_name}.csv', index=False)


    print(grouped_df)
