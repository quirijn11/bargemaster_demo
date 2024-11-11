import json
import pandas as pd
import datetime as dt
from data.service_database import load_datatable_from_db, load_query_from_db
from services.api_service.api_dave import get_ship_positions

class BargeDataGrader:
    def __init__(self, barges):

        self.barges = barges
        self.barge_scores = None
        self.quality_score = 0
        self.data_analyses = None

    def grade_quality(self):
        """
        Creating a new dataframe with the quality scores of the barges. Scoring on the following features:
        name_score: float
        capacity_score: float
        teu_score: float
        dimension_score: float
        mmsi_score: float
        category_score: float
        quality_score: float

        :param self.barges: Dataframe of the datatable barges.
        :return: Dataframe with the quality scores of the barges.
        """
        # Copy the barge_id, name, capacity, teu, dimension, mmsi, category from the original dataframe
        barge_scores = self.barges[['barge_id', 'name', 'gross_tonnage',
                                    'teu', 'length', 'breadth', 'mmsi', 'type']].copy()

        for label, row in barge_scores.iterrows():
            # Check if data is present and have a reasonable value
            name_score = 1 if self.barges[self.barges['name'] == row['name']].shape[0] > 1 else 0
            capacity_score = 1 if 0 < row['gross_tonnage'] < 10000 else 0
            teu_score = 1 if 0 < row['teu'] < 500 else 0
            dimension_score = 1 if 0 < row['breadth'] + row['length'] < 190 else 0
            mmsi_score = 1 if self.barges[self.barges['mmsi'] == row['mmsi']].shape[0] > 1 else 0
            type_score = 1 if row['type'] in ['Dry Bulk', 'Tanker', 'Container', 'Tug', 'Passenger'] else 0
            quality_score = (name_score + capacity_score + teu_score + dimension_score + mmsi_score + type_score) / 6

            # append the scores to the dataframe
            column_list = ['name_score', 'capacity_score',
                           'teu_score', 'dimension_score',
                           'mmsi_score', 'type_score',
                           'quality_score']

            score_list = [name_score, capacity_score,
                          teu_score, dimension_score,
                          mmsi_score, type_score,
                          quality_score]

            for column, score in zip(column_list, score_list):
                barge_scores.at[label, column] = score

        # Calculate the quality score of the barges
        self.quality_score = barge_scores['quality_score'].mean()
        self.barge_scores = barge_scores

        return barge_scores

    def analyze_barge_data(self):
        """
        Retrieve a dictionary with the data analyses of the barges. The data analyses are:
        - quantity: int
        - quality: float
        - date: datetime
        - table_data: dict {columns: list, duplicates: int, missing_values: int, unique_values: int}
        - barge_types: dict {type: quantity}
        - barge_country: dict {type: quantity}
        - barge_capacity: dict {mean: float, std: float, max: float, min: float, median: float}
        - barge_teu: dict {mean: float, std: float, max: float, min: float, median: float}
        - barge_length: dict {mean: float, std: float, max: float, min: float, median: float}
        - barge_width: dict {mean: float, std: float, max: float, min: float, median: float}
        - barge_gross_tonnage: dict {mean: float, std: float, max: float, min: float, median: float}

        :return: dict with the data analyses of the barges.
        """

        quantity = self.barges.shape[0]
        quality = self.quality_score
        date = pd.to_datetime('today').strftime('%Y-%m-%d %H:%M')
        table_data = {'columns': list(self.barges.columns),
                      'duplicates': self.barges.duplicated().sum(),
                      'missing_values': self.barges.isna().sum().sum(),
                      'unique_values': self.barges.nunique()}
        barge_types = self.barges['type'].value_counts().to_dict()
        barge_country = self.barges['country_iso'].value_counts().to_dict()
        barge_teu = self.barges['teu'].describe().to_dict()
        barge_length = self.barges['length'].describe().to_dict()
        barge_width = self.barges['breadth'].describe().to_dict()
        barge_gross_tonnage = self.barges['gross_tonnage'].describe().to_dict()

        self.data_analyses = {'quantity': quantity,
                              'quality': quality,
                              'date': date,
                              'table_data': table_data,
                              'barge_types': barge_types,
                              'barge_country': barge_country,
                              'barge_teu': barge_teu,
                              'barge_length': barge_length,
                              'barge_width': barge_width,
                              'barge_gross_tonnage': barge_gross_tonnage}

        return self.data_analyses

    def __str__(self):
        str_output = f"--------------//  Grading results barges on {self.data_analyses['date']} //--------------\n"
        str_output += f"Amount of barges:    {self.data_analyses['quantity']}.\n" \
                      f"Duplicated:          {self.data_analyses['table_data']['duplicates']}.\n"
        str_output += f"missing values:      {self.data_analyses['table_data']['missing_values']}.\n"
        str_output += f"\nQuality score:       {self.data_analyses['quality']:.2f}.\n"
        # str_output += f"\n----------------------------// Barge types //-----------------------------------------\n"
        # str_output += f"Tanker:              {self.data_analyses['barge_types']['Tanker']}.\n"
        # str_output += f"Dry Bulk:            {self.data_analyses['barge_types']['Dry Bulk']}.\n"
        # str_output += f"Container:           {self.data_analyses['barge_types']['Container']}.\n"
        # str_output += f"Tug:                 {self.data_analyses['barge_types']['Tug']}.\n"
        # str_output += f"Passenger:           {self.data_analyses['barge_types']['Passenger']}.\n"
        # str_output += f"\nThis leaves {self.data_analyses['barge_types']['Other']} " \
        #               f"barges marked as others.\n"
        str_output += f"\n----------------------------// Barge countries //--------------------------------------\n"
        for country, quantity in self.data_analyses['barge_country'].items():
            str_output += f"{country}: {quantity}.\n"

        return str_output

def dataframe_ship_positions(fleet_id = None):
    """
    Retrieve a dataframe with the ship positions of the barges.

    :param fleet_id: (int) fleet id in bos
    :return:
    """

    json_positions = get_ship_positions(fleet_id)  # based on fleet id
    position_dict = []
    start_position_dict = []

    # retrieve barges
    for barge_pos in json_positions:
        if barge_pos.get('location'):
            if barge_pos["location"].get("location"):
                location = barge_pos["location"]["location"]
        else:
            location = None

        timestamp_str = barge_pos["timestamp"]
        timestamp = dt.datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ') + dt.timedelta(hours=7)
        new_format_str = timestamp.strftime('%Y-%m-%d %H:%M')

        external_id = barge_pos["ship"]["externalId"]
        # Get barge TEU capacity from the database
        query = f"SELECT teu FROM barges WHERE call_sign = '{external_id}'"
        teu_capacity = load_query_from_db(query)['teu'][0]

        position_dict.append({"barge": barge_pos["ship"]["name"],
                              "call_sign": external_id,
                              "teu_cap": teu_capacity,
                              "average_speed": 6,
                              "in_use": True,
                              "start_position": "VNVUTDGML",
                              "last_location": location,
                              "last_signal": new_format_str})

    return pd.DataFrame(position_dict)