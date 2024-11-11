""" This module vizualises date"""
# import plotly.express as px
import plotly.express as px
import plotly.graph_objects as go
from polyline import decode
import pytz
import datetime as dt
import pandas as pd
import numpy as np
import streamlit as st

from data.service_database import load_query_from_db, load_datatable_from_db

""" Functions for retrieving data from the database."""


class VisualizationContainerOrders():
    """
    This Data visualisation is for the viewer to  understand the quality of the data. Understand the complexity of the
    task given to the PMA planning algorithm. And a general overview of the data.
    """

    def __init__(self, dataframe_containers):
        self.df = dataframe_containers

    def data_completeness_overview(self):
        """
        Description: This visualization shows the percentage of missing values for each data field.
        Purpose: Understand data completeness and identify fields with missing data.
        Type: Dataframe
        Values used: Count of missing values
        Title: Data Completeness Overview
        X-axis: Data fields
        Y-axis: Percentage of filled values

        :return: dataframe with the percentage of missing values for each data field
        """

        # Dataframe with the headers and the amount of filled values in percentage
        filled_values_prct = (self.df.count() / len(self.df))
        df_filled_values_prct = pd.DataFrame(filled_values_prct, columns=['Percentage_filled'])

        return df_filled_values_prct.T

    def container_weight_distribution(self):
        """
        Description: This visualization shows the distribution of container weights.
        Purpose: Understand the distribution of container weights.
        Type: Histogram
        Values used: Container weights
        Title: Container Weight Distribution
        X-axis: Container weights
        Y-axis: Count of containers
        :return: Weigth distribution of containers in streamlit app
        """

        # Create currect bins by taking the min, max and 25 bins equally spaced, rounded numbers
        bins = np.linspace(self.df['weight'].min(), self.df['weight'].max(), 25)

        # Create a histogram of the container weights
        fig = px.histogram(self.df, x='weight',
                           nbins=10,
                           labels={'x': 'Container weights in tonnage', 'y': 'Count of containers'},
                           title='Container Weight in tonnage Distribution')
        fig.update_xaxes(title_text="Weight in ton")
        fig.update_traces(marker_line_width=1, marker_line_color="black", opacity=0.8)  # Adjustments to show edges

        return fig

    def container_type_distribution(self):
        """
        Description: This visualization shows the distribution of container types.
        Purpose: Understand the distribution of container types.
        Type: Pie chart
        Values used: Container types
        Title: Container Type Distribution
        X-axis: Container types
        Y-axis: Count of containers
        :return: Container type distribution in streamlit app
        """

        # Count the number of containers of each type
        container_type_counts = self.df['containerType'].value_counts()

        # Create a pie chart of the container types
        fig = px.pie(container_type_counts,
                     values=container_type_counts.values,
                     names=container_type_counts.index,
                     labels={'names': 'Container types', 'values': 'Count of containers'},
                     title='Container Type Distribution')
        return fig

    def container_teu_distribution(self):
        """
        Description: This visualization shows the distribution of container TEUs.
        Purpose: Understand the distribution of container TEUs.
        Type: Pie chart
        Values used: Container TEUs
        Title: Container TEU Distribution
        X-axis: Container TEUs
        Y-axis: Count of containers
        :return: Container TEU distribution in streamlit app
        """
        # Count the number of containers of each TEU
        container_teu_counts = self.df['teu'].value_counts()

        # Create a pie chart of the container TEUs
        fig = px.pie(container_teu_counts,
                     values=container_teu_counts.values,
                     names=container_teu_counts.index,
                     labels={'names': 'Container TEUs', 'values': 'Count of containers'},
                     title='Container TEU Distribution')
        return fig

    def container_reefer_counts(self):
        """
        Description: This visualization shows the distribution of reefer and non-reefer containers.
        Purpose: Understand the distribution of reefer and non-reefer containers.
        Type: Pie chart
        Values used: Reefer and non-reefer containers
        Title: Reefer Container Distribution
        X-axis: Reefer and non-reefer containers
        Y-axis: Count of containers
        :return: Reefer container distribution in streamlit app
        """
        # Count the number of reefer and non-reefer containers
        reefer_counts = self.df['reefer'].value_counts()

        # Create a pie chart of the reefer and non-reefer containers
        fig = px.pie(reefer_counts,
                     values=reefer_counts.values,
                     names=reefer_counts.index,
                     labels={'names': 'Reefer containers', 'values': 'Count of containers'},
                     title='Reefer Container Distribution')
        return fig

    def container_dangerousGoods_counts(self):
        """
        Description: This visualization shows the distribution of dangerous and non-dangerous goods containers.
        Purpose: Understand the distribution of dangerous and non-dangerous goods containers.
        Type: Pie chart
        Values used: Dangerous and non-dangerous goods containers
        Title: Dangerous Goods Container Distribution
        X-axis: Dangerous and non-dangerous goods containers
        Y-axis: Count of containers
        :return: Dangerous goods container distribution in streamlit app
        """
        # Count the number of dangerous and non-dangerous goods containers
        dangerous_goods_counts = self.df['dangerousGoods'].value_counts()

        # Create a pie chart of the dangerous and non-dangerous goods containers
        fig = px.pie(dangerous_goods_counts,
                     values=dangerous_goods_counts.values,
                     names=dangerous_goods_counts.index,
                     labels={'names': 'Dangerous goods containers', 'values': 'Count of containers'},
                     title='Dangerous Goods Container Distribution')
        return fig

    def load_location_distribution(self):
        """
        Description: This visualization shows the distribution of load locations.
        Purpose: Understand the distribution of load locations.
        Type: Bar chart
        Values used: Load locations
        Title: Load Location Distribution
        X-axis: Load locations
        Y-axis: Count of containers
        :return: Load location distribution in streamlit app
        """
        # Count the number of containers at each load location
        load_location_counts = self.df['loadTerminal'].value_counts().reset_index()
        load_location_counts.columns = ['Load locations', 'Count of containers']

        # Create a bar chart of the load locations
        fig = px.bar(load_location_counts,
                     x='Load locations',
                     y='Count of containers',
                     labels={'x': 'Load locations', 'y': 'Count of containers'},
                     title='Load Location Distribution')
        return fig

    def discharge_location_distribution(self):
        """
        Description: This visualization shows the distribution of discharge locations.
        Purpose: Understand the distribution of discharge locations.
        Type: Bar chart
        Values used: Discharge locations
        Title: Discharge Location Distribution
        X-axis: Discharge locations
        Y-axis: Count of containers
        :return: Discharge location distribution in streamlit app
        """
        # Count the number of containers at each discharge location
        discharge_location_counts = self.df['dischargeTerminal'].value_counts()

        # Create a bar chart of the discharge locations
        fig = px.bar(discharge_location_counts,
                     x=discharge_location_counts.index,
                     y=discharge_location_counts.values,
                     labels={'x': 'Discharge locations', 'y': 'Count of containers'},
                     title='Discharge Location Distribution')
        return fig

    def load_window_duration(self):
        """
        Description: This visualization shows the distribution of load window durations.
        Purpose: Understand the distribution of load window durations.
        Type: Histogram
        Values used: Load window durations
        Title: Load Window Duration Distribution
        X-axis: Load window durations
        Y-axis: Count of containers
        :return: Load window duration distribution in streamlit app
        """

        # Convert the loadTimeWindowStart and loadTimeWindowEnd columns to datetime objects
        self.df['loadTimeWindowStart'] = pd.to_datetime(self.df['loadTimeWindowStart'])
        self.df['loadTimeWindowEnd'] = pd.to_datetime(self.df['loadTimeWindowEnd'])

        # Calculate the load window durations in hours by subtracting the loadTimeWindowStart from the LoadTimeWindowEnd
        self.df['load_window_duration'] = (self.df['loadTimeWindowEnd'] - self.df[
            'loadTimeWindowStart']).dt.total_seconds() / 3600

        # Create analytical description of the load window durations
        load_window_duration_description = self.df['load_window_duration'].describe()

        # Create a box plot of the load window durations
        fig = px.box(self.df, y='load_window_duration',
                     labels={'y': 'Load window durations'},
                     title='Load Window Duration Distribution')

        # Add the analytical description to the box plot
        fig.add_trace(go.Scatter(x=[0], y=[load_window_duration_description['mean']],
                                 mode='markers', name='Mean', marker=dict(color='red')))
        fig.add_trace(go.Scatter(x=[0], y=[load_window_duration_description['25%']],
                                 mode='markers', name='Q1', marker=dict(color='blue')))
        fig.add_trace(go.Scatter(x=[0], y=[load_window_duration_description['50%']],
                                 mode='markers', name='Median', marker=dict(color='green')))
        fig.add_trace(go.Scatter(x=[0], y=[load_window_duration_description['75%']],
                                 mode='markers', name='Q3', marker=dict(color='blue')))

        return fig

    def discharge_window_duration(self):
        """
        Description: This visualization shows the distribution of discharge window durations.
        Purpose: Understand the distribution of discharge window durations.
        Type: Histogram
        Values used: Discharge window durations
        Title: Discharge Window Duration Distribution
        X-axis: Discharge window durations
        Y-axis: Count of containers
        :return: Discharge window duration distribution in streamlit app
        """

        # Convert the dischargeTimeWindowStart and dischargeTimeWindowEnd columns to datetime objects
        self.df['dischargeTimeWindowStart'] = pd.to_datetime(self.df['dischargeTimeWindowStart'])
        self.df['dischargeTimeWindowEnd'] = pd.to_datetime(self.df['dischargeTimeWindowEnd'])

        # Calculate the discharge window durations in hours by subtracting the dischargeTimeWindowStart from the dischargeTimeWindowEnd
        self.df['discharge_window_duration'] = (self.df['dischargeTimeWindowEnd'] - self.df[
            'dischargeTimeWindowStart']).dt.total_seconds() / 3600

        # Create analytical description of the discharge window durations
        discharge_window_duration_description = self.df['discharge_window_duration'].describe()

        # Create a box plot of the discharge window durations
        fig = px.box(self.df, y='discharge_window_duration',
                     labels={'y': 'Discharge window durations'},
                     title='Discharge Window Duration Distribution',
                     )

        # Add the analytical description to the box plot. Add the values to legend
        fig.add_trace(go.Scatter(x=[0], y=[discharge_window_duration_description['mean']],
                                 mode='markers', name='Mean', marker=dict(color='red')))
        fig.add_trace(go.Scatter(x=[0], y=[discharge_window_duration_description['25%']],
                                 mode='markers', name='Q1', marker=dict(color='blue')))
        fig.add_trace(go.Scatter(x=[0], y=[discharge_window_duration_description['50%']],
                                 mode='markers', name='Median', marker=dict(color='green')))
        fig.add_trace(go.Scatter(x=[0], y=[discharge_window_duration_description['75%']],
                                 mode='markers', name='Q3', marker=dict(color='blue')))

        return fig

    def window_duration_distribution(self):
        # Convert the loadTimeWindowStart and loadTimeWindowEnd columns to datetime objects
        time_series = ['loadTimeWindowStart', 'loadTimeWindowEnd', 'dischargeTimeWindowStart', 'dischargeTimeWindowEnd']
        for time in time_series:
            self.df[time] = pd.to_datetime(self.df[time])

        data_record_load = {
            "duration": (self.df['loadTimeWindowEnd'] - self.df['loadTimeWindowStart']).dt.total_seconds()}
        data_record_load["type"] = data_record_load["duration"].apply(lambda x: "load")

        data_record_discharge = {
            "duration": (self.df['dischargeTimeWindowEnd'] - self.df['dischargeTimeWindowStart']).dt.total_seconds()}
        data_record_discharge["type"] = data_record_discharge["duration"].apply(lambda x: "discharge")

        window_duration = pd.concat([data_record_load, data_record_discharge])

        fig = px.box(window_duration, x="type", y="window_duration", points="all")

        return fig

    def order_creation_trend(self):
        """
        Description: This visualization shows the trend of order creation over time.
        Purpose: Understand the trend of order creation over time.
        Type: Line chart
        Values used: Order creation dates
        Title: Order Creation Trend
        X-axis: Order creation dates
        Y-axis: Count of orders
        :return: Order creation trend in streamlit app
        """

        # Convert the bookingDateCreated column to a datetime object
        self.df['bookingDateCreated'] = pd.to_datetime(self.df['bookingDateCreated'])

        # Count the number of orders created on each day
        order_creation_trend = self.df['bookingDateCreated'].dt.date.value_counts().sort_index()

        # Create a line chart of the order creation trend
        fig = px.line(order_creation_trend,
                      labels={'index': 'Booking creation dates', 'value': 'Count of orders'},
                      title='Booking Creation Trend')

        # Change the layout of the line chart,fill the area under the line
        fig.update_traces(fill='tozeroy')

        return fig

    def order_load_date_trend(self):
        """
        Description: This visualization shows the trend of order creation over time.
        Purpose: Understand the trend of order creation over time.
        Type: Line chart
        Values used: Order creation dates
        Title: Order Creation Trend
        X-axis: Order creation dates
        Y-axis: Count of orders
        :return: Order creation trend in streamlit app
        """

        # Convert the bookingDateCreated column to a datetime object
        self.df['loadTimeWindowStart'] = pd.to_datetime(self.df['loadTimeWindowStart'])

        # Count the number of orders created on each day
        order_creation_trend = self.df['loadTimeWindowStart'].dt.date.value_counts().sort_index()

        # Create a line chart of the order creation trend
        fig = px.line(order_creation_trend,
                      labels={'index': 'Load time window start date', 'value': 'Count of orders'},
                      title='Load time window trend')

        # Change the layout of the line chart,fill the area under the line
        fig.update_traces(fill='tozeroy')

        return fig

    def order_import_export_date_trend(self):
        """
        Description: This visualization shows the trend of order creation over time.
        Purpose: Understand the trend of order creation over time.
        Type: Line chart
        Values used: Order creation dates
        Title: Order Creation Trend
        X-axis: Order creation dates
        Y-axis: Count of orders
        :return: Order creation trend in streamlit app
        """

        # Convert the bookingDateCreated column to a datetime object
        self.df['loadTimeWindowStart'] = pd.to_datetime(self.df['loadTimeWindowStart'])
        self.df['dischargeTimeWindowEnd'] = pd.to_datetime(self.df['dischargeTimeWindowEnd'])

        temp_df = self.df.copy()

        # Sum the TEU per day based in the load and discharge external id
        temp_df['loadDayStart'] = temp_df['loadTimeWindowStart'].dt.date
        temp_df['dischargeDayEnd'] = temp_df['dischargeTimeWindowEnd'].dt.date

        order_import_trend = temp_df[temp_df['loadExternalId'] == 1].groupby('loadDayStart').agg({'teu': 'sum'})
        order_export_trend = temp_df[temp_df['dischargeExternalId'] == 1].groupby('dischargeDayEnd').agg({'teu': 'sum'})

        # Create a line chart of the order creation trend
        fig = px.line(order_import_trend,
                      labels={'index': 'Port cut-off or pick-up time', 'value': 'TEU of orders'},
                      title='TEU import and export numbers')
        fig.update_traces(name="Import orders")
        fig.update_xaxes(title_text="Port cut-off (export) or pick-up time (import)")

        # add a second line to the fig with the export trend
        fig.add_trace(
            go.Scatter(x=order_export_trend.index, y=[val[0] for val in order_export_trend.values], mode='lines',
                       name='Export orders'))

        return fig

    def random_teu_supply_barges(self):
        """
        Generates a random set of TEU capacity available to plan for.

        :return: a plotly line figure
        """

        # get all the dates of self.df['loadTimeWindowStart'] and self.df['dischargeTimeWindowEnd']
        dates = pd.concat([self.df['loadTimeWindowStart'].dt.date, self.df['dischargeTimeWindowEnd'].dt.date]).unique()
        dates = sorted(dates)
        # divide len dates by 3
        onethirddates = len(dates) // 3

        start_teus = list(np.random.randint(100, 250, onethirddates))
        middle_teus = list(np.random.randint(250, 438, onethirddates))
        end_teus = list(np.random.randint(430, 438, onethirddates + 5))

        # per date generate a random number of TEU capacity available to plan for with max of 1000
        teu_capacity = start_teus + middle_teus + end_teus
        teu_capacity = teu_capacity[:len(dates)]
        rndm_teu = pd.DataFrame({'date': dates, 'teu_capacity': teu_capacity}).sort_values('date')

        # create a plotly line figure with the dates on the x-axis and the TEU capacity on the y-axis
        fig = px.line(rndm_teu,
                      x='date',
                      y='teu_capacity',
                      title='TEU capacity available to plan for',
                      labels={'date': 'Date', 'teu_capacity': 'TEU capacity'})

        fig.update_layout(title='Random TEU capacity available to plan for',
                          xaxis_title='Date',
                          yaxis_title='TEU capacity')
        fig.update_traces(fill='tozeroy')

        return fig

    def order_creation_by_location(self):
        """
        Description: This visualization shows the distribution of order creation by location.
        Purpose: Understand the distribution of order creation by location.
        Type: Bar chart
        Values used: Load locations, Discharge locations
        Title: Order Creation by Location
        X-axis: Locations
        Y-axis: Count of orders
        :return: Order creation by location in streamlit app
        """

        # Count the number of orders created at each location
        load_location_counts = self.df['loadTerminal'].value_counts()
        discharge_location_counts = self.df['dischargeTerminal'].value_counts()

        # Create a bar chart of the order creation by location
        fig = go.Figure(data=[
            go.Bar(name='Load locations', x=load_location_counts.index, y=load_location_counts.values),
            go.Bar(name='Discharge locations', x=discharge_location_counts.index, y=discharge_location_counts.values)
        ])

        fig.update_layout(barmode='group', title='Order Creation by Location', xaxis_title='Locations',
                          yaxis_title='Count of orders')

        return fig

    def dangerous_goods_and_reefer_per_location(self):
        """
        Description: This visualization shows the distribution of dangerous goods and reefer containers per location.
        Purpose: Understand the distribution of dangerous goods and reefer containers per location.
        Type: Bar chart
        Values used: Load locations, Discharge locations, Dangerous goods, Reefer containers
        Title: Dangerous Goods and Reefer Containers per Location
        X-axis: Locations
        Y-axis: Count of containers
        :return: Dangerous goods and reefer containers per location in streamlit app
        """

        # Count the number of dangerous goods and reefer containers at each location
        dangerous_goods_per_location = self.df.groupby('loadTerminal')['dangerousGoods'].sum()
        reefer_per_location = self.df.groupby('loadTerminal')['reefer'].sum()

        # Create a bar chart of the dangerous goods and reefer containers per location
        fig = go.Figure(data=[
            go.Bar(name='Dangerous Goods', x=dangerous_goods_per_location.index, y=dangerous_goods_per_location.values),
            go.Bar(name='Reefer Containers', x=reefer_per_location.index, y=reefer_per_location.values)
        ])

        fig.update_layout(barmode='group', title='Dangerous Goods and Reefer Containers per Location',
                          xaxis_title='Locations', yaxis_title='Count of containers')

        return fig

    def traveled_routes(self):
        """
        Description: This visualization shows the distribution of traveled routes.
        Purpose: Understand the distribution of container orders on traveled routes.
        Type: a graph plot where the thickness of the line represents the number of orders on that route.
        Values used: Load locations, Discharge locations
        Title: Traveled Routes
        :return: Traveled routes in streamlit app
        """

        pass


class VisualizationPlanning():

    def __init__(self, calls, transit, occupancy):
        self.dataframe_calls = calls
        self.dataframe_transit = transit
        self.dataframe_occupancy = occupancy

    def add_barge_names(self):
        """
        For every barge id in the dataframe, add the barge name to the dataframe.

        :return:
        """
        barge_ids = (self.dataframe_calls['barge_id'].unique())

        query = f"SELECT barge_id, name " \
                f"FROM barges " \
                f"WHERE barge_id IN ({', '.join(map(str, barge_ids))})"

        retrieve_barge_names = load_query_from_db(query)

        self.dataframe_calls = self.dataframe_calls.merge(retrieve_barge_names, on='barge_id', how='left')
        self.dataframe_transit = self.dataframe_transit.merge(retrieve_barge_names, on='barge_id', how='left')
        self.dataframe_occupancy = self.dataframe_occupancy.merge(retrieve_barge_names, on='barge_id', how='left')

    def calls_gantt_chart(self):
        """
        Description: This visualization shows the calls in a Gantt chart.
        Purpose: Understand the calls and their duration.
        Type: Gantt chart
        Values used: Call start time, Call end time
        Title: Calls Gantt Chart
        X-axis: Time
        Y-axis: Calls
        :return: Calls Gantt chart in streamlit app
        """

        fig = px.timeline(self.dataframe_calls, x_start='start_date_time',
                          x_end='end_date_time',
                          y='name',
                          color='terminal_id',
                          labels={'terminal_id': 'Terminal ID', 'start_date_time': 'Arrival time',
                                  'end_date_time': 'Departure time', 'teu_loaded': 'TEU loaded',
                                  'teu_discharged': 'TEU discharged', 'teu_on_board': 'TEU on board'},
                          title='Barge Schedule Gantt Chart',
                          hover_name=None,
                          hover_data={'name': False, 'terminal_id': True, 'start_date_time': True,
                                      'end_date_time': True, 'teu_loaded': True, 'teu_discharged': True,
                                      'teu_on_board': True}
                          )
        fig.update_yaxes(categoryorder='total ascending')

        fig.update_layout(xaxis_title='Date',
                          yaxis_title='Barge Name',
                          legend=dict(
                              orientation='h',
                              yanchor='bottom',
                              y=1.02,
                              xanchor='left',
                              x=0.1
                          )
                          )

        return fig

    def occupancy_timeline_chart(self):
        """

        :return:
        """

        # Create a Plotly figure
        fig = go.Figure()

        # Iterate over unique barge IDs
        unique_names = self.dataframe_occupancy['name'].unique()

        for name in unique_names:
            # Add traces for occupied, available, and capacity TEU for each barge
            barge_data = self.dataframe_occupancy[self.dataframe_occupancy['name'] == name]
            barge_data['norm_occupancy_teu'] = barge_data['occupancy_teu'] / barge_data['capacity_teu']

            fig.add_trace(go.Scatter(x=barge_data['date_time'], y=barge_data['norm_occupancy_teu'],
                                     mode='lines', name=f'Barge {name} - Occupied TEU', stackgroup='one'))

        fig.update_layout(title='TEU allocation per barge, over time, normalized by capacity',
                          yaxis=dict(tickformat=".0%", title='Percentage'),
                          xaxis_title='Date')

        # Show plot
        return fig

    def stack_teu_occupancy(self):
        """
        The transit shows the TEU capacity of the barges per transit.
        Filter the data on DEPART. Than stack the occupacy teu per barge and color with transit location.


        :return:
        """

        # Filter on DEPART
        departures = self.dataframe_transit[self.dataframe_transit['transit_type'] == 'DEPART']

        # Create a bar chart of the TEU allocation per barge, per location
        fig = px.bar(departures, x='name', y='transit_occupancy_teu', color="transit_location_id")

        # add title "TEU allocation per barge, per location", x-axis "Barge name", y-axis "TEU"
        fig.update_layout(title='TEU allocation per barge, per location',
                          xaxis_title='Barge name',
                          yaxis_title='TEU')

        return fig

    def map_transit(self):
        """
        Description: This visualization shows the transit on a map.
        Purpose: Understand the transit of barges.
        Type: Map
        Values used: Latitude, Longitude
        Title: Transit Map
        :return: Transit map in streamlit app
        """

        self.dataframe_transit['terminal_code'] = [terminal_id[5:] for terminal_id in
                                                   self.dataframe_transit['transit_location_id']]
        terminal_codes = list(self.dataframe_transit['terminal_code'].unique())
        terminal_codes_str = ', '.join(f"'{code}'" for code in terminal_codes)

        t_query = f"""SELECT terminal_code, latitude, longitude FROM terminals WHERE terminal_code IN ({terminal_codes_str})"""
        retrieve_terminal_coordinates = load_query_from_db(t_query)

        self.dataframe_transit = self.dataframe_transit.merge(retrieve_terminal_coordinates,
                                                              on='terminal_code',
                                                              how='left')

        fig = go.Figure()

        fig.add_trace(go.Scattergeo(
            lon=self.dataframe_transit['longitude'],
            lat=self.dataframe_transit['latitude'],
            hoverinfo='text',
            text=self.dataframe_transit['terminal_code'],
            mode='markers',
            marker=dict(
                size=2,
                color='rgb(255, 0, 0)',
                line=dict(
                    width=3,
                    color='rgba(68, 68, 68, 0)'
                )
            )))

        return fig


class VisualizationWeatherData():
    pass


class VisualizationTideData():
    @staticmethod
    def vizualisation_tide_data(tide_data):
        """
        Description: This visualization shows the tide data.
        Purpose: Understand the tide data.
        Type: Line chart
        Values used: Tide data
        Title: Tide Data
        X-axis: Time
        Y-axis: Tide height
        :return: Tide data in streamlit app
        """

        heights = [record["sg"] for record in tide_data['data']]

        # use datetime.strftime to format the time to "2024-04-15 17:00"
        time = [dt.datetime.strptime(record["time"], '%Y-%m-%dT%H:%M:%S%z') for record in tide_data['data']]
        location = tide_data['meta']['station']['name']
        title = f'Tide height at {location}'

        # based on the data in the json, create a plotly line chart with filled area, time on x-axis, tide height on y-axis
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=time,
                                 y=heights,
                                 mode='lines',
                                 fill='tozeroy'))

        ict = pytz.timezone('Asia/Bangkok')
        ict_time = dt.datetime.now(ict)

        fig.add_vline(x=ict_time, line_dash="dash", line_color="red", line_width=1)

        # add title "Tide height at {location}", x-axis "Time", y-axis "Tide height"
        fig.update_layout(title=title,
                          xaxis_title='Time',
                          yaxis_title='Tide height')

        return fig


class MapDesign:
    def __init__(self, size, zoom):
        self.api_key = st.secrets["MAPBOX_API_KEY"]
        self.fig = go.Figure()
        self.size = size
        self.zoom = zoom

    def create_base_map(self):
        self.fig.update_layout(
            mapbox_style="mapbox://styles/flandheer/cl43og2o2001a15mryrxxpfzx",
            mapbox_accesstoken=self.api_key,
            mapbox_center={"lat": 10.68, "lon": 106.86},
            mapbox_zoom=self.zoom,
            width=self.size[0],
            height=self.size[1],
            title='Map of area',
            legend=dict(
                x=0,
                y=1,
                traceorder="normal",
                font=dict(family="sans-serif", size=12, color="black"),
                bgcolor="LightSteelBlue",
                bordercolor="Black",
                borderwidth=2
            )
        )
        return self.fig


def closest_teu(color_chart, teu):
    return min(color_chart.keys(), key=lambda x: abs(x - teu))

class VisualizationMapData(MapDesign):
    def __init__(self, size, zoom, locations=None, barges=None, corridors=None, quays=None, vessels=None,
                 anchorages=None, terminals=None):
        super().__init__(size, zoom)
        self.locations = locations
        self.barges = barges
        self.corridors = corridors
        self.quays = quays
        self.vessels = vessels
        self.anchorages = anchorages
        self.terminals = terminals

    def add_terminal_layer(self):
        self.fig.add_trace(
            go.Scattermapbox(
                lat=self.terminals['latitude'],
                lon=self.terminals['longitude'],
                mode='markers',
                marker=go.scattermapbox.Marker(size=9, color='red'),
                text= self.terminals['terminal_description'],
                name='Terminals'
            )
        )
        return self.fig

    def add_vessel_layer(self):
        self.fig.add_trace(
            go.Scattermapbox(
                lat=self.vessels['lat'],
                lon=self.vessels['lon'],
                mode='markers',
                marker=go.scattermapbox.Marker(
                    symbol='triangle',
                    angle=self.vessels['course'],
                    size=7,
                    color='purple',
                    opacity=0.7
                ),
                text=self.vessels['name'],
                name='Vessels'
            )
        )
        return self.fig

    def add_barge_layer(self):
        # Define the color chart
        color_chart = {98: ['lightblue', '98 TEU'], 128: ['royalblue', '128 TEU'], 198: ['darkblue', '198 TEU']}

        # Apply the closest TEU function to the 'teu' column
        #self.barges['teu'] = self.barges['teu'].apply(closest_teu, color_chart=color_chart)
        self.barges['teu'] = self.barges['teu'].apply(lambda x: closest_teu(color_chart, x) if x is not None else 98)

        self.barges['color'] = self.barges['teu'].apply(lambda x: color_chart[x][0])

        self.fig.add_trace(
            go.Scattermapbox(
                lat=self.barges['latitude'],
                lon=self.barges['longitude'],
                mode='markers',
                marker=go.scattermapbox.Marker(
                    angle=self.barges['course'],
                    size=11,
                    color=self.barges['color'],
                    opacity=1
                ),
                text=self.barges['name'],
                name='Barges - TEU capacity color'
            )
        )

        # Add direction lines
        for _, barge in self.barges.iterrows():
            lat = barge['latitude']
            lon = barge['longitude']
            course = barge['course']
            length = 0.001  # Length of the direction line

            # Calculate the end coordinates of the line
            end_lat = lat + length * np.cos(np.radians(course))
            end_lon = lon + length * np.sin(np.radians(course))

            self.fig.add_trace(
                go.Scattermapbox(
                    lat=[lat, end_lat],
                    lon=[lon, end_lon],
                    mode='lines',
                    line=dict(width=2, color='blue'),
                    showlegend=False
                )
            )
        return self.fig

    def add_corridor_layer(self):
        corridors = {
            'HCMC CORRIDOR WEST': 'm~e_A_{clS??qi@j[{Fpz@Q|`Akm@nTeoAqUal@nJ????aN`WyIfHsOtEeXTu_@cLeLkDiSyCuGjAiEjKcAjO}BzOwCjHmPdN}PfM}GrLcFbKuBfNkFdIiEzKaDpYwClPqEdTwC`RbA~TcAxGoCzGkFjD??k@rEQ`DyBxDk@pB??sUfo@gV~TuLb^rJpl@}iBpu@ch@rt@aIdPuVpG_Rd_@o[bHuVvPQ~\\vCxY~[tHxDha@fMzKnRzKwJrc@wClb@jFf`@jPzRr^nFtUvX??gC|ZaEfY}GhNoHdMoMhNw[zVuQrIqNhFip@`NaTdIuKzK}QxYmJjUsQdVwV~[iO`]yXxw@??gy@d~@??m_@j`@ws@re@wt@fQag@`@ej@kDw`@cPqp@ka@e}@uu@??zCfw@pEtq@{Fre@??o[pGwf@mEst@gj@{v@nJig@t_AhIn\\b_@bH??rYjAnR`D??`OnGhGlLR|M??JrYQhT????aR~^uUz]e[lIc_@oBk\\kJoWkOkO}t@cOiO_WgM{TqC_X`Cud@lTeRfEcZoJsYw[sA}[~WiNxb@wUtDyVQgUiT}W??eQeC_YJ??}SH',
            'HCMC CORRIDOR EAST': 'm~e_A_{clS??qi@j[{Fpz@Q|`Akm@nTeoAqUal@nJ????aN`WyIfHsOtEeXTu_@cLeLkDiSyCuGjAiEjKcAjO}BzOwCjHmPdN}PfM}GrLcFbKuBfNkFdIiEzKaDpYwClPqEdTwC`RbA~TcAxGoCzGkFjD??k@rEQ`DyBxDk@pB??sUfo@gV~TuLb^rJpl@}iBpu@ch@rt@aIdPuVpG_Rd_@o[bHuVvPQ~\\vCxY~[tHxDha@fMzKnRzKwJrc@wClb@jFf`@jPzRr^nFtUvX??gC|ZaEfY}GhNoHdMoMhNw[zVuQrIqNhFip@`NaTdIuKzK}QxYmJjUsQdVwV~[iO`]yXxw@??gy@d~@??m_@j`@ws@re@wt@fQag@`@ej@kDw`@cPqp@ka@e}@uu@??q]}i@wYar@qS_^??aUsY????m^ogA??xDqg@|PwZ??`A_o@??Wuf@??c~@mi@cUwn@hEgeAnf@qd@eMut@yu@sAag@p]{c@bA}m@_z@me@ae@i_AnFmbAlq@eaD|wButAnk@kw@lLgpBfJ_k@~J??qNf@??mGT',
            'HCMC CORRIDOR SOUTH': 'm~e_A_{clS??qi@j[{Fpz@Q|`Akm@nTeoAqUal@nJ????aN`WyIfHsOtEeXTu_@cLeLkDiSyCuGjAiEjKcAjO}BzOwCjHmPdN}PfM}GrLcFbKuBfNkFdIiEzKaDpYwClPqEdTwC`RbA~TcAxGoCzGkFjD??k@rEQ`DyBxDk@pB??sUfo@gV~TuLb^rJpl@}iBpu@ch@rt@aIdPuVpG_Rd_@o[bHuVvPQ~\\vCxY~[tHxDha@fMzKnRzKwJrc@wClb@jFf`@jPzRr^nFtUvX??gC|ZaEfY}GhNoHdMoMhNw[zVuQrIqNhFip@`NaTdIuKzK}QxYmJjUsQdVwV~[iO`]yXxw@??gy@d~@??m_@j`@ws@re@wt@fQag@`@ej@kDw`@cPqp@ka@e}@uu@??q]}i@wYar@qS_^??aUsY'
        }

        for name, polyline in corridors.items():
            decoded = decode(polyline)
            self.fig.add_trace(
                go.Scattermapbox(
                    lat=[p[0] for p in decoded],
                    lon=[p[1] for p in decoded],
                    mode='lines',
                    line=dict(width=4, color='pink' if name == 'HCMC CORRIDOR EAST' else 'green' if name == 'HCMC CORRIDOR WEST' else 'yellow'),
                    name=name
                )
            )
        return self.fig

    def add_port_layer(self, ports: pd.DataFrame) -> go.Figure:
        """
        Add a port layer to vizualise the allocation of containers to ports.
        :params ports: (pd.DataFrame) with port data
        :return: (go.Figure) with the port layer added
        """

        # Add locations, scale the size of the markers between 10 and 20 based on min and max values
        teus_sizes = (ports['teu'] - ports['teu'].min()) / (ports['teu'].max() - ports['teu'].min()) * 10 + 20

        self.fig.add_trace(
            go.Scattermapbox(
                lat=ports['port_latitude'],
                lon=ports['port_longitude'],
                mode='markers',
                marker=go.scattermapbox.Marker(size=teus_sizes, color='red', opacity=0.7),
                # add the port name and the teu capacity to the text
                text=ports['Port'] + ' ' + ports['teu'].astype(str) + ' TEU',
                name='Ports - TEU offered (size)'
            )
        )
        return self.fig


class VisualizationFinancialProjections():
    """
    Method to visualize the financial projections based on the input data:
    financial_transactions = {
            'voyage_number': [],
            'debtor': [],
            'creditor': [],
            'activity': [],
            'service': [],
            'unit': [],
            'amount': [],
            'tariff': [],
            'currency': [],
            'price': []
        }
    """

    def __init__(self, financial_transactions):
        self.financial_transactions = financial_transactions

    def financial_projections(self):
        """
        Create plotly barchart showing the creditor and debtor financial transactions. Meaning summarizing the price
        per creditor and debtor. The x-axis shows a party, the bar shows his credit and debt and the y-axis the price.
        :param: financial_transactions: (dict) with financial transactions
        :return: plotly barchart
        """

        debtors = self.financial_transactions.groupby('debtor')['price'].sum()
        debtors = debtors.rename('debt')
        creditors = self.financial_transactions.groupby('creditor')['price'].sum()
        creditors = creditors.rename('credit')

        debtors_creditors = pd.concat([debtors, creditors], axis=1)

        fig = go.Figure()
        fig.add_trace(go.Bar(x=debtors_creditors.index,
                             y=debtors_creditors['debt'],
                             name='Debt',
                             marker_color='lightcoral',
                             text=debtors_creditors['debt'],
                             textposition='auto'))

        fig.add_trace(go.Bar(x=debtors_creditors.index,
                             y=debtors_creditors['credit'],
                             name='Credit',
                             marker_color='LightSeaGreen',
                             text=debtors_creditors['debt'],
                             textposition='auto'))

        return fig


# def get_fig_map_data(size: tuple = (600, 900), zoom: int = 10, layers: list = None,
#                      ports: pd.DataFrame = None) -> go.Figure:
#     """
#     Generates a map visualization with various layers such as terminals, vessels, barges, corridors, and ports.
#
#     Parameters:
#     size (tuple): The size of the map (width, height).
#     zoom (int): The zoom level of the map.
#     layers (list): A list of layers to be added to the map. Possible values are 'corridors', 'terminals', 'vessels', 'barges', 'ports'.
#     ports (pd.DataFrame): A DataFrame containing port data to be added to the map.
#
#     Returns:
#     go.Figure: A Plotly figure object representing the map with the specified layers.
#     """
#     # Load terminal data from the database
#     terminal_data = load_datatable_from_db("terminals")
#
#     # Load barge data from the database
#     barge_data = load_datatable_from_db("barges")
#
#     # Request vessel location data from the API
#     request_vessels = location_tracking("VNVUT", "Cargo or Containership")
#
#     # Convert the vessel data to a DataFrame
#     ships_in_vungtau = pd.DataFrame.from_dict(request_vessels["data"]["vessels"])
#     ships_in_vungtau['mmsi'] = ships_in_vungtau['mmsi'].astype(str)
#     vessels_in_vungtau = ships_in_vungtau[ships_in_vungtau["type_specific"].isin(["Container Ship"])]
#
#     # Get ship positions and convert to a DataFrame
#     barges_json = get_ship_positions(3)
#     print(barges_json)
#     barge_pos = pd.DataFrame.from_dict(barges_json)
#
#     # Initialize the VizualisationMapData object with the loaded data
#     viz_map = VizualisationMapData(
#         zoom=zoom,
#         size=size,
#         terminals=terminal_data,
#         vessels=vessels_in_vungtau,
#         barges=barge_pos
#     )
#
#     # Create the base map
#     viz_map.create_base_map()
#
#     # Add specified layers to the map
#     if 'corridors' in layers:
#         viz_map.add_corridor_layer()
#     if 'terminals' in layers:
#         viz_map.add_terminal_layer()
#     if 'vessels' in layers:
#         viz_map.add_vessel_layer()
#     if 'barges' in layers:
#         viz_map.add_barge_layer()
#     if 'ports' in layers:
#         viz_map.add_port_layer(ports)
#
#     # Return the final map figure
#     return viz_map


def viz_barge_locations(barges:pd.DataFrame) -> go.Figure:
    """
    Create a plotly horizontal map figure with the barges, aggregated per locations
    :param barges: (pd.DataFrame) with column "location"

    """

    # Group by 'location' and count occurrences
    barge_allocations = barges.groupby('location').size().reset_index(name='count').copy()

    # Sort the DataFrame by 'count' in descending order
    barge_allocations.sort_values('count', ascending=True, inplace=True)
    barge_allocations.set_index('location', inplace=True)

    # Create a horizontal bar chart
    fig = go.Figure()

    fig.add_trace(go.Bar(x=barge_allocations['count'],
                         y=barge_allocations.index,
                         orientation='h',
                         marker_color='royalblue'))


    fig.update_layout(title='Barge Locations', xaxis_title='Number of Barges', yaxis_title='Location')

    return fig


def viz_barge_teu_locations(barges: pd.DataFrame) -> go.Figure:
    """
    Create a Plotly horizontal bar chart figure with the barges, aggregated per location and split by TEU capacity.

    :param barges: (pd.DataFrame) with column "location"
    """

    # TODO: NOW RANDOM between 98, 128, and 198, SHOULD BE ACTUAL TEU capacity

    color_chart = {98: ['lightblue', '98 TEU'], 128: ['royalblue', '128 TEU'], 198: ['darkblue', '198 TEU']}
    barges['teu'] = np.random.choice(np.array(list(color_chart.keys())),
                                     len(barges))

    # Group by 'location' and 'teu', and count occurrences
    barges['location'] = barges['location'].str.split(',').str[0]
    barges['location'] = barges['location'].apply(lambda x: 'on river' if 'River' in x else x)
    barge_allocations = barges.groupby(['location', 'teu']).size().reset_index(name='count')

    # Calculate the total count per location and sort by this value
    total_counts = barge_allocations.groupby('location')['count'].sum().sort_values(ascending=False)
    barge_allocations['location'] = pd.Categorical(barge_allocations['location'], categories=total_counts.index, ordered=True)

    # Sort the DataFrame by 'location' and 'teu'
    barge_allocations.sort_values(by=['location', 'teu'], ascending=[True, True], inplace=True)

    # Create a horizontal bar chart
    fig = go.Figure()

    # Iterate over the TEU values in ascending order to ensure proper stacking order
    for teu in sorted(barge_allocations['teu'].unique())[::]:
        teu_data = barge_allocations[barge_allocations['teu'] == teu]
        fig.add_trace(go.Bar(
            x=teu_data['count'],
            y=teu_data['location'],
            orientation='h',
            name=color_chart[teu][1],
            marker_color=color_chart[teu][0]
        ))

    # Update the layout to stack bars and position the legend in the top right corner
    fig.update_layout(
        barmode='stack',
        title='Barge Locations by TEU Capacity',
        xaxis_title='Number of Barges',
        yaxis_title='Location',
        yaxis={'categoryorder': 'total ascending'}  # Sort y-axis by total count
    )

    return fig

