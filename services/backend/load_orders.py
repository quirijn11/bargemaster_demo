import pandas as pd
import json
import plotly.express as xp
from datetime import datetime


def plot_gantt_chart(dataframe):
    """
    This function will plot the gantt chart
    :return:
    """

    df = dataframe.drop(columns=['Voyage', 'START Voy', 'Start Date', 'END Voy', 'End Date', 'Europa Nbr'])

    # planned arrival to pandas datetime in format "%Y-%m-%dT%H:%M:%S"

    df['Planned arrival'] = pd.to_datetime(df['Planned arrival'], format='ISO8601')
    df['Planned departure'] = pd.to_datetime(df['Planned departure'], format='ISO8601')


    fig = xp.timeline(df, x_start='Planned arrival', x_end='Planned departure', y='Barge', color='Location')
    fig.update_yaxes(autorange="reversed")

    return fig


def plot_teu_bar(df):
    """
    This function will plot the map
    :return:
    """
    pass


    fig = xp.bar(df, x='Barge', y='TEU', color='Location')

    return fig
