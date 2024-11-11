from datetime import datetime
import networkx as nx
import pandas as pd
from shapely.wkt import loads
from shapely.geometry import Point, LineString
from polyline import decode
import geopandas as gpd
import matplotlib.pyplot as plt
from data.service_database import load_datatable_from_db

def map_node_location():
    """
    By using geoPandas snearest function we can map the nodes to the nearest location.

    :return:
    """

    nodes = load_datatable_from_db("nodes")
    terminals = load_datatable_from_db("terminals")

    # Create a GeoDataFrame from the nodes
    gdf_nodes = gpd.GeoDataFrame(nodes,
                                 geometry=gpd.points_from_xy(nodes['longitude'],
                                                             nodes['latitude']),
                                    crs='EPSG:4326')

    gdf_terminals = gpd.GeoDataFrame(terminals,
                                     geometry=gpd.points_from_xy(terminals['longitude'],
                                                                 terminals['latitude']),
                                     crs='EPSG:4326')

    gdf_nodes = gdf_nodes.to_crs(epsg=3857)
    gdf_terminals = gdf_terminals.to_crs(epsg=3857)

    # Map the nodes to the nearest terminal
    mapped = gpd.sjoin_nearest(gdf_nodes, gdf_terminals, how='left', distance_col='distance', max_distance=500)

    # drop duplicates, keep where distance is less than 500 meters
    mapped['distance'] = round(mapped['distance'], 2)
    mapped.sort_values(['node_id', 'id', 'distance'], inplace=True)

    mapped = mapped.loc[mapped.groupby('id')['distance'].idxmin()]
    map_columns = {row['node_id']:row['id']  for index, row in mapped.iterrows()}


    # Update the nodes table with the mapped node_id
    nodes['loc_id'] = nodes['node_id'].map(map_columns)


class CreateGraph:
    """A function to create a graph and calculate the shortest path between two nodes. To optimize the process we can
    slice the whole network into the area in which we need to calculate the graph.

    To improve the ETA calculation we can add the barge specifications and retrieve the average speed for the barge.

    :param area: a list of lat/lon coordinates of the area in which we retrieve the nodes and edges.
    """

    def __init__(self):

        self.nodes = load_datatable_from_db("nodes")
        self.edges = load_datatable_from_db("legs")
        self.graph = None


    def create_graph(self):
        """
        Create a graph from the nodes and edges.
        :return:
        """

        self.graph = nx.Graph()
        self.graph.add_edges_from([(edge[1], edge[2],
                                    {'distance': edge[4]}) for edge in self.edges.values])

class RouteCalculator(CreateGraph):
    """

    :param from_location: the start location of the barge.
    :param to_location: the end location of the barge.
    :param barge_type: the type of barge for which we calculate the route.
    :param barge_size: the size of the barge for which we calculate the route.
    :param start_date: the start date of the route.
    :param start_time: the start time of the route.

    """

    def __init__(self,
                 barge_type=None, barge_size=None, start_date=None, start_time=None):
        super().__init__()


        self.shortest_path_nodes = None  # [0, 1, 2, 3, 4, 5]
        self.shortest_path_length = None  # int in meters
        self.shortest_path_edges = None  # [(0, 1),(1, 2)]
        self.shortest_path_linestring = None  # LineString object

    def process_location(self, location):
        """
        Function to process the location of the barge. The location can be a description or a lat/lon tuple. It needs
        to be converted to an existing node in the graph, or a new node needs to be created.

        :param location:
        :return:
        """

        terminals = load_datatable_from_db("terminals")
        connection_points = load_datatable_from_db("nodes")
        terminals['unlo_terminal_code'] = terminals['unlocode'] + terminals['terminal_code']

        try:
            terminal_id = terminals[terminals['unlo_terminal_code'] == location].iloc[0]['id']
            return terminal_id
        except:
            raise ValueError(f"The {location} is not a valid terminal. Please provide a valid terminal code.")

    def retrieve_line_string(self):
        """
        Retrieve the LineString object from the shortest path edges.
        :return:
        """
        pass


    def calculate_shortest_path(self, from_location, to_location):
        """

        :param from_location:
        :param to_location:
        :return:
        """
        from_node = self.process_location(from_location)
        to_node = self.process_location(to_location)


        # Calculate the top k shortest-distance routes
        self.shortest_path_nodes = nx.shortest_path(self.graph,
                                                    source=from_node,
                                                    target=to_node,
                                                    weight='distance')

        self.shortest_path_length = nx.shortest_path_length(self.graph,
                                                            source=from_node,
                                                            target=to_node,
                                                            weight='distance')

        self.shortest_path_edges = [(self.shortest_path_nodes[i], self.shortest_path_nodes[i + 1]) for i in
                                    range(len(self.shortest_path_nodes) - 1)]

        return f"The shortest path constist of {len(self.shortest_path_nodes)} nodes and " \
               f"has a length of {self.shortest_path_length / 1000} km."

if __name__ == "__main__":
    calc = RouteCalculator()
    calc.create_graph()
    print(calc.calculate_shortest_path("VNVUTDGML", "VNSGNDSTR"))
    print(calc.shortest_path_nodes)
    print(calc.shortest_path_edges)
    print(calc.shortest_path_length)


