# NOTE: THE ROUTER SOMETIMES FAILS FOR NODES IN THE SAME TAZ

from typing import Dict, List, Tuple, Any, Union
import pickle
import geopandas as gpd
import pandas as pd
from pyproj import Transformer
from shapely.ops import transform
import osmnx as ox
import networkx as nx
import numpy as np
import random as rnd
import openrouteservice as ors
import biogeme.database as db
from biogeme.expressions import Variable, Beta
import biogeme.biogeme as bio
from biogeme import models
from plotnine import ggplot, aes, geom_line, geom_point, scale_shape_manual, theme_minimal, scale_linetype_manual, scale_size_manual



# INPUT
#   table of street node IDs with corresponding TAZ ids
#   origin-destination-time table showing volume of travel between polygons at different times of day
# OUTPUT
#   request table with origin, destination, time of request, and attributes (e.g. travel time) for different modes of travel


def get_travel_metrics(origin: List[float], destination: List[float]) -> Dict[str, Dict[str, float]]:
    """Calculate travel metrics between two points for different transportation modes.
    
    Uses OpenRouteService to compute duration and distance for different travel modes
    between origin and destination coordinates.
    
    Args:
        origin: [longitude, latitude] coordinates of trip origin
        destination: [longitude, latitude] coordinates of trip destination
    
    Returns:
        Dictionary mapping mode profiles to their metrics:
            'driving-car': {'duration': float, 'distance': float}
            'cycling-regular': {'duration': float, 'distance': float}
            'cycling-electric': {'duration': float, 'distance': float}
            'foot-walking': {'duration': float, 'distance': float}
        Where duration is in seconds and distance in kilometers
    """
    result = {}
    for profile in ["driving-car","cycling-regular","cycling-electric","foot-walking"]: #,"public-transport"]:
        client = ors.Client(base_url='http://localhost:8080/ors') # instantiating this here every call may have overhead but keeps main code clean
        matrix = client.distance_matrix(
            locations=[origin,destination],
            profile=profile,
            metrics=["duration", "distance"],
            units="km",
            validate=False,
            sources=["0"],      # only need to calculate in one direction
            destinations=["1"]
        )
        result[profile] = {}
        result[profile]["duration"] = matrix["durations"][0][0]
        result[profile]["distance"] = matrix["distances"][0][0]
    return result


# INPUT 
#   filepath to nodeid polyid association table
#  
# OUTUT
#   dictionary with polyid key and list of nodes in that poly
def create_taznodes_dict(df: pd.DataFrame) -> Dict[int, List[int]]:
    """Create mapping from TAZ (Traffic Analysis Zone) IDs to their contained nodes.
    
    Args:
        df: DataFrame with columns:
            'taz': TAZ ID
            'node_index': Node IDs within that TAZ
    
    Returns:
        Dictionary mapping TAZ IDs to lists of node IDs contained within each TAZ
    """
    grouped_df = df.groupby("taz")["node_index"].apply(list).reset_index()
    result = grouped_df.set_index("taz").to_dict()["node_index"]
    return result

# INPUT
#   csv with column names "origin","destination","h0" (midnight to 1am), "h1" (1am to 2am), etc with float values in cells representing avg number of travellers.
# OUTPUT
#   dict with key format h0, h1, h2 representing hour of the day and dict values in format "originID_destinationID" with values representing flows
def convert_od_file_to_dict(filepath: str) -> Dict[str, Dict[str, float]]:
    """Convert origin-destination flow matrix CSV to nested dictionary structure.
    
    Processes CSV containing hourly travel demand between TAZ pairs.
    
    Args:
        filepath: Path to CSV file with columns:
            'origin': Origin TAZ ID
            'destination': Destination TAZ ID
            'h0', 'h1', etc.: Hourly flow volumes from midnight onwards
    
    Returns:
        Dictionary with structure:
            {'h0': {'origin_destination': flow_value}, ...}
        Where h0, h1 etc. are hours from midnight
        And origin_destination is formatted as 'originID_destinationID'
    """
    print("converting OD table to dictionary")
    df = pd.read_csv(filepath)
    result = {}
    # hour columns in CSV start with "h"
    hour_columns = [col for col in df.columns if col.startswith('h')]
    for hour_col in hour_columns:
        result[hour_col] = {}
    # populate the dict
    for _, row in df.iterrows():
        origin = int(row['origin'])
        destination = int(row['destination'])
        for hour_col in hour_columns:
            key = f"{origin}_{destination}"
            value = row[hour_col]
            result[hour_col][key] = value
    return result

def create_nodes_dict(df: pd.DataFrame) -> Dict[int, Dict[str, float]]:
    """Create mapping from node IDs to their coordinates.
    
    Args:
        df: DataFrame with columns:
            'node_index': Node ID
            'x': Longitude
            'y': Latitude
    
    Returns:
        Dictionary mapping node IDs to coordinate dictionaries:
            {node_id: {'x': longitude, 'y': latitude}}
    """
    result = {}
    for _, row in df.iterrows():
        node_index = int(row['node_index'])
        result[node_index] = {}
        result[node_index]['x'] = row['x']
        result[node_index]['y'] = row['y']
    return result




# simulation parameters
demand_start_hour = 7
demand_num_hours = 2
demand_end_hour = demand_start_hour+demand_num_hours

# cost assumptions
car_fuel_consumption_liters_per_km = 1.0/10.0
fuel_cost_shekels_per_liter = 7.78 
pt_cost_shekels = 6.0
nsm_cost_shekels = 8.0

car_ownership_rate = 0.67 # from survey


inpath = "D:\\users\\davideps\\Jerusalem\\SUM-Optimization\\JerusalemData\\demand\\Processed"
nodes_df = pd.read_csv(f"{inpath}\\fleetpy_node_taz.csv")
nodes_df["taz"] = nodes_df["taz"].astype(int)
taznodes = create_taznodes_dict(nodes_df) #returns dict
nodes = create_nodes_dict(nodes_df)
ignore_polys = [4711] # ignore these taz (outside target neighborhood or city, measurement error, etc)
ods = convert_od_file_to_dict("D:\\users\\davideps\\Jerusalem\\SUM-Optimization\\JerusalemData\\demand\\Processed\\weekday_top15_hourly.csv")
requests = {}

# Estimate PT compared to car travel (if GTFS is not available)
pt_duration_mult = 2.0
pt_distance_mult = 1.75

rid = 0 # request id
volume_scale = 30.0
print(f"GENERATING REQUESTS FROM TIME {demand_start_hour} TO {demand_start_hour+demand_num_hours}")
for h in range(demand_start_hour,demand_end_hour):
    print(f"HOUR:{h}")
    print("----------------------------------------------------")
    hkey = f"h{h}"
    for odkey in ods[hkey].keys():
        volume = ods[hkey][odkey] * volume_scale
        origid = int(odkey.split("_")[0])
        destid = int(odkey.split("_")[1])
        if origid in ignore_polys or destid in ignore_polys or volume == 0:
            pass
        else:
            num_requests = np.random.poisson(volume)
            print(f"\n\torigin taz:{origid} destination taz:{destid} requests:{num_requests} --> ",end="")
            for _ in range(num_requests):
                orig_node_id = rnd.choice(taznodes[origid])
                #print(f"orig_node:{orig_node_id}")
                dest_node_id = orig_node_id
                while dest_node_id == orig_node_id: # ensure orig != dest. requires at least two nodes in every poly!
                    dest_node_id = rnd.choice(taznodes[destid])
                #print(f"dest_node:{dest_node_id}")
                orig_x = nodes[orig_node_id]['x']
                orig_y = nodes[orig_node_id]['y']
                dest_x = nodes[dest_node_id]['x']
                dest_y = nodes[dest_node_id]['y']
                #print(f"orig_x:{orig_x} orig_y:{orig_y} dest_x:{dest_x} dest_y:{dest_y}")
                metrics = get_travel_metrics([orig_x,orig_y],[dest_x,dest_y])
                #print(metrics)


                if metrics["driving-car"]["duration"] == 0: # if router fails (very rare), skip this trip
                    print("\n")
                    print(f"\t\tWARNING: router failed for origin poly {origid} node {orig_node_id} destination poly {destid} node {dest_node_id}.")
                    print(f"\t\t         orig_x:{orig_x} orig_y:{orig_y} dest_x:{dest_x} dest_y:{dest_y}")
                    print(f"\t\t         SKIPPING THIS TRIP.")
                else:
                    requests[rid] = {}
                    requests[rid]["request_time"] = np.random.uniform(h+0.0,h+1.0)
                    requests[rid]["orig_taz"] = origid
                    requests[rid]["dest_taz"] = destid
                    requests[rid]["orig_node"] = orig_node_id
                    requests[rid]["dest_node"] = dest_node_id
                    requests[rid]["orig_x"] = nodes[orig_node_id]['x']
                    requests[rid]["orig_y"] = nodes[orig_node_id]['y']
                    requests[rid]["dest_x"] = nodes[dest_node_id]['x']
                    requests[rid]["dest_y"] = nodes[dest_node_id]['y']
                    
                    car_time = metrics["driving-car"]["duration"]
                    requests[rid]["car_time"] = car_time

                    car_dist = metrics["driving-car"]["distance"]
                    requests[rid]["car_dist"] = car_dist

                    bike_regular_time = metrics["cycling-regular"]["duration"]
                    requests[rid]["bike_regular_time"] = bike_regular_time

                    bike_regular_dist = metrics["cycling-regular"]["distance"]
                    requests[rid]["bike_regular_dist"] = bike_regular_dist

                    bike_electric_time = metrics["cycling-electric"]["duration"]
                    requests[rid]["bike_electric_time"] = bike_electric_time
                    
                    bike_electric_dist = metrics["cycling-electric"]["distance"]
                    requests[rid]["bike_electric_dist"] = bike_electric_dist

                    walk_time = metrics["foot-walking"]["duration"]
                    requests[rid]["walk_time"] = walk_time

                    walk_dist = metrics["foot-walking"]["distance"]
                    requests[rid]["walk_dist"] = walk_dist

                    pt_time = metrics["driving-car"]["duration"] * pt_duration_mult
                    requests[rid]["pt_time"] = pt_time

                    pt_dist = metrics["driving-car"]["distance"] * pt_distance_mult
                    requests[rid]["pt_dist"] = pt_dist

                    # Costs
                    car_cost = car_dist * car_fuel_consumption_liters_per_km * fuel_cost_shekels_per_liter
                    requests[rid]["car_cost"] = car_cost 
                    pt_cost = pt_cost_shekels
                    requests[rid]["pt_cost"] = pt_cost

                    print(rid,end=", ")
                    rid += 1


# write to file
# columns = ['request_id','demand_row_id','request_time', 'origin_taz', 'dest_taz', 'origin_x', 'origin_y', 'dest_x', 'dest_y', 'car_time', 'car_dist', 'bike_regular_time', 'bike_regular_dist', 'bike_electric_time', 'bike_electric_dist', 'walk_time', 'walk_dist'] #, 'pt_time', 'pt_dist']
df = pd.DataFrame.from_dict(requests, orient='index')
df = df.sort_values(["request_time"])
df = df.reset_index(drop=True) 
df.to_csv(f"{inpath}\\requests_fleetpy_nodes_with_nsm_7am9am_scale30x.csv", index=False)
print(f"trips:{len(df)}")
    