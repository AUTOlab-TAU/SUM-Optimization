import os
from shapely.geometry import Point, LineString, Polygon
import numpy as np
import pandas as pd
import geopandas as gpd
import osmnx as ox
import networkx as nx
import importlib.util
import sys
from config import FLEETPY_PATH

# Absolute path to FleetPy (may not be necessary depending on your setup)
filepath = os.path.join(FLEETPY_PATH, "src", "preprocessing", "networks", "extractors", "osm_converter.py")

# Load the module
spec = importlib.util.spec_from_file_location("FleetPy", filepath)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

# Download street network from OSM and convert to FleetPy format
# Should place the file where it belongs in {basepath}\\data\networks 
# network_name is what it must be called in FleetPy scenarios
# by_name is what OSMNX package calls it. 
module.createNetwork(network_name = "OSM_Toronto", by_name="Toronto, Canada", network_type="all")
