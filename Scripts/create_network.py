import os
from shapely.geometry import Point, LineString, Polygon
import numpy as np
import pandas as pd
import geopandas as gpd
import osmnx as ox
import networkx as nx
import importlib.util
import sys
from pathlib import Path

# Add project root to Python path to find config
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import FLEETPY_ROOT

# Path to FleetPy OSM converter
filepath = FLEETPY_ROOT / "src" / "preprocessing" / "networks" / "extractors" / "osm_converter.py"

# Load the module
spec = importlib.util.spec_from_file_location("FleetPy", str(filepath))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

# Download street network from OSM and convert to FleetPy format
# Should place the file where it belongs in FleetPy data/networks 
# network_name is what it must be called in FleetPy scenarios
# by_name is what OSMNX package calls it. 
module.createNetwork(network_name = "OSM_Toronto", by_name="Toronto, Canada", network_type="all")
