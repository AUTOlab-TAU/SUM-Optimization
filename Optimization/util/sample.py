import sys
import os
import pandas as pd
from typing import Any

# Add the directory containing setup.py to the sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from setup import *

def load_request_pool() -> pd.DataFrame:
    """Load and preprocess the pre-generated origin-destination request pool.
    
    Loads travel requests from a CSV file containing origin-destination pairs with:
    - Request times
    - Travel durations and distances for different modes
    - TAZ (Traffic Analysis Zone) information
    - Public transit accessibility metrics
    
    Returns:
        DataFrame with columns:
            orig_taz (int): Origin TAZ ID
            dest_taz (int): Destination TAZ ID
            start (int): Trip start time
            end (int): Trip end time
            orig_median_pt (float): Origin PT accessibility metric
            dest_median_pt (float): Destination PT accessibility metric
            car_time (float): Car travel time
            rq_time (float): Request time
            And other mode-specific time and distance columns
    """
    #print(f"Loading all pregenerated OD pairs with rq_time and duration & distance by mode")
    df = pd.read_csv(f"{requestsfilepath}")
    df["orig_taz"] = df["orig_taz"].astype(int)
    df["dest_taz"] = df["dest_taz"].astype(int)
    df["start"] = df["start"].astype(int)
    df["end"] = df["end"].astype(int)
    df["orig_median_pt"] = df["orig_median_pt"].astype(float)
    df["dest_median_pt"] = df["dest_median_pt"].astype(float)
    return df

def sample_request_pool(request_pool: pd.DataFrame, demand_ratio: float) -> pd.DataFrame:
    """Sample a subset of requests from the request pool based on demand ratio.
    
    Takes a random sample from the request pool, scaling the sample size based on
    the ratio between target demand and the request file's built-in demand ratio.
    
    Note: Does not stratify sample by origin-destination pair & time.
    
    Args:
        request_pool: DataFrame containing all possible travel requests
        demand_ratio: Target demand ratio where 1.0 = original Decell volume
            (e.g., 1.252 means 125.2% of original demand)
    
    Returns:
        DataFrame containing the sampled subset of requests, sorted by request time.
        Has same columns as input request_pool but fewer rows based on sampling ratio.
    """
    pool_size = len(request_pool)
    sample_ratio = demand_ratio/requestfile_ratio
    df = request_pool.sample(n=round(pool_size*sample_ratio),replace=False) #do not allow the same row twice to keep IDs unique
    df.sort_values(by=["rq_time"])
    return df
    