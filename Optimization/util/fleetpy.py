import csv
import pandas as pd
from statistics import mean, stdev
import math
from typing import Dict, List, Any, Union
from .dcacalc import reasign_unserved_requests
from config import WORKSPACE_PATH, FLEETPY_PATH, FLEETPY_RESULTS_PATH
import os


def get_fleetpy_params(inner_loop_config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract FleetPy-specific parameters from inner loop configuration.
    
    Args:
        inner_loop_config: Configuration dictionary containing simulation parameters
            Must include keys for FleetPy operational parameters:
            - op_max_wait_time: Maximum wait time for passengers
            - op_max_detour_time_factor: Maximum allowed detour factor
            - op_fleet_composition: Fleet size and vehicle type specification
    
    Returns:
        Dictionary containing only the FleetPy-relevant parameters
    """
    config = {}
    for key in inner_loop_config:
        if key in ["op_max_wait_time","op_max_detour_time_factor","op_fleet_composition"]:
            config[key]=inner_loop_config[key]
    return config

def write_fleetpy_scenario_config_file(filename: str, i: int, num_reps: int, basename: str, fleetpy_params: Dict[str, Any]) -> None:
    """Write FleetPy scenario configuration file for current iteration.
    
    Creates a CSV configuration file for FleetPy simulation scenarios,
    with one row per replication.
    
    Args:
        filename: Path to output configuration file
        i: Current iteration number
        num_reps: Number of replications to generate
        basename: Base name for scenario identification
        fleetpy_params: Dictionary of FleetPy operational parameters
    """
    scenario = {
    "scenario_name":"SUM_test_000", # always overwritten in loop
    "op_fleet_composition":"jerusalem_petrol_van_vehtype:20", 
    "rq_type":"BasicRequest",
    "rq_file":f"demand_osm_000", # always overwritten in loop
    "op_max_wait_time":240,
    "op_max_detour_time_factor":100, 
    #"op_rh_immediate_max_routes":7, # these are from the heuristic example in src
    #"op_rvh_nr_direction":1,
    #"op_rvh_nr_least_load":1,
    #"op_rvh_AM_nr_check_assigned_rrs":1,
    #"op_rvh_AM_nr_test_best_insertions":0
    } 

    # this can be handled better by using fleetpy's scenario/constant config files
    for p in fleetpy_params:
        scenario[p] = fleetpy_params[p]

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=scenario.keys())
        writer.writeheader()
        for r in range(num_reps):
            scenario_name = f"{basename}_i{i:03}_r{r:03}"
            scenario["scenario_name"] = scenario_name
            scenario["rq_file"] = f"i{i:03}_r{r:03}_nsm.csv"            
            writer.writerow(scenario)


fleetpy_constant_config = {
    "##1,FleetSimulation":{
        "network_name":"jerusalem_osm",
        "demand_name":"jerusalem_demand",
        "random_seed":0,
        "start_time":0,
        "end_time":7200,
        "time_step":60,
        "sim_env":"BatchOfferSimulation", # "ImmediateDecisionsSimulation"
        "network_type":"NetworkBasicWithStoreCpp",
        "op_module": "RidePoolingBatchAssignmentFleetcontrol", #"PoolingIRSOnly" #"PoolingIRSAssignmentBatchOptimization"
        "op_rp_batch_optimizer":"AlonsoMora",
        "op_init_veh_distribution":"neighborhood_focus.csv",
        "route_output_flag":"TRUE",
        "replay_flag":"TRUE",
        "nr_mod_operators":1,
        "user_max_decision_time":30
    },
    "##2,FleetControl":{
        "op_min_wait_time":0,
        #"op_max_wait_time":240,
        #"op_max_detour_time_factor":100,  #100=100% increase or twice the car travel time
        "op_const_boarding_time":30,
        "op_add_boarding_time":0,
        "op_base_fare":0,
        "op_distance_fare":0,
        "op_time_fare":0,
        "op_min_standard_fare":0,
        "op_repo_method":"AlonsoMoraRepositioning",
        "op_repo_horizons":60,
        "op_vr_control_func_dict":"func_key:distance_and_user_times_with_walk;vot:0.45",
        "op_reoptimisation_timestep":60
    },
    "##3,TravelerModel":{},
    "##4,PublicTransport":{},
    "##5,DynamicNetwork and PT Crowding":{},
    "##6,Evaluation":{}
}

def write_fleetpy_constant_config_file(filename: str, config_data: Dict[str, Dict[str, Any]], sim_start_time: int, sim_end_time: int) -> None:
    """Write FleetPy constant configuration parameters to file.
    
    Creates a CSV file containing simulation parameters that remain constant
    across all iterations and replications.
    
    Args:
        filename: Path to output configuration file
        config_data: Nested dictionary of configuration parameters by section
        sim_start_time: Simulation start time in seconds
        sim_end_time: Simulation end time in seconds
    """
    config_data["##1,FleetSimulation"]["start_time"] = sim_start_time
    config_data["##1,FleetSimulation"]["end_time"] = sim_end_time
    sections = ["##1,FleetSimulation","##2,FleetControl","##3,TravelerModel","##4,PublicTransport","##5,DynamicNetwork and PT Crowding", "##6,Evaluation"]
    with open(filename, 'w') as file:
        header = ",".join(["Input_Parameter_Name","Parameter_Value"])
        file.write(header + "\n")
        # Write the section names and param rows
        for section in sections:
            #print(f"\t{section}")
            file.write(section + "\n")
            for param in config_data[section].keys():
                #print(f"\t\t{param}")
                row = f"{param},{config_data[section][param]}"
                file.write(row + "\n")


def get_fleetpy_simdata_allreps(i: int, num_reps: int, basename: str) -> List[pd.DataFrame]:
    """Load simulation results for all replications of current iteration.
    
    Args:
        i: Current iteration number
        num_reps: Number of replications
        basename: Base name for scenario identification
    
    Returns:
        List of DataFrames containing simulation evaluation metrics for each replication
    """
    result = []
    missing_files = 0
    for r in range(num_reps):
        file_path = os.path.join(FLEETPY_RESULTS_PATH, f"{basename}_i{i:03}_r{r:03}", "standard_eval.csv")
        try:
            print(f"Reading simulation data for iter {i} rep {r}")
            df = pd.read_csv(file_path, skiprows=1, header=None)  # ignores column names, permits numerical indexing of columns
            result.append(df)
        except (FileNotFoundError, pd.errors.EmptyDataError) as e:
            error_type = "Missing" if isinstance(e, FileNotFoundError) else "Empty"
            print(f"Warning: {error_type} evaluation file for iter {i} rep {r}")
            missing_files += 1
            continue
    
    # Check if too many files are missing
    if missing_files > num_reps / 4:
        raise RuntimeError(f"Too many missing/empty evaluation files: {missing_files} out of {num_reps} files missing (threshold: {num_reps/4})")
    
    return result


def calc_fleetpy_simstats(dflist: List[pd.DataFrame]) -> Dict[str, Dict[str, float]]:
    """Calculate mean and standard deviation of simulation metrics across replications.
    
    Args:
        dflist: List of DataFrames containing simulation metrics for each replication
    
    Returns:
        Nested dictionary with structure:
            {metric_name: {'mean': float, 'stdev': float}}
        For each simulation metric
    """
    # create result dictionary and populate with values from first rep (000)
    df = dflist[0]
    data = {key: [float(value)] for key, value in zip(df.iloc[:, 0], df.iloc[:, 1])}
    # append values from other reps
    for r in range(1,len(dflist)):
        df = dflist[r]
        for key, value in zip(df.iloc[:, 0], df.iloc[:, 1]):
            data[key].append(float(value))
    # compute mean & stdev
    result = {}
    for key in data:
        valuelist = data[key]
        print(f"fleetpy simstats valuelist for key '{key}': {valuelist}")
        result[key] = {}
        result[key]["mean"] = mean(valuelist)
        if len(valuelist) >= 2 and all(math.isfinite(x) for x in valuelist):
            result[key]["stdev"] = stdev(valuelist)
        else:
            print("there is no replication so stdev cannot be computed. setting to zero.")
            result[key]["stdev"] = 0
    return result

def update_requests_fleetpy_users_served(i: int, r: int, scenario_basename: str) -> pd.DataFrame:
    """Update traveler data with FleetPy simulation results for served requests.
    
    Merges simulation results with original traveler data to update service metrics
    for travelers who were successfully served by the NSM service.
    
    Args:
        i: Current iteration number
        r: Current replication number
        scenario_basename: Base name for scenario identification
    
    Returns:
        DataFrame with updated service metrics for all travelers
    """
    # key = fleetpy column name
    # value = simulation-optimization framework column name
    try:
        alltrav = pd.read_csv(os.path.join(WORKSPACE_PATH, f"i{i:03}_r{r:03}_all_presim_demand.csv"))
        requests = pd.read_csv(os.path.join(FLEETPY_RESULTS_PATH, f"{scenario_basename}_i{i:03}_r{r:03}", "1_user-stats.csv"))
    except FileNotFoundError as e:
        print(f"Warning: Missing file for iter {i} rep {r}: {str(e)}")
        return None
    except pd.errors.EmptyDataError:
        print(f"Warning: Empty file for iter {i} rep {r}")
        return None

    results = requests.dropna(subset=["pickup_time"]).copy() # requests FleetPy served
    if len(results) == 0:
        print(f"Warning: No completed requests found for iter {i} rep {r}")
        return alltrav

    results.loc[:, "nsm_wait_time"] = results.loc[:,"pickup_time"] - results.loc[:,"rq_time"]
    results.loc[:, "nsm_travel_time"] = results.loc[:,"dropoff_time"] - results.loc[:,"pickup_time"]
    results.loc[:, "nsm_total_time"] = results.loc[:,"nsm_wait_time"] + results.loc[:,"nsm_travel_time"]
    results.loc[:, "served"] = 1
    # merge & update
    merged = alltrav.merge(results[['request_id','served','nsm_wait_time','nsm_travel_time','nsm_total_time']],on='request_id', how='left', suffixes=(None, '_fleetpy')) # suffix added if there is a name conflict
    mask = merged["served_fleetpy"]==1
    # probably a slow approach
    merged.loc[mask,'nsm_wait_time'] = merged.loc[mask,'nsm_wait_time_fleetpy']
    merged.loc[mask,'nsm_travel_time'] = merged.loc[mask,'nsm_travel_time_fleetpy']
    merged.loc[mask,'nsm_total_time'] = merged.loc[mask,'nsm_total_time_fleetpy']
    #merged.loc[mask,'nsm_dist'] = merged.loc[mask,'nsm_dist_fleetpy'] #distance not used right now
    merged.loc[mask,'served'] = merged.loc[mask,'served_fleetpy']
    #delete dupe columns
    merged.drop(['nsm_wait_time_fleetpy', 'nsm_travel_time_fleetpy', 'nsm_total_time_fleetpy','served_fleetpy'], axis=1, inplace=True)
    unserved_mask = (merged["choice"]==4) & (merged["served"] != 1)
    merged.loc[unserved_mask,"served"] = 0
    merged["served"] = merged["served"].astype(int)
    return merged


def get_fleetpy_userdata_allreps(i: int, num_reps: int, scenario_basename: str) -> List[pd.DataFrame]:
    """Get updated traveler data for all replications of current iteration.
    
    Args:
        i: Current iteration number
        num_reps: Number of replications
        scenario_basename: Base name for scenario identification
    
    Returns:
        List of DataFrames containing updated traveler data for each replication
    """
    result = []
    for r in range(num_reps):
        df = update_requests_fleetpy_users_served(i,r,scenario_basename)
        result.append(df)
    return result


def update_fleetpy_unservedchoose(df: pd.DataFrame) -> pd.DataFrame:
    """Update mode choices for unserved NSM requests.
    
    Reassigns mode choices for travelers who requested NSM service but were not served,
    based on their original mode choice probabilities (excluding NSM).
    
    Args:
        df: DataFrame containing traveler data with service outcomes
    
    Returns:
        DataFrame with updated mode choices for unserved NSM requests
    """
    # unserved NSM users must choose other modes based on initial probabilities but without NSM
    # if they didn't chose NSM, "served" = -1
    df["modified"] = 0 # initialize for everyone
    df["prevchoice"] = df["choice"] # initialize for everyone
    df.loc[df["served"] == 0, "choice"] = df[df["served"] == 0].apply(reasign_unserved_requests,axis=1)
    df.loc[df["served"] == 0, "modified"] = 1
    df["unserved"] = df["modified"]
    return df
        
def calc_userstats_allreps(dflist: List[pd.DataFrame]) -> Dict[str, Dict[str, float]]:
    """Calculate service performance statistics across all replications.
    
    Computes mean and standard deviation of key service metrics:
    - NSM wait time
    - NSM travel time
    - Car travel time
    - Ratio of NSM total time to car time
    
    Args:
        dflist: List of DataFrames containing updated traveler data for each replication
    
    Returns:
        Nested dictionary with structure:
            {metric_name: {'mean': float, 'stdev': float}}
        For each service metric
    """
    # stores mean from each simulation
    data = {}
    data["nsm_wait_time"] = []
    data["nsm_travel_time"] = []
    data["car_time"] = []
    data["nsm_car_time_ratio"] = []

    # stores mean and stdev of means (should be weighted by # requests served?)
    result = {}
    result["nsm_wait_time"] = {}
    result["nsm_travel_time"] = {}
    result["car_time"] = {}
    result["nsm_car_time_ratio"] = {}

    for df in dflist:
        nsm_df = df.loc[df["served"] == 1, ["nsm_wait_time", "nsm_travel_time","car_time"]].dropna()
        nsm_wait_time = mean(nsm_df["nsm_wait_time"])
        data["nsm_wait_time"].append(nsm_wait_time)       
        nsm_travel_time = mean(nsm_df["nsm_travel_time"])
        data["nsm_travel_time"].append(nsm_travel_time)
        car_time = mean(nsm_df["car_time"])
        data["car_time"].append(car_time)
        data["nsm_car_time_ratio"].append((nsm_wait_time + nsm_travel_time) / car_time) #CORRECT
        #data["nsm_car_time_ratio"].append(car_time / (nsm_wait_time + nsm_travel_time)) #WRONG!


    for key in ["nsm_wait_time","nsm_travel_time","car_time","nsm_car_time_ratio"]:
        valuelist = data[key]
        result[key]['mean'] = mean(valuelist)
        if len(valuelist) > 1:
            result[key]["stdev"] = stdev(valuelist)
        else:
            print("there is no replication so stdev cannot be computed. setting to zero.")
            result[key]["stdev"] = 0

    return result                    





