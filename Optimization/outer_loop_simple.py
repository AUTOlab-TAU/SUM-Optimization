# NSM CONFIGURATIONS ARE DEFINED IN A DICTIONARY
# HERE ARE THE ACCEPTABLE KEY/VALUE PAIRS:

# THESE ARE PASSED DIRECTLY TO FLEETPY's SCENARIO CONFIG FILE
# "op_max_wait_time" : max time operator can wait to pool trips before offering request a yes/no
# "op_max_detour_time_factor" : max detour compared to car. 100 = twice the time of a car, 50 = 1.5 times a car
# "op_fleet_composition" :  a string in the format "jerusalem_petrol_van_vehtype:20"
#                           the first part must match a CSV filename in fleetpy-root-dir\data\vehicles (without the csv extension)
#                           the second part is an integer reflecting the number of such vehicles in the fleet

# THESE ARE USED IN OTHER ASPECTS OF THE INNNER LOOP
# "config_name" : configuration results will be saved as [config_name]_results.csv
# "nsmfee" : cost of NSM ticket, used for utility computation (DCA)
# "maxiter" : max number of times to iterate before giving results if the stop_loop_threshold never triggers
# "reps" : number of replications, the number of times the same configuration is run and the resuts averaged
# "stop_loop_threshold" :   currently this represents the percentage point difference of the NSM modesplit
#                           if the threshold is 0.5 (half a percent) and NSM usage drops from 1.50% to 1.41%, 
#                           since the delta is 0.09%, the inner loop will stop and the threshold_triggered flag
#                           will be set to true in the configuration results CSV.

import sys
import json
import subprocess
from pathlib import Path

# Add project root to Python path to find config
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import OPTIMIZATION_DIR, RESULTS_PATH
from util.util import get_config_results

# inner_loop.py must be in same directory as this script
INNER_LOOP_PATH = OPTIMIZATION_DIR / "inner_loop.py"

nsmconfig = {
    "config_name":"PLACEHOLDER", # used to name files
    "op_max_wait_time":60,     # 300 = 5min lead time , 420 = 7min
    "op_max_detour_time_factor":100, #100
    "op_fleet_composition":"jerusalem_petrol_van_vehtype:20",
    "nsm_cost":8,# if this is less than or equal to PT cost of 6, NSM requests will soar, requiring higher maxiter and far more time per iteration
    "maxiter":25, # if stop_loop_threshold is not reached, how many iterations to run? depends on reasonableness of initial settings in util\setup.py
    "reps":10, # how many repetitions per configuration? start low. probably good to keep below #CPUs
    "stop_loop_threshold":0.020, #0.015, #absolute difference in percentage points in NSM usage between iterations
    } 

for fleet_size in [5]:
    for nsm_cost in [2]:

        # modify the basic configuration
        config_name = f"{fleet_size}_{nsm_cost}"
        nsmconfig["config_name"] = config_name
        nsmconfig["op_fleet_composition"] = f"jerusalem_petrol_van_vehtype:{fleet_size}"
        nsmconfig["nsm_cost"] = nsm_cost
        nsmconfig_text = json.dumps(nsmconfig)
        
        # run the inner loop
        print(f"Outer loop calling inner loop for configuration: {config_name}")
        command = [sys.executable, str(INNER_LOOP_PATH), '--config', nsmconfig_text]
        this_process = subprocess.run(command, capture_output=False, text=True) # write inner_loop text to screen
        #this_process = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) # ignore inner_loop text

        # get results
        print("Outer loop getting configuration results...")
        config_results = get_config_results(config_name) # dictionary
        print(config_results)
        print("----------------------------------------------------")




