import os

# Base paths
BASE_PATH = "D:\\users\\davideps\\Jerusalem"
PROJECT_ROOT = os.path.join(BASE_PATH, "SUM-Optimization-T2.2_Deliverable")

# Workspace and results paths
WORKSPACE_PATH = os.path.join(PROJECT_ROOT, "temp_workspace")
RESULT_PATH = os.path.join(PROJECT_ROOT, "framework_results")

# FleetPy related paths
FLEETPY_PATH = os.path.join(PROJECT_ROOT, "FleetPy_2024")
FLEETPY_DEMAND_PATH = os.path.join(FLEETPY_PATH, "data", "demand", "jerusalem_demand", "matched", "jerusalem_osm")
FLEETPY_SCENARIOS_PATH = os.path.join(FLEETPY_PATH, "studies", "jerusalem", "scenarios")
FLEETPY_RESULTS_PATH = os.path.join(FLEETPY_PATH, "studies", "jerusalem", "results")

# Data paths
REQUESTS_FILE_PATH = os.path.join(PROJECT_ROOT, "JerusalemData", "demand", "Processed", 
                                 "requests_fleetpy_nodes_with_nsm_7am9am_scale15x_focus_area.csv")

# Ensure all directories exist
for path in [WORKSPACE_PATH, RESULT_PATH, FLEETPY_DEMAND_PATH, FLEETPY_SCENARIOS_PATH, FLEETPY_RESULTS_PATH]:
    os.makedirs(path, exist_ok=True) 