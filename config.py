from pathlib import Path

# Project structure - all paths relative to project root
PROJECT_ROOT = Path(__file__).parent
SCRIPTS_DIR = PROJECT_ROOT / "Scripts"
OPTIMIZATION_DIR = PROJECT_ROOT / "Optimization"

# External dependencies and data paths
FLEETPY_ROOT = PROJECT_ROOT / "FleetPy_2024"
LL_DATA = PROJECT_ROOT / "JerusalemData"

# FleetPy specific paths
FLEETPY_SCENARIOS = FLEETPY_ROOT / "studies" / "jerusalem" / "scenarios"
FLEETPY_DEMAND = FLEETPY_ROOT / "data" / "demand" / "jerusalem_demand" / "matched" / "jerusalem_osm"
FLEETPY_RESULTS = FLEETPY_ROOT / "studies" / "jerusalem" / "results"
FLEETPY_VEHICLES = FLEETPY_ROOT / "data" / "vehicles"

# Data paths
DEMAND_PATH = LL_DATA / "demand" / "Processed"
REQUESTS_FILE = DEMAND_PATH / "requests_fleetpy_nodes_with_nsm_7am9am_scale15x_focus_area.csv"

# Working and results paths
WORK_PATH = PROJECT_ROOT / "temp_workspace"
RESULTS_PATH = PROJECT_ROOT / "framework_results"

def validate_paths():
    """Validate that critical paths exist."""
    required_paths = [
        PROJECT_ROOT,
        SCRIPTS_DIR,
        OPTIMIZATION_DIR,
        FLEETPY_ROOT,
        LL_DATA,
        DEMAND_PATH,
        FLEETPY_SCENARIOS,
        FLEETPY_DEMAND,
        FLEETPY_RESULTS
    ]
    
    missing_paths = []
    for path in required_paths:
        if not path.exists():
            missing_paths.append(str(path))
    
    if missing_paths:
        raise FileNotFoundError(
            "The following required paths do not exist:\n" + 
            "\n".join(missing_paths)
        )

def ensure_work_paths():
    """Create working directories if they don't exist."""
    paths_to_create = [
        RESULTS_PATH,
        WORK_PATH
    ]
    
    for path in paths_to_create:
        path.mkdir(parents=True, exist_ok=True)

def get_iteration_path(iteration: int, replication: int, prefix: str = "") -> Path:
    """Generate standardized paths for iteration files.
    
    Args:
        iteration: Iteration number
        replication: Replication number
        prefix: Optional prefix for the filename
    
    Returns:
        Path object with standardized naming
    """
    return WORK_PATH / f"{prefix}i{iteration:03}_r{replication:03}"

# Validate paths on module import
validate_paths()
ensure_work_paths()