import pandas as pd
from typing import Dict, List, Union, Any

class StatGroup:
    """Maintains running statistics with exponential smoothing for simulation parameters.
    
    Used to track and smooth various metrics throughout the simulation iterations,
    including mode choice model parameters and NSM (new shared mobility) performance metrics.
    Supports both raw values and smoothed values with configurable weighting.
    
    Attributes:
        weight (float): Smoothing weight between 0 and 1. Higher values give more weight to new values.
        data (Dict): Nested dictionary storing statistics:
            'value': Raw values for each statistic
            'smooth': Exponentially smoothed values
            'pval': P-values for statistical significance
    """
    
    def __init__(self, initstats: Dict[str, float], weight: float) -> None:
        """Initialize statistics group with initial values and smoothing weight.
        
        Args:
            initstats: Dictionary of initial values for each statistic to track.
                Special key 'pval' is reserved for p-values.
            weight: Smoothing weight between 0 and 1 for exponential smoothing.
                weight_alpha < 0.5 favors previous values over new values.
        """
        self.weight = weight
        self.data = {}
        for key in ['value','smooth','pval']:
            self.data[key] = {}
            for stat in initstats.keys():
                if stat != 'pval':
                    self.data[key][stat] = [initstats[stat]]
                else:
                    self.data[key][stat] = 1.0

    def add(self, stat: str, value: float, pval: float = 1) -> None:
        """Add a new value for a statistic and compute its smoothed value.
        
        Updates both raw and smoothed values for the given statistic using
        exponential smoothing: new_smooth = prev_smooth * (1-weight) + value * weight
        
        Args:
            stat: Name of the statistic to update
            value: New raw value for the statistic
            pval: P-value indicating statistical significance (default=1)
        """
        self.data['value'][stat].append(value)
        self.data['pval'][stat].append(pval)
        # insert smoothed value
        prev_smooth = self.data['smooth'][stat][-1]
        next_smooth = (prev_smooth * (1.0 - self.weight)) + (value * self.weight)
        self.data['smooth'][stat].append(next_smooth)
        print(f"\t{stat} prev:{prev_smooth}   result:{value}   smoothed:{next_smooth}")

    def get_smooth(self, stat: str) -> float:
        """Get the most recent smoothed value for a statistic.
        
        Args:
            stat: Name of the statistic to retrieve
            
        Returns:
            Most recent smoothed value for the statistic
        """
        return self.data['smooth'][stat][-1]
    
    def get_all_current_smooth(self) -> Dict[str, float]:
        """Get the most recent smoothed values for all statistics.
        
        Returns:
            Dictionary mapping statistic names to their current smoothed values
        """
        result = {}
        for stat in self.data["smooth"].keys():
            result[stat] = self.data["smooth"][stat][-1]
        return result
    
    def to_ggplot(self) -> pd.DataFrame:
        """Convert statistics history to a DataFrame suitable for plotting.
        
        Returns:
            DataFrame with columns:
                iter: Iteration number
                stat: Statistic name
                value: Raw value
                smoothed: Smoothed value
                pval: P-value
        """
        temp = []
        for stat in self.data['smooth']:
            for i in range(len(self.data['smooth'][stat])):
                pval = self.data['pval'][stat][i]
                value = self.data['value'][stat][i]
                smoothed = self.data['smooth'][stat][i]
                temp.append({'iter': i, 'stat': stat, 'value': value, 'smoothed': smoothed, 'pval': pval})
        return pd.DataFrame(temp)

# Paths for data and configuration files
basepath: str = "D:\\users\\davideps\\Jerusalem"
workpath: str = f"{basepath}\\python\\temp_workspace"
result_path: str = f"{basepath}\\python\\config_results"
fleetpy_path: str = f"{basepath}\\python\\FleetPy"
fleetpy_demand_path: str = f"{fleetpy_path}\\data\\demand\\jerusalem_demand\\matched\\jerusalem_osm"
requestsfilepath: str = f"{basepath}\\SUM-Optimization\JerusalemData\demand\Processed\\requests_fleetpy_nodes_with_nsm_7am9am_scale15x_focus_area.csv"
requestfile_ratio: float = 15.10924  # 2.0=twice the Deccell volume

# Simulation parameters
demand_ratio: float = 1.252  # 1.0 = original Decell volume
weight_alpha: float = 0.25  # Smoothing weight for statistics

# Cost parameters
car_fuel_consumption_liters_per_km: float = 1.0/10.0
fuel_cost_shekels_per_liter: float = 7.78
pt_cost: float = 6.0

# Available transportation modes
modes: List[str] = ["walk","bike","car","pt","nsm"]

# Mode availability rates
avails: Dict[str, float] = {
    "walk": 1.0,
    "bike": 1.0,
    "car": 0.67,  # car ownership rate from survey
    "pt": 1.0,
    "nsm": 1.0
}

# Initial mode choice model parameters
initmodel: Dict[str, float] = {
    "ASC_WALK": 4.1,
    "ASC_BIKE": -0.5,
    "ASC_CAR":  4.5,
    "ASC_PT":   5.5,
    "ASC_NSM":  5.0,
    "B_COST":   -0.35,
    "B_TIME":   -0.0020,
    "B_RISK":   0.0
}

# Initialize statistics trackers
choicestats: StatGroup = StatGroup(initmodel, weight_alpha)

# Initial NSM (new shared mobility) performance metrics
nsmstats: Dict[str, float] = {
    "occupancy": 1.0,  # assume taxi service
    "service_rate": 0.2,  # assume very few served initially
    "nsm_car_time_ratio": 1.5,  # travel time ratio compared to car
    "nsm_wait_time": 150,  # initial wait time estimate
    "nsm_travel_time": 375,  # initial travel time estimate
    "car_time": 250 # initial car time estimate
}
nsmstats: StatGroup = StatGroup(nsmstats, weight_alpha)

# Mode split tracking statistics
modestats: Dict[str, Dict[int, List[Union[int, float]]]] = {
    "ratio": {
        0: [], 1: [], 2: [], 3: [], 4: []
    },
    "count": {
        0: [], 1: [], 2: [], 3: [], 4: []
    }
}
