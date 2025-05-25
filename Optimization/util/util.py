import pandas as pd
import csv
from typing import Dict, Union, Any
from .setup import result_path

def print_mode_stats(df: pd.DataFrame) -> None:
    """Print mode choice statistics from a DataFrame containing travel choices.
    
    Prints a summary of mode choices including counts and percentages for each mode.
    Used for debugging and monitoring mode choice distribution during simulation.
    
    Args:
        df: DataFrame containing a 'choice' column with mode choices where:
            0 = walk
            1 = bike
            2 = car/truck/ride/drive
            3 = PT (public transit)
            4 = NSM (new shared mobility)
    """
    freqs = df['choice'].value_counts().reset_index()
    freqs.columns = ['mode', 'count']
    total = sum(freqs["count"])
    freqs["total"] = total
    freqs["prc"] = (freqs["count"]/freqs["total"])*100
    print(freqs.sort_values(by=["mode"]))

def get_mode_stats(df: pd.DataFrame) -> Dict[str, Dict[int, float]]:
    """Calculate mode choice statistics from a DataFrame containing travel choices.
    
    Computes both absolute counts and ratios for each transportation mode choice.
    Used to track mode split changes across simulation iterations.
    
    Args:
        df: DataFrame containing a 'choice' column with mode choices where:
            0 = walk
            1 = bike
            2 = car/truck/ride/drive
            3 = PT (public transit)
            4 = NSM (new shared mobility)
    
    Returns:
        Dictionary containing:
            'ratio': Dict mapping mode number to proportion of total choices (0.0-1.0)
            'count': Dict mapping mode number to absolute count of choices
    """
    result = {
        "ratio":{},
        "count":{}
    }
    value_counts = df["choice"].value_counts()
    value_counts = value_counts.sort_index()
    total_rows = len(df)
    for value, count in value_counts.items():
        percentage = float(count / total_rows) * 100.0
        result['count'][value] = count
        result['ratio'][value] = float(count/total_rows)
    return result

def get_config_results(config_name: str) -> Dict[str, str]:
    """Read simulation results from a configuration's CSV results file.
    
    Loads the final metrics and parameters from a completed simulation run.
    Used to analyze results and prepare for next simulation iteration.
    
    Args:
        config_name: Name of the configuration whose results should be loaded.
            Used to construct filename as {config_name}_results.csv
    
    Returns:
        Dictionary mapping metric names to their values, including:
            - occupancy: Average vehicle occupancy
            - service_rate: Percentage of NSM requests served
            - nsm_wait_time: Average wait time for NSM service
            - nsm_car_time_ratio: Ratio of NSM to car travel time
            - threshold_triggered: Whether stop threshold was reached
            - last_iter: Final iteration number
            - num_iterations: Total iterations run
            - mode_{n}: Mode split percentage for mode n
    """
    result = {}
    with open(f"{result_path}\\{config_name}_results.csv", mode="r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            key = row[0]
            value = row[1]
            result[key] = value
    return result
