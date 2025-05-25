import numpy as np
from typing import Dict, List, Union, Any
import pandas as pd


def check_nulls(df: pd.DataFrame, loc: str) -> None:
    """Check for null values in DataFrame and print diagnostic information.
    
    Args:
        df: DataFrame to check for null values
        loc: Location identifier for error reporting
    
    Raises:
        SystemExit: If any null values are found in the DataFrame
    """
    if df.isnull().any().any():
        null_rows = df[df.isnull().any(axis=1)]
        print(f"There are {len(null_rows)} rows with null values in the DataFrame by location {loc}")
        print(null_rows.head())
        quit()
    else:
        print(f"No null values in the DataFrame by location {loc}")


def calc_all_availabilities(df: pd.DataFrame, avails: Dict[str, Union[str, float, int]]) -> pd.DataFrame:
    """Calculate mode availability flags for each traveler.
    
    Sets availability flags for each transportation mode based on different rules:
    a) Fixed availability (0 or 1) for all travelers
    b) Probabilistic availability (0 < value < 1) for random assignment
    c) Column-based availability (string value references column name)
    
    Args:
        df: DataFrame containing traveler data
        avails: Dictionary mapping mode names to availability rules where:
            - 0 or 1: Fixed availability for all travelers
            - Float between 0-1: Probability of mode being available
            - String: Name of column containing availability flags
    
    Returns:
        DataFrame with added availability columns (mode_av) for each mode
    
    Raises:
        ValueError: If availability rule is invalid
    """
    for mode in avails.keys():
        avkey = f"{mode}_av"
        if isinstance(avails[mode], str):
            df[avkey] = df[avails[mode]]
        elif avails[mode] == 0 or avails[mode] == 1:
            df[avkey] = avails[mode]
        elif avails[mode] > 0.0 and avails[mode] < 1.0:
            df[avkey] = (np.random.rand(len(df)) < avails[mode]).astype(int)
        else:
            raise ValueError(f"cannot calculate availility for {mode} value: {avails[mode]}")
    return df


def calc_all_utilities(df: pd.DataFrame, m: Dict[str, float], nsm: Dict[str, float]) -> pd.DataFrame: #m = model nsm = nsm estimates
    """Calculate utility values for each transportation mode.
    
    Computes utility scores based on mode-specific attributes and model parameters.
    
    Args:
        df: DataFrame containing traveler and trip attributes
        m: Dictionary of model coefficients (ASCs and Betas)
        nsm: Dictionary of NSM (new shared mobility) service metrics
    
    Returns:
        DataFrame with added utility columns (u_mode) for each mode
    """
    NSM_RISK  = 1.0 - nsm["service_rate"]
    df["u_walk"] = m["ASC_WALK"] + m["B_TIME"] * df["walk_time"] 
    df["u_bike"] = m["ASC_BIKE"] + m["B_TIME"] * df["bike_electric_time"]
    df["u_car"] = m["ASC_CAR"] + m["B_TIME"] * df["car_time"] + m["B_COST"] * df["car_cost"] 
    df["u_pt"] = m["ASC_PT"] + m["B_TIME"] * df["pt_time"] + m["B_COST"] * df["pt_cost"]
    df["u_nsm"] = m["ASC_NSM"] + m["B_TIME"] * df["nsm_total_time"] + m["B_COST"] * df["nsm_cost"] + m["B_RISK"] * NSM_RISK
    #print("utils")
    #print(df)
    return df

def calc_all_exps(df: pd.DataFrame, modes: List[str]) -> pd.DataFrame:
    """Calculate exponential utility values and total for mode choice model.
    
    Args:
        df: DataFrame containing utility values for each mode
        modes: List of mode names (walk, bike, car, pt, nsm)
    
    Returns:
        DataFrame with added exponential columns (exp_mode) and total (exp_total)
    """
    total = 0
    for mode in modes:
        expkey = f"exp_{mode}"
        df[expkey] = np.exp(df[f"u_{mode}"]) * df[f"{mode}_av"]
        total += df[expkey]
    df["exp_total"] = total
    #print("exps")
    #print(df)
    return df


def recalc_nsm_util_and_exp(df: pd.DataFrame, m: Dict[str, float], nsm: Dict[str, float]) -> pd.DataFrame:
    """Recalculate NSM utilities and exponentials after service updates.
    
    Updates NSM-specific utility values and exponentials after service metrics change,
    maintaining other modes' values.
    
    Args:
        df: DataFrame containing current utilities and exponentials
        m: Dictionary of model coefficients
        nsm: Dictionary of updated NSM service metrics
    
    Returns:
        DataFrame with updated NSM utility and exponential values
    """
    df["u_nsm"] = m["ASC_NSM"] + m["B_TIME"] * df["nsm_total_time"] + m["B_COST"] * df["nsm_cost"] + m["B_RISK"] * (1.0-nsm["service_rate"]) # risk = 1 - service_rate
    df["exp_nsm"] = np.exp(df[f"u_nsm"]) * df[f"nsm_av"]
    df["exp_total"] = df["exp_walk"] + df["exp_bike"] + df["exp_car"] + df["exp_pt"] + df["exp_nsm"]
    return df


def calc_all_probs(df: pd.DataFrame, modes: List[str]) -> pd.DataFrame:
    """Calculate choice probabilities for each mode.
    
    Computes multinomial logit probabilities for each mode based on exponential utilities.
    
    Args:
        df: DataFrame containing exponential utilities
        modes: List of mode names
    
    Returns:
        DataFrame with added probability columns (prc_mode) for each mode
    """
    for mode in modes:
        prckey = f"prc_{mode}"
        df[prckey] = df[f"exp_{mode}"] / df["exp_total"]
    return df


def select_choice(row: pd.Series) -> int:
    """Select transportation mode based on calculated probabilities.
    
    Makes random choice based on mode probabilities for a single traveler.
    
    Args:
        row: Series containing mode probabilities (prc_mode columns)
    
    Returns:
        Integer representing chosen mode:
            0: walk
            1: bike
            2: car
            3: public transit
            4: NSM
    """
    probs = [row["prc_walk"], row["prc_bike"], row["prc_car"], row["prc_pt"],row["prc_nsm"]]
    return np.random.choice([0, 1, 2, 3, 4], p=probs)




# Function to scale probabilities and make a random choice
def reasign_unserved_requests(row: pd.Series) -> int:
    """Reassign mode choice for unserved NSM requests.
    
    Rescales probabilities of non-NSM modes and makes new choice for travelers
    who requested NSM but were not served.
    
    Args:
        row: Series containing mode probabilities and previous choice
    
    Returns:
        Integer representing new mode choice (0-3, excluding NSM)
    """
    # print("-----------------------------------------------------------------")
    # print("reasigning_unserved_requests")
    # Calculate remaining probability total after excluding prc_nsm
    remaining_prob_total = row['prc_walk'] + row['prc_bike'] + row['prc_car'] + row['prc_pt']
    # Scale probabilities so they sum to 1
    scaled_prc_walk = row['prc_walk'] / remaining_prob_total
    scaled_prc_bike = row['prc_bike'] / remaining_prob_total
    scaled_prc_car = row['prc_car'] / remaining_prob_total
    scaled_prc_pt = row['prc_pt'] / remaining_prob_total
    # Create scaled probability list
    scaled_probs = [scaled_prc_walk, scaled_prc_bike, scaled_prc_car, scaled_prc_pt]
    # Make random choice based on scaled probabilities
    # print(f"previous choice: {int(row["choice"])}")
    choice = np.random.choice([0, 1, 2, 3], p=scaled_probs)
    # print(f"current choice: {choice}")
    return choice


