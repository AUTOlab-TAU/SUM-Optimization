# THIS WILL
# 1) load CSV file where first two columns are origin TAZ and destination TAZ and remaining columns are time periods (0800_1200, 1200_1400, etc) the integer values in these columns are the number of travellers from the origin TAZ to the destination TAZ during that time.
# 2) generates CSV file with time, originx, originy, destx, and desty columns where the origin (x,y) is within the origin TAZ and the destination (x,y) is within the destination TAZ and both points are on the road network.

import pandas as pd
import numpy as np
import random
import time

period = '8_10'
scale_prc = 10
basedir = "D:/Documents/AUTOlab/Jerusalem"
street_filepath = f"{basedir}/GIS/SW_Data.gpkg"
street_layername = "FullWalkNetwork_centerlines_jerusalem"
taz_filepath = f"{basedir}/GIS/SW_Data.gpkg"
taz_layername = "TAZ_with_demographics"
demand_filepath = f"{basedir}/demand/matrix_15destinations.csv"
demand_df = pd.read_csv(demand_filepath)

arrivals = []
start_hour = int(period.split("_")[0])
end_hour = int(period.split("_")[1])
num_hours = end_hour - start_hour

np.random.seed()
arrivals = [] # KEY = time, which can be sorted to add sequentual passenger numbers later
print(f"GENERATING ARRIVALS FROM TIME {start_hour} TO {end_hour}")
for index, row in demand_df.iterrows():
    origin = row['origin']
    destination = row['destination']
    requests_in_period = row[period]
    scaled_requests_in_period = requests_in_period * scale_prc * .01
    scaled_requests_per_hour = float(scaled_requests_in_period/num_hours) # might be removed in favor of poisson over whole period
    print(f"row: {index}, origin: {origin}, destination: {destination}, volume: {requests_in_period} scaled to {scaled_requests_in_period}")
    n = 1
    for hour in range(start_hour,end_hour):
        poisson = np.random.poisson(scaled_requests_per_hour)
        for _ in range(poisson):
            request_time = np.random.uniform(hour,hour+1) 
            print(f"\t{n}: {request_time}")
            arrivals.append([request_time,origin,destination])
            n+=1