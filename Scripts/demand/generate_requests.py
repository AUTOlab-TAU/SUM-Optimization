# WARNING: Under development!
# 1) load CSV file where first two columns are origin TAZ (traffic analysis zone) and destination TAZ and remaining columns are time periods (0800_1200, 1200_1400, etc) the integer values in these columns are the number of travellers
#    from the origin TAZ to the destination TAZ during that time.
# 2) generates CSV file with time, originx, originy, destx, and desty columns where the origin (x,y) is within the origin TAZ and the destination (x,y) is within the destination TAZ and both points are on the road network.
#    Can separately add requests to a spatialite database with geoms, depending on the users' workflow.

from spatialite_demand_toolbox import create_sqlite_cursor, create_point_xy_taz
import pandas as pd
import numpy as np
import random
import time

period = '8_10' # currently handles one period of the data at a time
scale_prc = 10
basedir = "D:/Documents/AUTOlab/Jerusalem/SUM-Optimization"
db_filepath = f"{basedir}/JerusalemData/gis/jerusalem.sqlite"
demand_filepath = f"{basedir}/JerusalemData/demand/matrix_15destinations.csv"
demand_df = pd.read_csv(demand_filepath)

requests = []
start_hour = int(period.split("_")[0])
end_hour = int(period.split("_")[1])
num_hours = end_hour - start_hour
np.random.seed()
conn, cursor = create_sqlite_cursor(db_filepath)

print(f"GENERATING REQUESTS FROM TIME {start_hour} TO {end_hour}")
for index, row in demand_df.iterrows():
    n = 1
    origin_taz_num = int(row['origin'])
    dest_taz_num = int(row['destination'])
    requests_in_period = row[period]
    scaled_requests_in_period = requests_in_period * scale_prc * .01
    print(f"row: {index}, origin: {origin_taz_num}, destination: {dest_taz_num}, volume: {requests_in_period} scaled to {scaled_requests_in_period}")
    poisson = np.random.poisson(scaled_requests_in_period)
    for _ in range(poisson):
        request_time = np.random.uniform(start_hour,end_hour) 
        origin_x, origin_y, origin_geom = create_point_xy_taz(cursor,origin_taz_num)
        dest_x, dest_y, dest_geom = create_point_xy_taz(cursor,dest_taz_num)
        # save origin and destination as 


        # for writing to CSV later
        requests.append([request_time,origin_taz_num,dest_taz_num,origin_x,origin_y,dest_x,dest_y])
        n+=1

# write to file

columns = ['request_time', 'origin_taz', 'dest_taz', 'origin_x', 'origin_y', 'dest_x', 'dest_y']

# Create a DataFrame and save as CSV
df = pd.DataFrame(requests, columns=columns)
df.to_csv(f"{basedir}/demand/demand_{scale_prc}prc.csv", index=False) 







