# WARNING: Only partially-optimized. Still under development!
#
# INPUT: CSV file where first two columns are origin TAZ (traffic analysis zone) and destination TAZ
#    and remaining columns are time periods (0800_1200, 1200_1400, etc) the integer values in these
#    columns are the number of travellers from the origin TAZ to the destination TAZ during that time.
#
# OUTPUT: CSV file with time, originx, originy, destx, and desty columns where the origin (x,y)
#    is within the origin TAZ and the destination (x,y) is within the destination TAZ, both points
#    are on the road network, and the number of points follow a Poisson distribution. 
#    Can separately add requests to a spatialite database with geoms, depending on the users' workflow.

from spatialite_demand_toolbox import create_sqlite_cursor, create_points_table, create_points_on_streets_in_OD, insert_point_in_table
import pandas as pd
import numpy as np
import random
import time

period = '8_10' # currently handles one period of demand at a time
scale_prc = 5  # scale demand by this percent
basedir = "D:/Documents/AUTOlab/Jerusalem/SUM-Optimization"
db_filepath = f"{basedir}/JerusalemData/gis/jerusalem.sqlite"
demand_filepath = f"{basedir}/JerusalemData/demand/matrix_15destinations.csv"
demand_df = pd.read_csv(demand_filepath)
ignore = ["4711"] # ignore these taz (outside target neighborhood or city, measurement error, etc)

requests = []
start_hour = int(period.split("_")[0])
end_hour = int(period.split("_")[1])
num_hours = end_hour - start_hour
np.random.seed()

# create new table for demand instance
tablename = f"demand_{scale_prc}prc"
conn, cursor = create_sqlite_cursor(db_filepath)
create_points_table(cursor,tablename)

request_id = 0
print(f"GENERATING REQUESTS FROM TIME {start_hour} TO {end_hour}")
for demand_row_id, row in demand_df.iterrows():
    origin_taz_num = f"{int(row['origin'])}".zfill(4)
    dest_taz_num = f"{int(row['destination'])}".zfill(4)
    if origin_taz_num not in ignore and dest_taz_num not in ignore:
        requests_in_period = row[period]
        scaled_requests_in_period = requests_in_period * scale_prc * .01
        print(f"row: {demand_row_id}, origin: {origin_taz_num}, destination: {dest_taz_num}, volume: {requests_in_period} scaled to {scaled_requests_in_period}")
        num_requests = np.random.poisson(scaled_requests_in_period)
        if num_requests > 0:
            ods = create_points_on_streets_in_OD(cursor, origin_taz_num, dest_taz_num, num_requests)
            for od in ods:
                origin_x, origin_y, origin_geom, dest_x, dest_y, dest_geom = od
                request_time = np.random.uniform(start_hour,end_hour) 
                # save origin and destination to sqlite db
                insert_point_in_table(cursor,tablename,request_id,demand_row_id,request_time,"origin",origin_taz_num, origin_x,origin_y,origin_geom)
                insert_point_in_table(cursor,tablename,request_id,demand_row_id,request_time,"dest",dest_taz_num, dest_x,dest_y,dest_geom)
                # for writing to CSV later
                requests.append([request_id,demand_row_id,request_time,origin_taz_num,dest_taz_num,origin_x,origin_y,dest_x,dest_y])
                request_id+=1
conn.commit()

# write to file
columns = ['request_id','demand_row_id','request_time', 'origin_taz', 'dest_taz', 'origin_x', 'origin_y', 'dest_x', 'dest_y']
df = pd.DataFrame(requests, columns=columns)
df.to_csv(f"{basedir}/demand/demand_{scale_prc}prc.csv", index=False) 
