# TO CREATE HIGH RESOLUTION DEMAND BASED ON O-D MATRIX OF TRAFFIC ANALYSIS ZONES (TAZs)
#
# WARNING: Under development!
#
# This SQLITE proof of concept is faster than pure python solutions
# in part because the data are already "loaded". Spatialite may also
# leverage spatial indexes better?
#
# Prereqs:  (1) spatialite is installed
#           (2) spatialite database with (a) polygon layer called "taz" (traffic analysis zones)
#                                        (b) line layer called "streets"
#
# NOTE: QGIS can export even large layers to spatialite databases
# quickly and handles Hebrew characters well. Spatialite was selected over
# PostgreSQL's PostGIS for the convenience of having all data in a single
# file.

import sqlite3
import random


def create_sqlite_cursor(db_path):
    conn = sqlite3.connect(db_path)
    conn.enable_load_extension(True)
    conn.load_extension('mod_spatialite')  # Adjust path as needed for your setup
    cursor = conn.cursor()
    return conn,cursor


def get_selected_taz(cursor,taz_num): 
    cursor.execute("""
        SELECT ST_AsText(geometry) FROM taz WHERE taz_num = ?
    """, (taz_num,))
    taz_geom = cursor.fetchone()[0]
    return taz_geom


def get_streets_in_taz(cursor,taz_geom):
    cursor.execute("""
    SELECT objectid, ST_AsText(geometry) FROM streets 
    WHERE ST_Intersects(geometry, ST_GeomFromText(?))
    OR ST_Within(geometry, ST_GeomFromText(?))
    """, (taz_geom, taz_geom))
    streets = cursor.fetchall()
    return streets


def create_point_on_street(cursor,street_geom,taz_geom):
    # Generate random point on street within taz using SpatiaLite function
    cursor.execute("""
        SELECT ST_AsText(
            ST_PointOnSurface(
                ST_Intersection(
                    ST_GeomFromText(?),
                    ST_GeomFromText(?)
                )
            )
        ) AS point
    """, (street_geom, taz_geom))
    point_geom = cursor.fetchone()[0]
    return point_geom


def drop_points_table(conn,cursor,table_name):
    query = f"DROP TABLE IF EXISTS {table_name};"
    cursor.execute(query)
    # Probably not needed:
    #cursor.execute("DELETE FROM geometry_columns WHERE f_table_name = 'taz_points';")
    #cursor.execute("SELECT DisableSpatialIndex('taz_points', 'geometry');")  # Disable the spatial index
    #cursor.execute("DROP TABLE IF EXISTS idx_taz_points_geometry;")  # Drop the index table if it was created\
    conn.commit()


def create_points_table(cursor,table_name):

    # Visualizing trips with traditional GIS tools is sometimes easier if origin and destination are
    # stored as separate rows (not a single row with two spatial geoms). "part" is either origin
    # or destination with a shared "id".

    query = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER, request_time FLOAT, part TEXT, x DOUBLE, y DOUBLE, UNIQUE(id, part));"
    cursor.execute(query)
    query = f"SELECT AddGeometryColumn({table_name}, 'geometry', 2039, 'POINT', 'XY');"
    try:
        cursor.execute(query)
    except sqlite3.OperationalError as e:
        print(f"AddGeometryColumn() skipped: {e}")
    try:
        query = f"SELECT CreateSpatialIndex({table_name}, 'geometry');"
        cursor.execute(query)
    except sqlite3.OperationalError as e:
        print(f"CreateSpatialIndex() skipped: {e}")

def create_point_xy_taz(cursor, taz_num):
    taz_geom = get_selected_taz(cursor,taz_num)
    streets = get_streets_in_taz(cursor,taz_geom)    
    points = []
    # Randomly select street
    street_id, street_geom = random.choice(streets)
    point_geom = create_point_on_street(cursor,street_geom,taz_geom)
    if point_geom:
        # Extract X and Y from the point geometry
        cursor.execute("""
            SELECT ST_X(ST_GeomFromText(?,2039)), ST_Y(ST_GeomFromText(?,2039))
        """, (point_geom, point_geom))
        x, y = cursor.fetchone()
        return x, y, point_geom
    else:
        raise Exception(f"Failed to create point in TAZ {taz_num} on street {street_geom}")

def insert_point_in_table(request_time,part,x,y,geom):
    pass

    
    # # Insert the point along with X and Y
    #     cursor.execute("""
    #         INSERT INTO taz_points (X, Y, geometry) VALUES (?, ?, ST_GeomFromText(?,2039)) 
    #     """, (x, y, point_geom)) # if SRID e.g. 2039 is not specified here it defaults to zero and causes problems




def create_point_within_taz(db_path, taz_num, num_points=1):
    conn, cursor = create_sqlite_cursor(db_path)
    taz_geom = get_selected_taz(cursor,taz_num)
    streets = get_streets_in_taz(cursor,taz_geom)    
    points = []
    while len(points) < num_points:
        # Randomly select street
        street_id, street_geom = random.choice(streets)
        point_geom = create_point_on_street(cursor,street_geom,taz_geom)
        if point_geom:
            points.append(point_geom)
    drop_points_table(conn,cursor,"taz_points")
    create_points_table(cursor,"taz_points")

    # Insert the generated points into the new table
    for point_geom in points:
    # Extract X and Y from the point geometry
        cursor.execute("""
            SELECT ST_X(ST_GeomFromText(?,2039)), ST_Y(ST_GeomFromText(?,2039))
        """, (point_geom, point_geom))
        x, y = cursor.fetchone()
    
    # Insert the point along with X and Y
        cursor.execute("""
            INSERT INTO taz_points (X, Y, geometry) VALUES (?, ?, ST_GeomFromText(?,2039)) 
        """, (x, y, point_geom)) # if SRID e.g. 2039 is not specified here it defaults to zero and causes problems

    conn.commit()
    conn.close()

# MAIN
db_path = 'D:/Documents/AUTOlab/Jerusalem/GIS/jerusalem.sqlite'
create_points_within_taz(db_path, 4604, 100)
