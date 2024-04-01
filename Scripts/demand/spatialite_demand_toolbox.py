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


def create_points_table(cursor,tablename): 
    # Visualizing trips with traditional GIS tools is sometimes easier if origin and destination are
    # stored as separate rows (not a single row with two spatial geoms). "part" is either origin
    # or destination with a shared "id".
    query = f"DROP TABLE IF EXISTS {tablename};"
    cursor.execute(query)
    query = f"CREATE TABLE IF NOT EXISTS {tablename} (id INTEGER, demand_row, request_time FLOAT, part TEXT, taz, x DOUBLE, y DOUBLE, UNIQUE(id, part));"
    cursor.execute(query)
    query = f"SELECT AddGeometryColumn('{tablename}', 'geometry', 2039, 'POINT', 'XY');"
    try:
        cursor.execute(query)
    except sqlite3.OperationalError as e:
        print(f"AddGeometryColumn() skipped: {e}")
    try:
        query = f"SELECT CreateSpatialIndex('{tablename}', 'geometry');"
        cursor.execute(query)
    except sqlite3.OperationalError as e:
        print(f"CreateSpatialIndex() skipped: {e}")


def create_points_on_streets_in_OD(cursor, origin_taz_num, dest_taz_num, num_requests):
    result = []
    orig_taz_geom = get_selected_taz(cursor,origin_taz_num)
    dest_taz_geom = get_selected_taz(cursor,dest_taz_num)
    origin_street_geoms = get_streets_in_taz(cursor,orig_taz_geom)
    dest_street_geoms = get_streets_in_taz(cursor,dest_taz_geom)
    for _ in range(num_requests):
        origin_x, origin_y, origin_geom = create_point_on_street_in_taz(cursor, origin_taz_num, orig_taz_geom, origin_street_geoms)
        dest_x, dest_y, dest_geom = create_point_on_street_in_taz(cursor, dest_taz_num, dest_taz_geom, dest_street_geoms)
        result.append([origin_x, origin_y, origin_geom, dest_x, dest_y, dest_geom])
    return result


def create_point_on_street_in_taz(cursor, taz_num, taz_geom, street_geoms):
    # Randomly select street
    street_id, street_geom = random.choice(street_geoms)
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

def insert_point_in_table(cursor,tablename,id,demand_row,request_time,part,taz, x,y,point_geom): # potential security issue since tablename is parameter
    print(f"{id} {request_time} {part} {x} {y} {point_geom}")
    query = f"""INSERT INTO {tablename} (id, demand_row, request_time, part, taz, X, Y, geometry) VALUES (?, ?, ?, ?, ?, ?, ?, ST_GeomFromText(?,2039))""" # if SRID e.g. 2039 is not specified here it defaults to zero and causes problems
    cursor.execute(query,(id,demand_row,request_time,part,taz,x,y,point_geom))
