# CONVERTS A GEOPACKAGE LINE LAYER INTO A GRAPHML FILE
# FOR USE WITH ExMAS

import geopandas as gpd
import geonetworkx as gnx
import networkx as nx
from shapely.wkt import dumps
from shapely.geometry import Point, LineString, MultiLineString
import fiona

# Define your input geopackage file and multipartline (network) layer to convert
basedir = "D:\\Documents\\AUTOlab\\Jerusalem\\GIS"
gpk_file_path = f"{basedir}\\SW_Streets_Cleaned.gpkg"
layer_name = "streets_mapa_sw"


# returns all attributes (not methods) of an object
def get_attributes(obj):
    attributes = [attr for attr in dir(obj) if not callable(getattr(obj, attr)) and not attr.startswith("__")]
    return attributes

def process_geometry(idx, geom, nodes, edges):
    if isinstance(geom, LineString):
        segments = [geom]  # Wrap the LineString in a list for consistent processing
    elif isinstance(geom, MultiLineString):
        segments = list(geom.geoms)  # Access the LineStrings within the MultiLineString
    else:
        # If the geometry is neither a LineString nor a MultiLineString, report an error.
        raise ValueError(f"Unexpected geometry type at index {idx}: {type(geom)}")
    for part_num, old_line in enumerate(segments):
        global node_id_counter
        global edge_id_counter
        new_line = [] # list of unique edge ids
        for old_coords in old_line.coords:
            found = False
            old_point = Point(old_coords)
            for id in nodes:
                node_point = Point(nodes[id].x,nodes[id].y)
                if old_point.distance(node_point) <= threshold:
                    print(f"{old_coords} found in database as node {id}")
                    new_line.append(id)
                    found = True
            if not found:
                print(f"{old_coords} not in node database. assigned to new node {node_id_counter}")
                new_line.append(node_id_counter)
                nodes[node_id_counter] = old_point
                node_id_counter+=1
        print(f"New Line Nodes: {new_line}")
        for line_part_id in range(len(new_line)-1):
            node1 = new_line[line_part_id]
            node2 = new_line[line_part_id+1]
            edges[edge_id_counter]=[node1,node2]
            edge_id_counter+=1
    return nodes, edges


def make_geograph(nodes,edges):
    g = gnx.GeoGraph()
    for id in nodes:
        wkt = dumps(nodes[id])
        # Add nodes with WKT geometry and x, y attributes
        g.add_node(id, geometry=str(wkt), x=str(nodes[id].x), y=str(nodes[id].y))
    for id in edges:
        node1_id = edges[id][0]
        node1 = nodes[node1_id]
        node2_id = edges[id][1]
        node2 = nodes[node2_id]
        distance = node1.distance(node2)
        g.add_edge(node1_id,node2_id,length=str(distance))
    return g

# Read your data
threshold = 1.0 # in meters for matching/merging points
multipart_edges_gdf = gpd.read_file(gpk_file_path, layer=layer_name, engine="pyogrio")
nodes = dict() # key=counter val = [x,y]
edges = dict() # key=counter val = [node1,node2]
attribs = dict() #key=idx from main loop, val=attribute dictionary
node_id_counter = 0
edge_id_counter = 0
for idx, row in multipart_edges_gdf.iterrows():
    #IGNORE ATTRIBUTES FOR NOW. COMPUTE NEW LENGTH (DISTANCE)
    #attributes = row.drop('geometry').to_dict()
    # process the row, returning all nodes and edges, includes new ones
    print(f"Feature: {idx}")
    print(row['geometry'])
    nodes, edges = process_geometry(idx, row['geometry'], nodes, edges)
    print("------------------------------------------------------")
g = make_geograph(nodes,edges)
crs = multipart_edges_gdf.crs 
g.graph['crs'] = crs.to_string()

try:
    nx.write_graphml(g, f"{basedir}\\Jerusalem_SW_streets_cleaned.graphml")
    print("Graph successfully written to GraphML.")
except Exception as e:
    print(f"An error occurred while writing the graph: {e}")
