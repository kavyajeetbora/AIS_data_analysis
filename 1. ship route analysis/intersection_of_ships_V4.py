# -*- coding: utf-8 -*-
"""
Created on Thu July 1st 11:44:54 2021

@author: KVBA
"""
import pandas as pd
# import dask.dataframe as dd
import pickle
from shapely.geometry import LineString, Point
import geopandas as gpd
from tqdm import tqdm
import time
from glob import glob
import warnings
import os
warnings.filterwarnings("ignore")
import pickle


## Convert the AIS data to geodataframe
def convert_ais_data_to_gdf(df):
    
    '''
    Convert the AIS data to geodataframe - 
    
    PARAMETER: pandas dataframe, df
    OUTPUT: returns a geodataframe with shapely.geometry.Point 
    representing the coordinates of ais data.
    '''
    
    geometry = [Point(x,y) for (x,y) in df[["Longitude", "Latitude"]].values]
    # geometry = list(map(Point, df["Longitude"], df["Latitude"]))
    df['geometry'] = geometry
    geo_df = gpd.GeoDataFrame(df.copy(), geometry=geometry, crs="epsg:4326")
    geo_df.drop(["Latitude", "Longitude"], axis=1, inplace=True)
    return geo_df


## function to find the intersection point only from a given geodataframe and cs
def ais_data_to_path(geo_df):
    
    '''
    Converts the ais coordinate data of each ship to a Line representing its journey/path.
    
    The Line is a shapely.geoemetry.LineString. 
    
    returns a geopandas.GeoDataFrame consisting of ship ID and its path in the form of LineString
    '''
    
    
    ## Filtering out the ships with only one signal
    values = geo_df.MMSI.value_counts()
    x = values[values==1].index.values
    geo_df = geo_df[~geo_df.MMSI.isin(x)]

    # ## then make the linestring/ship_path of each ships
    ship_path = geo_df.groupby("MMSI")['geometry'].apply(lambda x: LineString(x.to_list())).copy()
    ship_path = gpd.GeoDataFrame(ship_path).reset_index()
    return ship_path

def find_intersections(ship_path, cs):
    
    '''
    Finds the intersection coordinate of a ship path with given cross-section
    PARAMETERS
    ------------
    1. Ship path - takes a geodataframe containing the path of each ship
    2. cross-section - shapefile of the cross where we want the intersections
    
    RETURNS: a geodataframe with intersection coordindates stored as shapely.geometry.Point object
    '''
    
    ship_path["intersection"] = ship_path.geometry.intersection(cs)
    intersections = ship_path[ship_path['intersection'].apply(lambda x: isinstance(x,Point))]
    intersections.drop("geometry", axis=1, inplace=True) ## remove the path line string 
    return intersections

def ais_data_near_cs(ais_gdf, cs, MMSI_ids=None, properties = ["# Timestamp", "Ship type"]):
    '''
    Finds the nearest available data to the given cross-section. 
    
    PARAMETERS
    ---------------
    ais_data: GeoDataframe containing ais data of all the ships (coordindates in Point(x,y))
    cs: cross-section line
    MMSI_ids: filtering ships based on MMSI, 
    if given none then the function will determine for all the ships in the data
    
    RETURNS
    -----------------
    a dataframe containing the nearest available data for each ship
    '''
    
    if MMSI_ids is not None:
        ais_gdf = ais_gdf[ais_gdf["MMSI"].isin(MMSI_ids)].copy().reset_index()

    ## 4.1 Calculate the distance of each ais data points to find the nearest point from the reference point of each ship/MMSI:
    ais_gdf['distance'] = ais_gdf.distance(cs)
    
    ## 4.2 find the indices with nearest point from the center:
    indices = ais_gdf.groupby(by="MMSI")['distance'].idxmin()
    
    ## 4.3 filtered out the entries that are the nearest ones:
    details = ais_gdf[ais_gdf.index.isin(indices)].set_index("MMSI")[properties]

    return details
    
def find_routes(ais_data_folder, waypoint):
    
    '''
    Pipeline:
    1. Finds out the intersection of ais data points with 
    listed waypoints (shapefile) from a large number of csv files
    
    PARAMETERS:
    -------------
    folder - where all the ais data in csv format is stored
    waypoint - a ESRI shapefile containing the lines representing waypoints
    (here the number of lines can be of anynumber > 1)
    
    RETURNS:
    ------------
    returns a concatenated dataframes of intersection
    '''
    
    start_time = time.time()
    
    
    all_intersections = []
    
    pickle_files = glob(folder+"*.pickle")
    for file in tqdm(pickle_files, unit="pkl_file"):
        
        with open(file, "rb") as pkl_file:

            ais_data = pickle.load(pkl_file)
            #filtering the data that belongs to fehmarnbelt region
            # to reduce the reading time
            x1,y1 = 9.961,54.8190
            x2,y2 = 12.15,54.2363
            df = ais_data.loc[(ais_data['Latitude']>=y2) 
                                    & (ais_data['Latitude']<=y1) 
                                    & (ais_data['Longitude']>=x1) 
                                    & (ais_data['Longitude']<=x2)]
            
            df = df[["# Timestamp", "MMSI", "Latitude", "Longitude","Ship type"]].copy() 
            del(ais_data)
            
            gdf = convert_ais_data_to_gdf(df)
            routes = ais_data_to_path(gdf)
            
            ## find the intersecting ships at all the cross-sections
            for layer, line in zip(waypoint.waypoint, waypoint.geometry):
                intersections = find_intersections(routes, line)
                intersections["waypoint"] = layer
                
                ## Find the ais data for each ship near the cross-section 
                mmsi_ids = intersections.MMSI.values
                properties = ais_data_near_cs(gdf, line, MMSI_ids=mmsi_ids)
                
                ## Join the datasets
                intersections.set_index("MMSI", inplace=True)
                intersections = intersections.join(properties, how="left")
                
                all_intersections.append(intersections)

    
    duration = time.time()-start_time
    print("Time taken: %.2f minutes to find the intersection points" % (duration/60))
    output_df = pd.concat(all_intersections)
    return output_df

   
## Now loop through the ais data given day-wise and find the intersection with given cross-section

folder = "C:\\Users\kvba\\OneDrive - Ramboll\\Projects\\Ship traffic data of Fehmarnbelt\\data\\ais_data_pkl\\"
shapefile_location = "C:\\Users\\kvba\\OneDrive - Ramboll\\Projects\\Ship traffic data of Fehmarnbelt\\T-route\\Analysis 4 - Trip analysis on same route\\shapefile\\kiel_to_klaipeda.zip"
shapefile = gpd.read_file("zip://"+shapefile_location)
intersections = find_routes(folder, shapefile)

output_folder = "C:\\Users\\kvba\\OneDrive - Ramboll\\Projects\\Ship traffic data of Fehmarnbelt\\T-route\\Analysis 4 - Trip analysis on same route\\output"
output_file = os.path.join(output_folder,"intersections.pkl")
with open(output_file, "wb") as pickle_file:
    pickle.dump(intersections, pickle_file)
    