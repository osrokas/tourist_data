# Importing libraries
from datetime import datetime
from re import T
from time import time
import pandas as pd
import requests
import json
from shapely.geometry import Polygon
import geopandas as gpd
import re

def unix_to_timestamp(unix: int):
    """
    The function convert unix to human readable timestamp
    """
    # Checking if its timestamp in seconds
    if len(str(unix)) == 10:
        timestamp = datetime.utcfromtimestamp(
            unix).strftime(('%Y-%m-%d'))
        return timestamp
    # Checking if its timestamp in miliseconds
    elif len(str(unix)) == 13:
        timestamp = datetime.utcfromtimestamp(
            unix/1000).strftime(('%Y-%m-%d'))
        return timestamp
    else:
        print('Wrong time')


def create_grids_list():
    """
    The function creates list of grids cells.
    """
    return_offsets = [*range(0, 5700, 50)]
    grids = []

    for return_offset in return_offsets:
        url_grid = 'https://services-eu1.arcgis.com/hv2udBVWYCpbTvAz/ArcGIS/rest/services/Day/FeatureServer/0/query?where=+1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=Gardele&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=true&cacheHint=false&orderByFields=Gardele&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset={0}&resultRecordCount=50&returnZ=false&returnM=false&returnExceededLimitFeatures=false&quantizationParameters=&sqlFormat=none&f=pjson&token='.format(
            str(return_offset))
        req = requests.get(url_grid)
        decoded_content = req.content.decode('utf-8')
        data_json = json.loads(decoded_content)
        data_features = data_json['features']
        for attributes in data_features:
            grid = attributes['attributes']['Gardele']
            grids.append(grid)

    return grids


def create_days_list():
    """
    The functions creates list of days.
    """
    return_days = [*range(0, 200, 50)]
    days = []

    for day in return_days:
        url_day = 'https://services-eu1.arcgis.com/hv2udBVWYCpbTvAz/ArcGIS/rest/services/Day/FeatureServer/0/query?where=+1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=period&returnGeometry=false&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=true&cacheHint=false&orderByFields=period&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset={0}&resultRecordCount=50&returnZ=false&returnM=false&returnExceededLimitFeatures=false&quantizationParameters=&sqlFormat=none&f=pjson&token='.format(
            str(day))
        req = requests.get(url_day)
        decoded_content = req.content.decode('utf-8')
        data_json = json.loads(decoded_content)
        data_features = data_json['features']
        for attributes in data_features:
            period = attributes['attributes']['period']
            timestamp = unix_to_timestamp(period)
            days.append(timestamp)

    return days


def read_day(day: str, grid_id: int):
    """
    The function reads feature.

    Arguments
    ---------
    day: str
        timestamp of grid feature
    grid_id: int
        grid feature id
    ---------
    """
    try:
        url = 'https://services-eu1.arcgis.com/hv2udBVWYCpbTvAz/ArcGIS/rest/services/Day/FeatureServer/0/query?where=period+%3D+%27{0}%27+and+Gardele+%3D+{1}&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=*&returnGeometry=true&returnCentroid=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=false&quantizationParameters=&sqlFormat=none&f=pjson&token='.format(
            day,
            grid_id)
        req = requests.get(url)
        decoded_content = req.content.decode('utf-8')
        data_json = json.loads(decoded_content)
        data_fields = data_json['fields']

        columns = []
        for fields in data_fields:
            columns.append(fields['name'])
        df = pd.DataFrame(columns=columns)
        data_features = data_json['features']
        for data in data_features:
            data_attributes = data['attributes']
            data_geometry = data['geometry']
            data_attributes.update(data_geometry)
            df = df.append(data_attributes, ignore_index=True)
            
        df['geometry'] = df.apply(lambda row: Polygon(row['rings'][0]), axis=1)
        gdf = gpd.GeoDataFrame(df,geometry='geometry',crs='epsg:3857')
        gdf = gdf.to_crs({'init': 'epsg:4326'})
        gdf = gdf[['Gardele','period','Apsilankym','geometry']]

        return gdf

    except KeyError:
        pass


def load_data():
    """
    The function loads data from service
    """
    grids = create_grids_list()
    days = create_days_list()
    gdf = gpd.GeoDataFrame()
    for day in days:
        for grid in grids:
            try:
                dataframe = read_day(day=day, grid_id=grid)
                gdf = gdf.append(dataframe, ignore_index=True)
                # gdf.to_file('dataset/temp/op_data.shp')
            except:
                pass

    gdf.to_file(driver = 'ESRI Shapefile', filename= "dataset/tourists.shp")


if __name__ == '__main__':
    load_data()

