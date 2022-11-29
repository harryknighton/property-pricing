"""Provide access to raw data that can be used for property price prediction.

Licenses:
    - Contains HM Land Registry data © Crown copyright and database right 2021. This data is licensed under the Open Government Licence v3.0.
    - Office for National Statistics licensed under the Open Government Licence v.3.0
    - Contains OS data © Crown copyright and database right 2022
"""
import logging
from typing import List

import geopandas as gpd
import pandas as pd
import osmnx as ox

from .config import config
from .database import execute_query, download_data_file, create_connection, read_credentials

PP_DATA_TABLE = """
    DROP TABLE IF EXISTS `pp_data`;
    CREATE TABLE IF NOT EXISTS `pp_data` (
        `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
        `price` int(10) unsigned NOT NULL,
        `date_of_transfer` date NOT NULL,
        `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
        `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
        `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
        `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
        `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
        `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
        `street` tinytext COLLATE utf8_bin NOT NULL,
        `locality` tinytext COLLATE utf8_bin NOT NULL,
        `town_city` tinytext COLLATE utf8_bin NOT NULL,
        `district` tinytext COLLATE utf8_bin NOT NULL,
        `county` tinytext COLLATE utf8_bin NOT NULL,
        `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
        `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
        `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY
    ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1;
    
    CREATE INDEX `pp.postcode` USING HASH
        ON `pp_data`
            (postcode);
    CREATE INDEX `pp.date` USING HASH
        ON `pp_data` 
            (date_of_transfer);
"""

POSTCODE_DATA_TABLE = """
    DROP TABLE IF EXISTS `postcode_data`;
    CREATE TABLE IF NOT EXISTS `postcode_data` (
        `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
        `status` enum('live','terminated') NOT NULL,
        `usertype` enum('small', 'large') NOT NULL,
        `easting` int unsigned,
        `northing` int unsigned,
        `positional_quality_indicator` int NOT NULL,
        `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
        `latitude` decimal(11,8) NOT NULL,
        `longitude` decimal(10,8) NOT NULL,
        `postcode_no_space` tinytext COLLATE utf8_bin NOT NULL,
        `postcode_fixed_width_seven` varchar(7) COLLATE utf8_bin NOT NULL,
        `postcode_fixed_width_eight` varchar(8) COLLATE utf8_bin NOT NULL,
        `postcode_area` varchar(2) COLLATE utf8_bin NOT NULL,
        `postcode_district` varchar(4) COLLATE utf8_bin NOT NULL,
        `postcode_sector` varchar(6) COLLATE utf8_bin NOT NULL,
        `outcode` varchar(4) COLLATE utf8_bin NOT NULL,
        `incode` varchar(3)  COLLATE utf8_bin NOT NULL,
        `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY
    ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
    
    CREATE INDEX `po.postcode` USING HASH
        ON `postcode_data`
            (postcode);
"""


def data(bbox, start_date, end_date):
    """Generate the required data and return it as a GeoDataFrame"""
    credentials = read_credentials()
    conn = create_connection(
        user=credentials["username"],
        password=credentials["password"],
        host=config["database_url"],
        database=config["database_name"],
        port=config["port"]
    )
    join_region_prices_with_coordinates(conn, *bbox, start_date, end_date)
    df = pd.read_sql("SELECT * FROM `prices_coordinates_data`;", conn)
    df_geometry = gpd.points_from_xy(df.longitude, df.latitude)
    gdf = gpd.GeoDataFrame(df, geometry=df_geometry)
    gdf.crs = "EPSG:4326"
    conn.close()
    return gdf


def download_property_data(year: int):
    url = f"http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-{year}.csv"
    download_data_file(url, f"{year}.csv")


def download_postcode_data():
    url = "https://www.getthedata.com/downloads/open_postcode_geo.csv.zip"
    filename = "open_postcode_geo.csv.zip"
    download_data_file(url, filename, unzip=True)


def join_region_prices_with_coordinates(conn, north, south, east, west, start_date, end_date):
    """Join `postcode_data` to `pp_data` table and store in `prices_coordinates_data` table."""
    query = """
        DROP TEMPORARY TABLE IF EXISTS `prices_coordinates_data`;
        CREATE TEMPORARY TABLE `prices_coordinates_data` AS (
            SELECT 
                price,
                date_of_transfer,
                pp.postcode,
                property_type,
                new_build_flag,
                tenure_type,
                locality,
                town_city,
                district,
                county,
                country,
                latitude,
                longitude,
                pp.db_id
            FROM pp_data AS pp INNER JOIN postcode_data AS pc
            ON
                pp.postcode = pc.postcode AND
                pc.latitude <= %s AND
                pc.latitude >= %s AND
                pc.longitude <= %s AND
                pc.longitude >= %s AND
                pp.date_of_transfer >= %s AND
                pp.date_of_transfer < %s
        );
    """
    args = (north, south, east, west, start_date, end_date)
    execute_query(conn, query, args)


def attach_shop_distances(df):
    """Attach the distance to the nearest shop to each property."""
    tags = {'shop': True}
    keys = ['geometry']
    pois = get_osm_pois(df, tags, keys)
    df_with_closest_shop = gpd.sjoin_nearest(
        df,
        pois,
        how='left',
        distance_col='distance_to_shop'
    )
    return df_with_closest_shop.drop(['index_right0', 'index_right1'], axis=1)


def get_osm_pois(df, tags, keys):
    """Get the list of nodes marked with tags and return a subset of their keys."""
    north = df['latitude'].max() + 0.01
    south = df['latitude'].min() - 0.01
    east = df['longitude'].max() + 0.01
    west = df['longitude'].min() - 0.01
    pois = ox.geometries_from_bbox(north, south, east, west, tags)
    keys_not_available = list(filter(lambda k: k not in pois.columns, keys))
    if keys_not_available:
        logging.warning(f"{keys_not_available} are not available in the OSM data")
    return pois[list(set(keys) - set(keys_not_available))]
