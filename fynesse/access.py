"""Provide access to the desired property price data without cleaning the data."""
import pandas as pd

from .config import config
from .database import execute_query, download_data_file, create_connection, read_credentials

"""Make sure you have legalities correct, both intellectual property and personal data privacy rights."""

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


def data():
    credentials = read_credentials()
    conn = create_connection(
        user=credentials["username"],
        password=credentials["password"],
        host=config["database_url"],
        database=config["database_name"],
        port=config["port"]
    )
    query = """SELECT * FROM prices_coordinates_data;"""
    return pd.read_sql(query, conn)


def download_property_data(year: int):
    url = f"http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-{year}.csv"
    download_data_file(url, f"{year}.csv")


def download_postcode_data():
    url = "https://www.getthedata.com/downloads/open_postcode_geo.csv.zip"
    filename = "postcodes.csv.zip"
    download_data_file(url, filename, unzip=True)


def select_region_period_prices(conn, postcode_region, start_date, end_date):
    postcode_regex = f"{postcode_region}%"
    query = """
        CREATE VIEW region_prices AS (
            SELECT * FROM pp_data
            WHERE
                postcode LIKE %s AND
                date_of_transfer >= %s AND
                date_of_transfer < %s
        ); 
    """
    args = (postcode_regex, start_date, end_date)
    execute_query(conn, query, args)


def join_region_prices_with_coordinates(conn):
    """Join postcode_data to region_prices table and store in prices_coordinates_data table."""
    query = """
        CREATE TABLE prices_coordinates_data AS (
            SELECT 
                price,
                date_of_transfer,
                postcode,
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
                db_id
            FROM `region_prices` INNER JOIN `postcode_data`
            ON region_prices.postcode = postcode_data.postcode
        );
    """
    execute_query(conn, query)
