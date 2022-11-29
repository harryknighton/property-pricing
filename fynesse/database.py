"""Provide helper functions to access and handle data in the database."""

import logging
import os
import zipfile

import pymysql
from pymysql.constants import CLIENT
import requests
import yaml

from fynesse.config import config


def read_credentials():
    with open('credentials.yaml', 'r') as in_file:
        return yaml.safe_load(in_file)


def create_connection(user, password, host, database, port=3306):
    """Create a database connection to the MariaDB database
    specified by the host url and database name.
    """
    conn = None
    try:
        conn = pymysql.connect(
            user=user,
            passwd=password,
            host=host,
            port=port,
            local_infile=True,  # Allow uploads of local files
            db=database,
            client_flag=CLIENT.MULTI_STATEMENTS,  # Allow multiple statements to be executed in one call.
            autocommit=True,
        )
    except Exception as e:
        logging.error(f"Error connecting to the MariaDB Server: {e}")
    return conn


def execute_query(conn, query, args=None):
    """Execute the query on the connected database with the supplied arguments"""
    cur = conn.cursor()
    if args is None:
        cur.execute(query)
    else:
        cur.execute(query, args)
    rows = cur.fetchall()
    return rows


def download_data_file(src_url, filename, unzip=False):
    """Download the file stored at `src_url` to `filename` in the data directory."""
    filepath = os.path.join(config['data_directory'], filename)
    if not os.path.exists(filepath):
        response = requests.get(src_url)
        with open(filepath, 'wb') as out_file:
            out_file.write(response.content)
        if unzip:
            with zipfile.ZipFile(filepath, "r") as zipped_file:
                zipped_file.extractall(config['data_directory'])


def upload_csv_data_file(conn, filename, table_name, enclosing_char):
    """Upload a CSV file from the data directory to a connected database table."""
    filepath = os.path.join(config['data_directory'], filename)
    filepath = filepath.replace('\\', '/')
    query = f"""
        LOAD DATA LOCAL INFILE %s INTO TABLE {table_name}
        FIELDS TERMINATED BY ','
        ENCLOSED BY '{enclosing_char}'
        LINES TERMINATED BY '\\n';
    """
    execute_query(
        conn,
        query,
        (filepath, table_name),
    )
