import pandas as pd
import pytest
import os
from sqlalchemy import create_engine
import logging
import cx_Oracle

# Configure the logging
from CommonUtilities.config import *
logging.basicConfig(
    filename='logs/etlTestAutomation.log',  # Name of the log file
    filemode='a',        # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO    # Set the logging level
)
logger = logging.getLogger(__name__)


# create mysql database commection
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')
# Create Oracle engine
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')


def file_to_db_verify(file_path,table_name,db_engine,file_type):
    if file_type == "csv":
        expected_df = pd.read_csv(file_path)
    elif file_type == "xml":
        expected_df = pd.read_xml(file_path,xpath = './/item')
    elif file_type == "json":
        expected_df = pd.read_json(file_path)
    else:
        raise ValueError(f"unsupported file type : {file_type}")

    query = f"select * from {table_name};"
    actual_df = pd.read_sql(query, db_engine)
    assert actual_df.equals(expected_df), f"Data extraction failed."
    print("Sales data extarcted and laoded to staging successfuly")

def db_to_db_verify(query1,db_engine1,query2,db_engine2,):
    actual_df = pd.read_sql(query1,db_engine1).astype(str)
    expected_df = pd.read_sql(query2, db_engine2).astype(str)
    assert actual_df.equals(expected_df), f"Data extraction failed."

