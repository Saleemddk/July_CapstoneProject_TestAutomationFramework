import pandas as pd
import pytest
from sqlalchemy import create_engine
import logging


# Configure the logging
from CommonUtilities.config import *
from CommonUtilities.utilities import file_to_db_verify, db_to_db_verify

logging.basicConfig(
    filename='Logs/etlTestAutomation.log',  # Name of the log file
    filemode='a',        # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO    # Set the logging level
)
logger = logging.getLogger(__name__)


# create mysql database commection
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')
# Create Oracle engine
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')



def test_dataExtrcation_from_Sales_data_File():
    file_to_db_verify("TestData/sales_data.csv","staging_sales",mysql_engine,'csv')


def test_dataExtrcation_from_product_data_File():
    file_to_db_verify("TestData/product_data.csv","staging_product",mysql_engine,'csv')

def test_dataExtrcation_from_inventory_data_File():
    file_to_db_verify("TestData/inventory_data.xml","staging_inventory",mysql_engine,'xml')

def test_dataExtrcation_from_supplier_data_File():
    file_to_db_verify("TestData/supplier_data.json", "staging_supplier", mysql_engine,'json')


def test_dataExtrcation_from_stores_oracle_table():
    query1 = """select * from stores"""
    query2 = """select * from staging_store"""
    db_to_db_verify(query1,oracle_engine,query2,mysql_engine)

