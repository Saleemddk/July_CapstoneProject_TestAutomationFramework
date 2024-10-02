import pandas as pd
import pytest
from sqlalchemy import create_engine
import logging


# Configure the logging
from CommonUtilities.config import *
from CommonUtilities.utilities import *
logging.basicConfig(
    filename='etlTestAutomation.log',  # Name of the log file
    filemode='a',        # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO    # Set the logging level
)
logger = logging.getLogger(__name__)


# create mysql database commection
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')


# test load fact_sales
def test_fact_sales_table_load_check():
    query_expected = """  SELECT
        sales_id,
        product_id,
        store_id,
        quantity,
        total_sales,
        sale_date
    FROM sales_with_details"""
    query_actual = """SELECT * FROM fact_sales"""
    db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)



# test load fact_inventory
def test_fact_inventorytable_load_check():
    query_expected = """SELECT
        product_id,
        store_id,
        quantity_on_hand,
        last_updated
    FROM staging_inventory"""
    query_actual = """SELECT * FROM fact_inventory"""
    db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)



# test load_monthly_sales_summary
def test_load_monthly_sales_summarycheck():
    query_expected = """SELECT 
        product_id,
        month,
        year,
        total_sales
    FROM monthly_sales_summary_source"""
    query_actual = """SELECT * FROM monthly_sales_summary"""
    db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)





# test load_inventory_levels_by_store
def test_load_inventory_levels_by_store_check():
    query_expected = """SELECT 
        store_id,
        total_inventory
    FROM aggregated_inventory_levels"""
    query_actual = """SELECT * FROM inventory_levels_by_store"""
    db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)
