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
@pytest.mark.regression
# filter transformation check
def test_filter_transformation_check():
    query_expected = """SELECT * FROM staging_sales where sale_date>='2024-09-05'"""
    query_actual = """SELECT * FROM filtered_sales"""
    db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)

@pytest.mark.regression
# Router high_sales transformation check
def test_Router_high_sales_transformation_check():
    query_expected = """SELECT * FROM filtered_sales WHERE region = 'High'"""
    query_actual = """SELECT * FROM high_sales"""
    db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)

# Router low_sales transformation check
def test_Router_low_sales_transformation_check():
    query_expected = """SELECT * FROM filtered_sales WHERE region = 'low'"""
    query_actual = """SELECT * FROM low_sales"""
    db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)

# aggregate_sales_data transformation check
def test_aggregate_sales_data_transformation_check():
    query_expected = """SELECT 
        product_id,
        MONTH(sale_date) AS month,
        YEAR(sale_date) AS year,
        SUM(quantity * price) AS total_sales
    FROM filtered_sales
    GROUP BY product_id, MONTH(sale_date), YEAR(sale_date)"""
    query_actual = """SELECT * FROM monthly_sales_summary_source"""
    db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)

# joiner_sales_data transformation check
def test_join_sales_data_transformation_check():
    query_expected = """SELECT 
        s.sales_id,
        s.product_id,
        p.product_name,
        s.store_id,
        st.store_name,
        s.quantity,
        (s.quantity * s.price) AS total_sales,
        s.sale_date
    FROM filtered_sales s
    JOIN staging_product p ON s.product_id = p.product_id
    JOIN staging_store st ON s.store_id = st.store_id"""
    query_actual = """SELECT * FROM sales_with_details"""
    db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)

# aggregate_inventory_levels transformation check
def test_aggregate_inventory_levels_transformation_check():
    query_expected = """SELECT 
        store_id,
        SUM(quantity_on_hand) AS total_inventory
    FROM staging_inventory
    GROUP BY store_id"""
    query_actual = """SELECT * FROM aggregated_inventory_levels"""
    db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)
