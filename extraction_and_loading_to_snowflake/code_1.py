import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

# credentials
user="EGAH1111"
password="*****"
account="uo11147.eu-west-1"
role="ACCOUNTADMIN"

# Establishing connection to snowflake using connector
conn=sf.connect(user=user,password=password,account=account,role=role);

# Defining a function for connecting only
def connect(conn):
 
    return conn

# Defining a function for running queries
def run_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()

# Creating queries and setup of snowflake infrastruture
statement_Z='CREATE OR REPLACE WAREHOUSE mywarehouse WITH WAREHOUSE_SIZE="X-SMALL" AUTO_SUSPEND = 120 AUTO_RESUME = TRUE INITIALLY_SUSPENDED=TRUE;'
statement_Y='USE WAREHOUSE mywarehouse'
statement_1='DROP DATABASE IF EXISTS mine';
statement_2='CREATE DATABASE mine';
statement_3='USE DATABASE mine';
statement_X='CREATE SCHEMA sch_1'
statement_V='USE SCHEMA sch_1'
statement_a='CREATE OR REPLACE FILE FORMAT mycsvformat TYPE = "CSV" FIELD_DELIMITER = "," SKIP_HEADER = 1';
# Creating a staging schema
statement_4='CREATE STAGE franegah6821_staging URL="s3://d2b-internal-assessment-bucket/orders_data/"';

# statement for loading reviews.csv into reviews table
statement_5='CREATE TABLE reviews ( "review" INT NOT NULL, "product_id" INT NOT NULL PRIMARY KEY)';
statement_6='COPY INTO reviews FROM @franegah6821_staging/reviews.csv file_format = (type= "csv" field_delimiter="," skip_header=1)';

# statement for loading orders.csv into orders table
statement_7='CREATE TABLE orders ( "order_id" INT NOT NULL PRIMARY KEY, "customer_id" INT NOT NULL, "order_date" DATE NOT NULL, "product_id" VARCHAR NOT NULL, "unit_price" INT NOT NULL, "quantity" INT NOT NULL, "amount" INT NOT NULL)';
statement_8='COPY INTO orders FROM @franegah6821_staging/orders.csv file_format = (type= "csv" field_delimiter="," skip_header=1)';

# statement for loading schipments.csv into orders table
statement_9='CREATE TABLE shipments_deliveries ( "shipment_id" INT NOT NULL PRIMARY KEY, "order_id" INT NOT NULL, "shipment_date" DATE NULL, "delivery_date" DATE NULL)';
# statement_10='COPY INTO shipments_deliveries FROM @franegah6821_staging/shipments_deliveries.csv FILE_FORMAT = (FORMAT_NAME = MYCSVFORMAT) VALIDATION_MODE=RETURN_ALL_ERRORS';

# statemet for Creating analytics schema
statement_L='CREATE SCHEMA franegah6821_analytics'

# Running set_up, extraction and loading queries
run_query(conn,statement_Z)
run_query(conn,statement_Y)
run_query(conn,statement_1)
run_query(conn,statement_2)
run_query(conn,statement_3)
run_query(conn,statement_X)
run_query(conn,statement_V)
run_query(conn,statement_a)
run_query(conn,statement_4)
run_query(conn,statement_5)
run_query(conn,statement_6)
run_query(conn,statement_7)
run_query(conn,statement_8)
run_query(conn,statement_9)

#  Using pandas to load shipment_deliveries after download
df=pd.read_csv("shipment_deliveries.csv", sep=",")
write_pandas(conn=connect(conn),df=df,table_name="SHIPMENTS_DELIVERIES")

# creating analytic schema query
run_query(conn,statement_L)



