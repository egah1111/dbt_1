Snowflake Data Loading and Analytics
This repository contains Python code for setting up a Snowflake infrastructure, loading data into tables, and creating an analytics schema. The code uses the snowflake.connector library to establish a connection to Snowflake, create warehouses, databases, schemas, and tables, and load data into those tables. The code also uses pandas to load a CSV file into a Snowflake table.

Requirements
To run this code, you will need:

Python 3.x
The snowflake-connector-python package
The pandas package
You will also need credentials for a Snowflake account.

Usage
Clone this repository to your local machine.
Install the required packages by running pip install -r requirements.txt.
Edit the user, password, account, and role variables in the code to match your Snowflake credentials.
Run the code using python snowflake_loading.py.
The code will create a Snowflake infrastructure consisting of a warehouse, a database, a staging schema, and three tables (reviews, orders, and shipments_deliveries). The code will also load data into the reviews and orders tables using the COPY INTO statement, and load data from a CSV file into the shipments_deliveries table using pandas.

Finally, the code will create an analytics schema named franegah6821_analytics.
