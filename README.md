

# Process of extracting data from an S3 bucket into a Snowflake warehouse and transforming using dbt:

1. Create a Snowflake database and schema to store the extracted data.

2. Set up a Snowflake external stage that points to the S3 bucket location. This will allow Snowflake to access the data in the S3 bucket.

3. Create a Snowflake table to hold the data extracted from the S3 bucket. This table should have columns that match the data in the S3 bucket.

4. Use Snowflake's COPY INTO command to copy the data from the S3 bucket into the Snowflake table. This command can be run manually or set up to run on a schedule using a Snowflake task.

5. Create a dbt project and connect it to the Snowflake database.

6. Define a dbt model to transform the data as needed. This can involve joining tables, aggregating data, or creating new columns.

7. Run dbt to execute the defined models and create new tables in the Snowflake schema.

8. Analyze the transformed data in Snowflake or visualize it in a BI tool such as Tableau or Looker.

9. Monitor the process and make necessary adjustments to ensure data accuracy and completeness.


# Snowflake Data Extraction and Loading

This repository also contains Python code for setting up a Snowflake infrastructure, loading data into tables, and creating an analytics schema. The code uses the snowflake.connector library to establish a connection to Snowflake, create warehouses, databases, schemas, and tables, and load data into those tables. The code also uses pandas to load a CSV file into a Snowflake table.

## Requirements
To run this code, you will need:

Python 3.x
The snowflake-connector-python package
The pandas package
You will also need credentials for a Snowflake account.

Check the "requirement.txt" file

## Usage
Clone this repository to your local machine.
Install the required packages by running pip install -r requirements.txt.
Edit the user, password, account, and role variables in the code to match your Snowflake credentials.
Run the code using python snowflake_loading.py.
The code will create a Snowflake infrastructure consisting of a warehouse, a database, a staging schema, and three tables (reviews, orders, and shipments_deliveries). The code will also load data into the reviews and orders tables using the COPY INTO statement, and load data from a CSV file into the shipments_deliveries table using pandas.

Finally, the code will create an analytics schema named franegah6821_analytics.

## STEPS FOR USING CODE

- Clone the files to local machine
- Install requirements in the different folders
- Open two command line terminals 
- Run 'python code_1.py' for extraction and loading data into snowflake in one terminal
- Run 'dbt run' for data transformation on the other terminal 
- Run 'dbt run-operation unload_data_to_s3' to load table to s3