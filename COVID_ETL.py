# Author: Sparsh Tekriwal
# Date: 28th Jan, 2021
# ETL Function to load COVID Data 

import requests
import json
import re
import pandas as pd
from datetime import datetime
import sqlite3
import concurrent.futures 
from typing import List
from tqdm import tqdm
import time


class COVID_ETL(object):
    # Default URL and DB Name
    url = ""
    db_name = "" 
    
    def __init__(self, url= "https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD",\
                db_name = "covid.db"):
        self.url = url
        self.db_name = db_name
    
    ## Performs basic transformations to data from response and returns a Pandas DataFrame
    def transform(response) -> pd.DataFrame:

        ## Parsing Column Names
        columns_meta_data = response.json()["meta"]["view"]["columns"]
        column_names = [re.sub(':', '' , x["fieldName"]) for x in columns_meta_data]
        print("There are {} columns in this data set".format (str(len(column_names))))
        print("There are {} rows of data".format(str(len(response.json()["data"]))))

        ## Storing and cleaning data as a DataFrame
        df = pd.DataFrame(response.json()["data"], columns=column_names)
        df["test_date"] = pd.to_datetime(df["test_date"]).astype("str")
        df["county"] = df["county"].apply(lambda x: re.sub(' ', '_' , x.lower()).replace(".", ""))
        df[["new_positives", "cumulative_number_of_positives", "total_number_of_tests", "cumulative_number_of_tests"]] = df[["new_positives", "cumulative_number_of_positives", "total_number_of_tests", "cumulative_number_of_tests"]].astype("int")

        ## Selecting Desired Columns 
        df = df[["county", "test_date", "new_positives", "cumulative_number_of_positives", "total_number_of_tests", "cumulative_number_of_tests"]]
        df["load_date"] = datetime.today().strftime("%Y-%m-%d")

        return df

    def load(df, county_names, db_name = "covid.db"):
        # Create tables
        conn = sqlite3.connect(db_name)

        ## Since our program is CPU bound and not IO bound - using multi-processing instead of multi-threading 
        t1 = time.perf_counter()

        with concurrent.futures.ProcessPoolExecutor() as executer:
            results = [executer.submit(COVID_ETL.ingest, df, county_name, db_name) for county_name in county_names]
            for f in concurrent.futures.as_completed(results):
                print(f.result())
        t2 = time.perf_counter()

        print(f'Finished in {t2-t1} seconds')

    ## Function to generate table creation command
    def create_table_cmd(county_name):

        type_map = { "test_date": "TEXT", 
                        "new_positives": "INTEGER",
                        "cumulative_number_of_positives": "INTEGER",
                        "total_number_of_tests": "INTEGER",
                        "cumulative_number_of_tests": "INTEGER",
                        "load_date": "TEXT"  }
        sql_cols =  []
        sql_cols += [f"    {col} {type_map[col]}" for col in type_map]
        sql_cols = ',\n'.join(sql_cols)

        cmd = f"""CREATE TABLE if not exists {county_name} (
                    {sql_cols}
                    );"""
        return cmd


    ## Function to add data to table
    def ingest(df, county_name, db_name) -> str:
        conn_temp = sqlite3.connect(db_name)
        c = conn_temp.cursor()

        # Create Table with for County if it does not exist
        cmd = COVID_ETL.create_table_cmd(county_name)
        c.execute(cmd)

        # Adding Data to Table 
        df_county = df[df["county"] == county_name].drop(["county"], axis = 1)

        max_date_in_table = pd.read_sql(f"select max(test_date) from {county_name}", conn_temp).values[0][0]

        if max_date_in_table is not None:
             df_county = df_county[pd.to_datetime(df_county.test_date) > pd.to_datetime(max_date_in_table)]

        df_county.to_sql(county_name, conn_temp, if_exists='append', index = False)

        conn_temp.commit()
        conn_temp.close()

        return f"{county_name} table is updated on {datetime.today().date()} at {datetime.today().time().strftime('%H:%M %p')}. {df_county.shape[0]} row(s) added."

    ## Executer Function
    def run(self):
        
        try:
            response = requests.get(self.url)
            print(f"Loaded response from {self.url}")
            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            print (e.response.text)

            print(f"Response Code: {response.status_code}")

        df = COVID_ETL.transform(response)
        county_names = df.county.unique()
        assert(len(county_names) == 62), "Mismatch in the number of counties"

        COVID_ETL.load(df, county_names, self.db_name)
