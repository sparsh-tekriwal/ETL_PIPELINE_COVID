# Author: Sparsh Tekriwal
# Date: 28th Jan 2021


import sys
from COVID_ETL import COVID_ETL

if __name__ == "__main__":
    
    if len(sys.argv) == 2:
        url = sys.argv[1]
        db_name = sys.argv[2]
        etl_pipeline = COVID_ETL(url, db_name)
    else:
        etl_pipeline = COVID_ETL()
        
    etl_pipeline.run()

