import sys
import time

import yaml

from loguru import logger
from ETL.extract import (
    get_yearly_data_urls,
    read_data,
    download_dir
)
from ETL.transform import (
    concatenate_data,
    transform_data,
    generate_id_dim
)
from ETL.load import (
    spark_s,
    load_to_bronze,
    load_to_gold,
    load_to_silver
)

# Remove the default logger configuration
logger.remove()

# Log to a file with DEBUG level
logger.add("estate.log", rotation="900kb", level="DEBUG")

# Log to the console with INFO level (less verbose)
logger.add(sys.stderr, level="INFO") 


with open("config.yml", "r") as file:
    global_config = yaml.safe_load(file)

def run_pipeline():
    """This function run your Data pipline

    """
    logger.info(f"Start process ...")


    all_links    = get_yearly_data_urls()
    #logger.info(f"All links get correctly.")

    read_links   = read_data(download_dir, all_links)
    logger.info("All links read and download correctly.")

    # Transformation
    data_combine = concatenate_data(read_links)
    data_transf  = transform_data(data_combine, global_config["col_list"], global_config["col_drop"])
    logger.info(f"All data are transformed correctly")

    all_dim_facts_tables = generate_id_dim(data_transf)
    logger.info(f"All facts table and dim tables are created correctly.")


    # load to bronze 
    #load_to_bronze(read_links)

    # Load to silver
    load_to_silver(data_transf)

    # Load to gold layer
    #load_to_gold(all_dim_facts=all_dim_facts_tables)

    logger.info("All data are loaded correctly.")
    
    
    logger.info(f"Pipeline finished!")

if __name__ == "__main__":

    run_pipeline()

    #print(df)