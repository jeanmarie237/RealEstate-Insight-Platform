import sys
import config
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col


# Remove the default logger configuration
logger.remove()

# Log to a file with DEBUG level
logger.add("estate.log", rotation="900kb", level="DEBUG")

# Log to the console with INFO level (less verbose)
logger.add(sys.stderr, level="INFO") 

spark_s = (
    SparkSession.builder.appName("Write To Azure")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.maxResultSize", "4g")
    .config("spark.sql.shuffle.partitions", "200")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-azure:3.2.0,org.apache.hadoop:hadoop-azure-datalake:3.2.0",
    )

    .getOrCreate()
)

spark_s.conf.set("fs.azure.account.auth.type", "OAuth")
spark_s.conf.set(
    "fs.azure.account.oauth.provider.type",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark_s.conf.set(f"fs.azure.account.oauth2.client.id"    , config.app_id)
spark_s.conf.set(f"fs.azure.account.oauth2.client.secret", config.client_secret)
spark_s.conf.set(
    f"fs.azure.account.oauth2.client.endpoint",
    f"https://login.microsoftonline.com/{config.tenant_id}/oauth2/token",
)
spark_s.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")



def load_to_bronze(all_list_data):
    """This function load the data cleaned to bronze layer 
    :param all_list_data: Links of the data
    :return: none
    """

    #file_path = f"abfss://{config.container_name}@{config.storage_account_name}.dfs.core.windows.net/{config.directory_name}/{name_table}"
    file_path = f"abfss://{config.container_name}@{config.storage_account_name}.dfs.core.windows.net/{config.directory_name}"

    logger.debug(f"Directoryt {file_path}")

    for i, df in enumerate(all_list_data):
        path_file = f"{file_path}/df_{i+1}.parquet"
        df.write.mode('overwrite').option("header", "true").parquet(path_file)
        logger.info(f"DataFrame {i+1} sauvegardé à : {path_file}")

    logger.success("All data have been saved to bronze layer successfully.")


def load_to_silver(all_df_cleaned:object):
    """This function load the data cleaned to silver layer
    :param all_df_cleaned: It is data cleaned
    :return: none
    """
    file_path = f"abfss://{config.container_name_s}@{config.storage_account_name}.dfs.core.windows.net/{config.directory_name_s}"
    logger.debug(f"Directoryt {file_path}")

    path_file = f"{file_path}/all_real_estate"
    # save data on parquet format
    all_df_cleaned.repartition(1).write.mode('overwrite').option("header", "true").parquet(path_file)

    logger.success("Data has been saved to silver successfully.")



def load_to_gold(all_dim_facts):
    """This function load data to gold
    
    """
    file_path = f"abfss://{config.container_name_g}@{config.storage_account_name}.dfs.core.windows.net/{config.directory_name_g}"
    
    # Tables list
    tables_names = ["dim_temps", "dim_commune", "dim_mutation", "dim_local", "df_facts"]

    logger.debug(f"Directoryt {file_path}")

    for i, df in enumerate(all_dim_facts):
        # path we would save the table
        path_file = f"{file_path}/{tables_names[i]}"
        # save data on parquet format
        df.coalesce(1).write.mode('overwrite').option("header", "true").parquet(path_file)

        logger.info(f"Table {tables_names[i]} has been saved to {path_file}")

    logger.success("All the tables has been saved to gold successfully.")

