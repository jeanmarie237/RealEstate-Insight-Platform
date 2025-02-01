
import os
import io
import sys
import zipfile
import requests
from pyspark.sql import SparkSession
from bs4 import BeautifulSoup
from loguru import logger 

# Remove the default logger configuration
logger.remove()

# Log to a file with DEBUG level
logger.add("estate.log", rotation="900kb", level="DEBUG")

# Log to the console with INFO level (less verbose)
logger.add(sys.stderr, level="INFO") 

# Param 
base_url = "https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/" 
download_dir = "data/downloads"

# Create a spark session 
def spark_session():

    logger.info("Creation on spark session")

    spark = SparkSession.builder\
        .appName("REALSTATE-INSIGHT-PLAFORM-DATA") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.maxResultSize", "4g") \
        .getOrCreate()
        
        #.master("spark://spark-master:7077") \
        #.config("spark.executor.memory", "1g") \
        #.config("spark.driver.memory", "1g") \
        #.config("spark.driver.host", "localhost") \

    return spark

def get_yearly_data_urls():
    """ This function extracts the files urls for each year
    :param fileparth: url of page
    :return: dictionary content year as key and url of this year 

    """
    logger.info("Fetching yearly data URls from the base URL.")

    # Send request GET for take the contenu of page
    response = requests.get(base_url)

    # If request loss we have message error
    if response.status_code != 200:
        logger.error(f"Failed to access the main page: {response.status_code}")
        raise Exception(f"Failed to access the main page: {response.status_code}") 

    # If request okay we analyse the HTML contenu of page
    soup = BeautifulSoup(response.content, "html.parser")

    # reaserch all links have (balise <a>) with attribut 'href'
    links = soup.find_all("a", href=True)

    # Dictionnary to stock links per year
    yearly_urls = {}
    for link in links:
        href = link['href']     # Take link of attribut 'href'

        # test all links which have "valeursfoncieres" and finsh with ".zip"
        if "valeursfoncieres" in href and href.endswith(".zip"):
            # Extract year on url
            year = href.split("-")[-1].split(".")[0]
            yearly_urls[year] = href
            logger.info(f"Found URL for {year}: {href}")

    return yearly_urls

def download_and_unzip(urlo:dict, download_dir:str):

    os.makedirs(download_dir, exist_ok=True)
    logger.info(f"Created download directory at {download_dir}")

    try:
        local_filename = os.path.join(download_dir, urlo.split('/')[-1])
        logger.info(f"Downloading {urlo} to {local_filename}")

        with requests.get(urlo,stream=True) as r:
            with open(local_filename,  'wb') as f:
                f.write(r.content)
        logger.success(f"Successfully downloaded {local_filename}")

        # Unzip file
        with zipfile.ZipFile(local_filename, 'r') as zip_ref:
            zip_ref.extractall(download_dir)
        logger.success(f"Successfully extrated {local_filename}")

    except Exception as e:
        logger.error(f"Error downloading or extractin {urlo}: {e}")

# Read the unzip file with spark
def read_data(download_dir: str, yearly_urls):
    """This function read data unzip
    :param download_dir: path wchich content the zip file and unzip file
    : :
    """
    for key, urlp in yearly_urls.items():
        download_and_unzip(urlp, download_dir)    

    spark = spark_session()
    dfs = []  # Liste pour stocker les DataFrames

    for file in os.listdir(download_dir):
        if file.endswith(".txt"):
            file_path = os.path.join(download_dir, file)
            try:
                logger.info(f"Reading file {file_path}")

                df = spark.read.csv(file_path, header=True, sep='|')
                #df.show(3)
                logger.success(f"Successfully read and displayed {file_path}")
                dfs.append(df)
            except Exception as e:
                logger.error(f"Error reading file {file_path} : {e}")

    return dfs

def main():
    yearly_urls = get_yearly_data_urls()
    df = read_data(download_dir, yearly_urls)
    return df
# print(df_f)

# def main():
#     yearly_urls = get_yearly_data_urls()
#     read_data(DOWNLOAD_DIR, yearly_urls)

if __name__ == "__main__":
    main()


