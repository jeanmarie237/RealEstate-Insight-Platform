import sys
from loguru import logger
#from ETL.extract import spark_session, get_yearly_data_urls, download_and_unzip, read_data
from pyspark.sql import functions as F 
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    split, 
    explode, 
    col,
    monotonically_increasing_id,
    row_number, 
    to_date, 
    year, 
    regexp_replace, 
    dayofmonth, 
    month, 
    when,
    avg
)

# Remove the default logger configuration
logger.remove()

# Log to a file with DEBUG level
logger.add("estate.log", rotation="900kb", level="DEBUG")

# Log to the console with INFO level (less verbose)
logger.add(sys.stderr, level="INFO") 

departements_cibles = [11, 75, 77, 78, 91, 92, 93, 94, 95]

def concatenate_data(read_data:list):
    """This function merge the dataframes
    
    """
    df_concat = read_data[0]

    for df in read_data[1:]:
        df_concat = df_concat.union(df)

    return df_concat


def transform_data(data_contact:object, col_list:list, col_drop:list):

    """
    
    """
    logger.info("Start the cleaning process ...")

    try:

        # Drop columns
        df_clean = data_contact.drop(*col_drop)

        # Rename columns corectly
        col_renam = [col.lower().replace(" ", "_") for col in df_clean.columns]
        df_clean = df_clean.toDF(*col_renam)

        df_clean = (

            df_clean.withColumn("surface_reelle_bati"        , col("surface_reelle_bati").cast("double"))
            .withColumn("nombre_pieces_principales"          , col("nombre_pieces_principales").cast("int"))
            .withColumn("surface_terrain"                    , col("surface_terrain").cast("double"))
            .withColumn("code_departement"                   , col("code_departement").cast("int"))
            .withColumn("code_commune"                       , col("code_commune").cast("int"))
            .withColumn("nombre_de_lots"                     , col("nombre_de_lots").cast("int"))
            .withColumn("code_postal"                        , col("code_postal").cast("int"))
        )

        df_clean = (
            df_clean.withColumn("date_mutation"  , to_date(df_clean["date_mutation"], "dd/MM/yyyy"))
            .withColumn("valeur_fonciere"        , regexp_replace(df_clean["valeur_fonciere"], ",", "."))
            .withColumn("valeur_fonciere"        , col("valeur_fonciere").cast("double"))
        )


         # moyenne
        mean_valeur_fonciere = df_clean.select(avg("valeur_fonciere")).first()[0]
        mean_surface_reelle_bati = df_clean.select(avg("surface_reelle_bati")).first()[0]
        mean_nombre_piece = df_clean.select(avg("nombre_pieces_principales")).first()[0]
        mean_surface_terrain = df_clean.select(avg("surface_terrain")).first()[0]

        window_spec = Window.orderBy("date_mutation")

        df_clean = (
             df_clean.withColumn("code_departement", when(col("code_departement").isNull(), 0).otherwise(col("code_departement")))
             .withColumn("code_postal"         , when(col("code_postal").isNull(), 0).otherwise(col("code_postal")))
             .withColumn("code_commune"        , when(col("code_commune").isNull(), 0).otherwise(col("code_commune")))

             .withColumn("commune"             , when(col("commune").isNull(), "inconnu").otherwise(col("commune")))
             .withColumn("nature_mutation"     , when(col("nature_mutation").isNull(), "inconnu").otherwise(col("nature_mutation")))

             .withColumn("date_mutation"        , when(col("date_mutation").isNull(), F.lag("date_mutation", 1).over(window_spec))
             .otherwise(col("date_mutation")))

             .withColumn("valeur_fonciere"     , when(col("valeur_fonciere").isNull(), mean_valeur_fonciere).otherwise(col("valeur_fonciere")))
             .withColumn("surface_reelle_bati" , when(col("surface_reelle_bati").isNull(), mean_surface_reelle_bati).otherwise(col("surface_reelle_bati")))
             .withColumn("surface_terrain" , when(col("surface_terrain").isNull(), mean_surface_terrain).otherwise(col("surface_terrain")))
             .withColumn("nombre_pieces_principales" , when(col("nombre_pieces_principales").isNull(), mean_nombre_piece).otherwise(col("nombre_pieces_principales")))
        )
    

        df_clean = (
            df_clean.withColumn("annee", year(col("date_mutation")))
            .withColumn("mois"         , month(col("date_mutation")))
            .withColumn("jour"         , dayofmonth(col("date_mutation")))
        )

        # 
        df_clean = df_clean.withColumn(
            "prix_a_payer",
            when(
                col("code_departement").isin(departements_cibles),
                col("valeur_fonciere")*0.97
            )
            .otherwise(
                col("valeur_fonciere")
            )
        )

        df_clean = df_clean.dropDuplicates()
    except Exception as e:
        logger.error(f"Error : {e} during the cleaning process.")

    logger.info("End of cleaning process.")
        
    return df_clean



def generate_id_dim(df_clean):
    """This function create the dimention table and generate the id
    :param df_clean:
    :return: return the list objects
    """

    window_temps = Window.orderBy("annee", "mois", "jour")
    window_commune = Window.orderBy("code_commune")
    window_mutation = Window.orderBy("nature_mutation")
    window_local = Window.orderBy("type_local", "code_type_local")

    all_dim_facts = []

    # Create dimension for temps
    dim_temps = (
        df_clean.select("annee", "mois", "jour") \
        .distinct() \
        .withColumn("id_temps", row_number().over(window_temps))
        .persist()
    )
    all_dim_facts.append(dim_temps)

    # Create dimension for localisation 
    dim_commune = (
        df_clean.select("code_commune", "commune", "code_postal", "code_departement") \
        .distinct() \
        .withColumn("id_commune", row_number().over(window_commune))
        .persist()
    )
    all_dim_facts.append(dim_commune)

    # Create dimension for Mutation
    dim_mutation = (
        df_clean.select("nature_mutation", "nombre_de_lots") \
        .distinct() \
        .withColumn("id_mutation", row_number().over(window_mutation))
        .persist()
    )
    all_dim_facts.append(dim_mutation)

    # Create dimension for immo
    dim_local = (
        df_clean.select("type_local", "code_type_local", "surface_reelle_bati", "nombre_pieces_principales") \
        .distinct() \
        .withColumn("id_local", row_number().over(window_local))
        .persist()
    )
    all_dim_facts.append(dim_local)

    # Jointure avec les dimensions pour récupérer les IDs
     # "identifiant_de_document"

    df_facts = df_clean.join(dim_temps, ["annee", "mois", "jour"], "left") \
             .join(dim_local, ["type_local", "code_type_local", "surface_reelle_bati", "nombre_pieces_principales"], "left") \
             .join(dim_commune, ["code_commune", "commune", "code_postal", "code_departement"], "left") \
             .join(dim_mutation, ["nature_mutation", "nombre_de_lots"], "left")

    df_facts = df_facts.select(
        "identifiant_de_document", "id_temps", "id_local", "id_commune", "id_mutation",
        "valeur_fonciere", "surface_reelle_bati", "nombre_pieces_principales",
        "surface_terrain", "prix_a_payer"
    ).persist()
    all_dim_facts.append(df_facts)


    return all_dim_facts 
