{\rtf1\ansi\ansicpg936\cocoartf2639
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0

\f0\fs24 \cf0 # bronze_to_silver.py\
\
import json\
import pandas as pd\
from pyspark.sql import SparkSession\
\
# Initialize Spark session\
spark = SparkSession.builder \\\
    .appName("Bronze to Silver Transformation") \\\
    .getOrCreate()\
\
def load_bronze_tables(movie_files):\
    """Load movie files into bronze tables."""\
    bronze_tables = []\
    for file in movie_files:\
        with open(file, 'r') as f:\
            data = json.load(f)\
            df = pd.DataFrame(data)\
            bronze_tables.append(df)\
    return bronze_tables\
\
def fix_negative_runtimes(bronze_table):\
    """Identify and quarantine records with negative runtimes."""\
    quarantine = bronze_table[bronze_table['runtime'] < 0]\
    bronze_table.loc[bronze_table['runtime'] < 0, 'quarantine'] = True\
    bronze_table.loc[bronze_table['runtime'] < 0, 'runtime'] = bronze_table['runtime'].abs()\
    return bronze_table, quarantine\
\
def fix_missing_genre_names(bronze_table):\
    """Handle cases where genre names are missing."""\
    # Assuming genre_id is present but name is missing\
    # We can fetch the missing names from another source if available\
    # For now, we'll keep the missing values as they are\
    return bronze_table\
\
def adjust_budgets(bronze_table):\
    """Ensure all movies have a minimum budget of 1 million."""\
    bronze_table.loc[bronze_table['budget'] < 1000000, 'budget'] = 1000000\
    return bronze_table\
\
def deduplicate_records(bronze_table):\
    """Ensure only unique records appear in the silver tables."""\
    # Assuming 'status' column indicates the status of records\
    bronze_table = bronze_table.sort_values(by='status', ascending=False).drop_duplicates(subset='movie_id')\
    return bronze_table\
\
def create_silver_tables(bronze_table):\
    """Create silver tables for movies, genres, and original languages."""\
    # Assuming bronze table contains columns: movie_id, genre_id, original_language_id, ...\
    # Extract unique values for genres and original languages\
    genres = bronze_table[['genre_id', 'genre_name']].drop_duplicates()\
    original_languages = bronze_table[['original_language_id', 'original_language_name']].drop_duplicates()\
    \
    # Create Spark DataFrame for silver tables\
    movie_df = spark.createDataFrame(bronze_table)\
    genre_df = spark.createDataFrame(genres)\
    original_language_df = spark.createDataFrame(original_languages)\
    \
    return movie_df, genre_df, original_language_df\
\
def persist_silver_tables(silver_tables):\
    """Persist silver tables in Delta format to DBFS."""\
    # Assuming DBFS path for storage\
    movie_df, genre_df, original_language_df = silver_tables\
    movie_df.write.format("delta").save("/Users/liuyunrui/Downloads/adb_project/movie_silver")\
    genre_df.write.format("delta").save("/Users/liuyunrui/Downloads/adb_project/genre_silver")\
    original_language_df.write.format("delta").save("/Users/liuyunrui/Downloads/adb_project/original_language_silver")\
\
def bronze_to_silver(movie_files):\
    """Transform bronze tables to silver tables."""\
    bronze_tables = load_bronze_tables(movie_files)\
    for bronze_table in bronze_tables:\
        bronze_table, quarantine = fix_negative_runtimes(bronze_table)\
        bronze_table = fix_missing_genre_names(bronze_table)\
        bronze_table = adjust_budgets(bronze_table)\
        bronze_table = deduplicate_records(bronze_table)\
        silver_tables = create_silver_tables(bronze_table)\
        persist_silver_tables(silver_tables)\
\
if __name__ == "__main__":\
    movie_files = ["/Users/liuyunrui/Downloads/adb_project/movie1.json", "/Users/liuyunrui/Downloads/adb_project/movie2.json", ...]\
    bronze_to_silver(movie_files)\
}