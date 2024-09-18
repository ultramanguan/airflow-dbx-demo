"""
The entry point of the Python Wheel
"""

import sys
from src.common.functions import add_ingestion_date
from src.common.configuration import raw_folder_path,processed_folder_path,presentation_folder_path
from pyspark.sql import SparkSession
appName = "abc"


def main():
  # This method will print the provided arguments
  # Set up Spark configuration
  # Create a SparkSession
  spark = SparkSession.builder \
      .appName(appName) \
      .getOrCreate()
  drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
              .withColumnRenamed("number", "driver_number") \
              .withColumnRenamed("name", "driver_name") \
              .withColumnRenamed("nationality", "driver_nationality") 
  
  constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
  .withColumnRenamed("name", "team") 

  circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") 
  
  races_df = spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date") 
  
  results_df = spark.read.parquet(f"{processed_folder_path}/results") \
.withColumnRenamed("time", "race_time") 
  
  race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)
  
  race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)
  
  final_df_stg = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position")
  final_df=add_ingestion_date(final_df_stg)

  final_df.createOrReplaceTempView("final")

  sql="""
  select *
  from final
  where race_year=2020 and race_name='Abu Dhabi Grand Prix'
  order by points desc
  """

  spark.sql(sql).show()


if __name__ == '__main__':
  main()