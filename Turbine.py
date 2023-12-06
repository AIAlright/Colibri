# Assuming that we return any records that are missing timestamp or turbine_id
# Assuming that we want to take the daily average power output for a turbine and assign that to any missing power_outputs for that turbine in the same day
#
#
#
#
#

from pyspark.sql import SparkSession
from functools import reduce 
from pyspark.sql.functions import col, avg, stddev_samp, min, max, trim, when
from pyspark.sql.types import StringType


# Initialize Spark session
spark = SparkSession.builder.appName('WindTurbineDataPipeline').getOrCreate()

# File paths for data loading
file_paths = [
    r'data_group_1.csv',
    r'data_group_2.csv',
    r'data_group_3.csv'
]




# Function to load data from CSV files
def load_data(file_paths):
    dfs = [spark.read.csv(file_path, inferSchema=True, header=True) for file_path in file_paths]
    return reduce( lambda df1, df2 : df1.union(df2), dfs).distinct().repartition("turbine_id", "timestamp")




# Function to clean the loaded data
def clean_data(data):
    # Filtering out bad data and converting timestamp to date format
    cleaned_data = data.filter((col("timestamp").isNotNull()) & (col("turbine_id").isNotNull()) & \
                               (trim(col("timestamp"))!="") & (trim(col("turbine_id"))!="")) \
                       .withColumn("timestamp", col("timestamp").cast("date")).withColumnRenamed("timestamp", "EventDate")
    
    # Filtering out bad data for upstream reprocessing
    bad_data = data.filter((col("timestamp").isNull()) | (col("turbine_id").isNull()) | \
                           (trim(col("timestamp"))=="") | (trim(col("turbine_id"))==""))
    
    return cleaned_data, bad_data





#use average of present rows as substitute for missing values
def impute_data(data):
    dfCleanAverage = data.filter( (col("power_output").isNotNull()) & (trim(col("power_output"))!="") ).\
                    groupby("EventDate","turbine_id").agg(avg("power_output").alias("DefaultDailyTurbineAverage"))
   
    data = data.withColumnRenamed("EventDate","ts").withColumnRenamed("turbine_id","tid").withColumnRenamed("power_output","po")
    
    imputed_data = dfCleanAverage.join(data, (dfCleanAverage["EventDate"] ==  data["ts"] ) & \
                                       ( dfCleanAverage["turbine_id"] ==  data["tid"]  ),"inner")
    

    imputed_finished = imputed_data.withColumn("imputed_power_output", \
                                               when((imputed_data.po.isNull()) | (trim(imputed_data.po.cast(StringType()))==""), imputed_data.DefaultDailyTurbineAverage)
                                               .otherwise(imputed_data.po)).\
                                                select("EventDate","turbine_id","wind_speed","wind_direction","imputed_power_output")
    
    return imputed_finished




# Function to compute summary statistics
def compute_summary_statistics(cleaned_data):  
    # Calculate min, max, average power output for each turbine
    summary_statistics = cleaned_data.groupBy("turbine_id", "EventDate") \
                                            .agg(min("imputed_power_output").alias("Minpower_output"), \
                                            max("imputed_power_output").alias("Maxpower_output"), \
                                            avg("imputed_power_output").alias("TurbineDayAvgpower_output"),\
                                            stddev_samp("imputed_power_output").alias("TurbineDailyStdDev"))
    
    return summary_statistics



# Function to identify anomalies and regular data
def identify_anomalies(impute_data, turbine_statistics):
    # Join cleaned data with daily average power to identify anomalies and regular data
    impute_data = impute_data.withColumnRenamed("EventDate", "ED").withColumnRenamed("turbine_id", "TD")
    regular_data = impute_data.join(turbine_statistics, \
        ((impute_data.ED == turbine_statistics.EventDate) & (impute_data.TD == turbine_statistics.turbine_id) ),"inner") \
        .filter(col("imputed_power_output").between( \
               col("TurbineDayAvgpower_output") - (2 * col("TurbineDailyStdDev")), \
               col("TurbineDayAvgpower_output") + (2 * col("TurbineDailyStdDev")) ) \
               )

    abnormal_data = impute_data.join(turbine_statistics, \
        ((impute_data.ED == turbine_statistics.EventDate) & (impute_data.TD == turbine_statistics.turbine_id) ),"inner") \
        .filter(~(col("imputed_power_output").between( \
               col("TurbineDayAvgpower_output") - (2 * col("TurbineDailyStdDev")), \
               col("TurbineDayAvgpower_output") + (2 * col("TurbineDailyStdDev")) ) \
               ))

    regular_data = regular_data.select(col("EventDate"),\
                              col("turbine_id"), col("wind_speed"),\
                              col("wind_direction"),col("imputed_power_output"),col("TurbineDayAvgpower_output"),\
                               col("TurbineDailyStdDev") )
    abnormal_data = abnormal_data.select(col("EventDate"),\
                              col("turbine_id"), col("wind_speed"),\
                              col("wind_direction"),col("imputed_power_output"),col("TurbineDayAvgpower_output"),\
                               col("TurbineDailyStdDev") )
    
    return regular_data, abnormal_data



data = load_data(file_paths)
cleaned_data , bad_data = clean_data(data)
imputed_data = impute_data(cleaned_data)
turbine_statistics = compute_summary_statistics(imputed_data)
regular, anomalies = identify_anomalies(imputed_data, turbine_statistics)

#show sample of data
bad_data.show(5)
imputed_data.show(5)
turbine_statistics.show(5)
anomalies.show(5)
regular.show(5)

# Write files to a database
#results = [ bad_data, imputed_data, turbine_statistics, regular, anomalies]
# [result.write.jdbc(url=jdbc_url, table="your_table_name", mode="overwrite", properties=properties) for result in results]

spark.stop()
