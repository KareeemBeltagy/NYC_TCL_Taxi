#!/usr/bin/env python
# coding: utf-8

# ### Downloading Data files

# In[9]:


import requests
import os
import subprocess
import argparse
import sys


# In[32]:


parser = argparse.ArgumentParser()
parser.add_argument(
        "-d", "--download",
        action="store_true",
        help="Download data and move to HDFS."
    )

# Use parse_known_args() to ignore unexpected arguments (e.g., Jupyter's)
args, unknown = parser.parse_known_args()





# In[3]:


# downloading data from gihub repo.
def downloadData(path):
    for url in range(1,13):
        # construct file url for 2023 
        url = f"https://raw.githubusercontent.com/KareeemBeltagy/NYC_TCL_Taxi/refs/heads/master/data/yellow_tripdata_2022-{url:02}.parquet"
        file_path = path + os.path.basename(url)
        # Ensure the parent directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        if  os.path.exists(file_path):
            print(f"File {file_path} exist.")
            continue  # Skip the current iteration and go to the next file
        
        # get request to download the file
        with requests.get(url , stream=True)as r:
            r.raise_for_status()
            # write file in chunks
            print(f'Writting file to {file_path}')
            with open(file_path,"wb") as f:
                for chunk in r.iter_content(chunk_size=80000):
                    f.write(chunk)


# In[2]:


if args.download:
    print("*********************Running in download mode.*********************")
    path = os.path.join('.','data','')     
    downloadData(path)


# In[17]:


#download taxi_zone_lookup.csv
if args.download:
    url="https://raw.githubusercontent.com/KareeemBeltagy/NYC_TCL_Taxi/refs/heads/master/data/taxi_zone_lookup.csv"
    file_path = path + os.path.basename(url)
    with requests.get(url , stream=True)as r:
                r.raise_for_status()
                # write file in chunks
                with open(file_path,"wb") as f:
                    for chunk in r.iter_content(chunk_size=80000):
                        f.write(chunk)


# ### Moving Data from Local to HDFS

# In[29]:


# moving data files from local ot HDFS directory
if args.download:    
    local_dir = os.path.join('.','data','')  # "/home/itversity/itversity-material/NYC_TCL/data/"
    hdfs_dir =  os.path.join('/','NYC','raw','')# "/NYC/raw"
    # create parent directory on HDFS if not exist
    subprocess.run(["hdfs","dfs","-mkdir","-p",hdfs_dir])
    # loop over files and move from local to HDFS
    for file in range(1,13):
        file_path =local_dir + f"yellow_tripdata_2022-{file:02}.parquet"
        file_name = os.path.basename(f"{local_dir}yellow_tripdata_2022-{file:02}.parquet")
        hdfs_file_path = os.path.join(hdfs_dir,file_name)
        # check if file not found on local 
        if not os.path.exists(file_path):
            print(f"File {file_path} does not exist. Skipping this file.")
            continue  # Skip the current iteration and go to the next file
        print(f"moving file:{file_name}  to \n {hdfs_file_path} ")
        subprocess.run(["hdfs", "dfs", "-copyFromLocal", file_path, hdfs_file_path])
    # moving taxi_zone_lookup.csv to HDFS
    file_path =local_dir + f"taxi_zone_lookup.csv"
    file_name = os.path.basename(f"{local_dir}taxi_zone_lookup.csv")
    hdfs_file_path = os.path.join(hdfs_dir,file_name)
    subprocess.run(["hdfs", "dfs", "-copyFromLocal", file_path, hdfs_file_path])

# run in default if no argument is passed to download the data    
print("*********************Running in default mode.*********************")


# ### Creating Spark Session

# In[4]:


import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.functions import * # year,month,dayofyear,datediff,unix_timestamp,dayofmonth,to_date,date_format,round,col
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import StructType, StructField, LongType, TimestampType, DoubleType, StringType
from pyspark.sql.functions import broadcast


# In[6]:


spark = SparkSession.builder.appName("NYC_taxi").enableHiveSupport().getOrCreate()
spark


# ### Reading files and saving as one table

# In[9]:


df = spark.read.parquet("/NYC/raw/yellow_tripdata_2022*.parquet",compression="snappy" )
df_zone = spark.read.csv("/NYC/raw/taxi_zone_lookup.csv",header=True,inferSchema=True)
df.printSchema()
df_zone.printSchema()


# In[8]:


df.count()
# zone_lookup.count()


# ### Exploring and Cleaning data

# In[5]:


# function to count null values for each column containing nulls
def null_count(df):
    null_columns_count=[]
    row_count=df.count()
    for c in df.columns:
        null_rows=df.where(df[f"{c}"].isNull()).count()
        if null_rows > 0:
            temp= c, null_rows , (null_rows/row_count)*100
            null_columns_count.append(temp)
            
    return null_columns_count


# In[6]:


null_columns = null_count(df)
print(null_columns)


# ##### Substituting  records containing nulls for "passenger_count" column with AVG number of passenger per trip
# ##### Substituting  records containing nulls for "total_amount" column using a factor of "fare_per_distance"

# In[7]:


# Get average passenger count per trip  
avg_passenger_count = df.select("passenger_count").where((col("passenger_count").isNotNull()) & (col("passenger_count")!= 0) ).         select(ceil(avg("passenger_count"))).collect()[0][0]
# Calculate factor for fare per distance
avg_fare_trip = df.select(["trip_distance","total_amount"]).where(
            (col("trip_distance").isNotNull())|(col("trip_distance")!=0) | (col("total_amount").isNotNull())|(col("total_amount")!=0)) \
            .withColumn("fare_per_trip", col("total_amount")/col("trip_distance")).agg(avg("fare_per_trip")).collect()[0][0]

# handling nulls in columns passenger_count & total_amount
df=df.withColumn("passenger_count",                 when(
    (col("passenger_count").isNull())|(col("passenger_count")==0),lit(avg_passenger_count)).otherwise(col("passenger_count"))) \
    .withColumn("total_amount" ,
               when((col("total_amount").isNull()) | (col("total_amount")==0), col("trip_distance")*avg_fare_trip ).otherwise(col("total_amount")) )
df.show(15)


# In[8]:


# to focus on necessary features for the analysis some columns will be excluded 
# assuming trips with distance greater than 150 or less than 0.5 miles are outliers 
# assuming trips with total_amount greater than 350 or less than 
# considering only the absolute value for total_amount 
required_columns = ["tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance","RatecodeID",
                    "PULocationID","DOLocationID","payment_type","total_amount"]
df = df.withColumn("total_amount",abs(format_number(col("total_amount")+col("airport_fee"),2).cast("double"))).select(required_columns)         .withColumn("trip_distance" , col("trip_distance")*1.6)         .where(
                (col("trip_distance")<=150)&(col("trip_distance") >= 0.5)
                ) \
        .where(
                (col("total_amount") <=350)
                )
# df.count()


# ##### Chechk if any integer field contains negative values

# In[9]:


integr_fields =[field.name for field in df.schema if isinstance(field.dataType , (LongType, DoubleType))  ]
negative_values = {column: df.select(column).where(col(column) < 0).count() > 0 for column in integr_fields  }
negative_values=[column  for column , has_negative in negative_values.items() if has_negative]
negative_values


# In[10]:


# take the absolute for any negative value 
for column in negative_values:
    df = df.withColumn(column,abs(col(column)))


# ##### Creating columns year,month,day to partition data based on it 

# In[11]:


columns_arranged=["tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance",
                  "RatecodeID","PULocationID","DOLocationID","duration","payment_type","total_amount","year","month","day","day_name"]
df_final =df.withColumn("trip_distance",round(col("trip_distance"),2))         .withColumn("year", year("tpep_pickup_datetime").cast("string"))         .withColumn("month",month("tpep_pickup_datetime").cast("string"))         .withColumn("day",dayofmonth("tpep_pickup_datetime").cast("string"))         .withColumn("day_name",date_format("tpep_pickup_datetime" , "E"))         .withColumn("duration",(unix_timestamp("tpep_dropoff_datetime")-unix_timestamp("tpep_pickup_datetime"))/60)         .withColumn("duration", round("duration",2))             .select(columns_arranged).where(col("year")=='2022')  # filtring for year 2022


# In[12]:


df_final.count()


# ### save as dataframe partitioned by year, month

# In[14]:


path_to_table_trip = "/NYC/processed/Yellow_trip"  # change to the desired path 
# df_final.write.partitionBy("year","month").mode("overwrite").format("parquet").option("path",path_to_table).saveAsTable("Yellow_trip")
df_final.write.partitionBy("year","month").mode("overwrite").format("parquet").option("path",path_to_table_trip).save()


# In[15]:


path_to_table_zone = "/NYC/processed/zone_lookup"
df_zone.write.mode("overwrite").format("parquet").option("path",path_to_table_zone).save() 


# #### Reading data for further analysis

# In[16]:


# Reading table and presisting it to avoid recomputation
# df_trips= spark.read.table("Yellow_tripdata_cleaned").persist(StorageLevel.MEMORY_AND_DISK)
df_trips= spark.read.format("parquet").load(path_to_table_trip)


# In[17]:


# zone_lookup=spark.read.table("zone_lookup")
zone_lookup=spark.read.format("parquet").load(path_to_table_zone)


# In[18]:


# df_trips.count()
# zone_lookup.count()


# In[19]:


total_trips= spark.sparkContext.broadcast(df_trips.count())


# ##### Number of trips distribution for each month.

# In[20]:


# total_trips= df_trips.count()
trip_per_month = df_trips.groupby("year","month").agg(count("*").alias("trip/month")).orderBy(col("month").cast("int"))                 .withColumn("per%", round((col("trip/month")/total_trips.value)*100,2))
trip_per_month


# ##### Distribution of trips over the week.

# In[21]:


# creating a column to custom order the day of week 
day_order = when(col("day_name") == "Sat", 1)             .when(col("day_name") == "Sun", 2)             .when(col("day_name") == "Mon", 3)             .when(col("day_name") == "Tue", 4)             .when(col("day_name") == "Wed", 5)             .when(col("day_name") == "Thu", 6)             .when(col("day_name") == "Fri", 7) 
    

trips_per_day_of_week = df_trips.groupby("day_name").agg(count("*").alias("trip/day_of_week"))                             .withColumn("per%", round((col("trip/day_of_week")/total_trips.value)*100,2))                             .withColumn("day_order",day_order)                             .orderBy(col("day_order")).select("day_name","trip/day_of_week","per%")
trips_per_day_of_week


# #### join zone_lookup DF to include location info.

# In[22]:


# Broadcast join zon_lookup and include Borough for both pickup and drop off locations
trips = df_trips.join(broadcast(zone_lookup["LocationID","Borough","Zone"].alias("PU"))                       ,df_trips.PULocationID == col("PU.LocationID"),"left")         .join(broadcast(zone_lookup["LocationID","Borough","Zone"].alias("DO"))                       ,df_trips.DOLocationID == col("DO.LocationID"),"left")         .select(df_trips["*"],
                            col("PU.Borough").alias("PU_borough"),col("PU.Zone").alias("PU_Zone") 
                            ,col("DO.borough").alias("DO_borough"),col("DO.Zone").alias("DO_Zone")) \
        .persist(StorageLevel.MEMORY_AND_DISK)
# df_trips.unpersist()
trips


# #### Get number of trips per each Borough

# In[23]:


# the result shows that most of demand is in Manhattan boroug by 89% of total trips
trips_per_borough=trips.groupby("PU_borough")                         .agg(count("*").alias("Trips"))                         .withColumn("per%",round((col("Trips")/total_trips.value)*100,2))
trips_per_borough


# #### Investigating the trips distribution in manhattan

# In[24]:


total_trips_manhattan = spark.sparkContext.broadcast(trips.where(col("PU_borough")=="Manhattan").count())
total_trips_manhattan.value


# In[53]:


# get top ten pickup zones in manhattan
trips_in_manhattan = trips.select("PU_borough","PU_Zone")                             .where(col("PU_borough")=="Manhattan")                             .groupby("PU_Zone")                             .agg(count("*").alias("Trips"))                             .withColumn("per%",round((col("Trips")/total_trips_manhattan.value)*100,2))                             .orderBy(col("Trips").desc())                             .limit(10) 
trips_in_manhattan


# #### Preferred payment type by passengers

# In[34]:


# first create dictionary to map payment methods
dict_payment_type = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}
# selecting necessary columns and avoid unknown payment type
df_payment = trips.select("payment_type").where(col("payment_type")!=5)             .groupby("payment_type").agg(count("*").alias("Trips")) 

# crete column based on payment type dictionary
col_payment_type = None
for value,type in dict_payment_type.items():
    if col_payment_type is None:
        col_payment_type = when(col("payment_type")==value , type)
    else:
        col_payment_type = col_payment_type.when(col("payment_type")==value , type)
# Number of trips per payment type. 
df_payment = df_payment.withColumn("payment_type", col_payment_type)           .withColumn("per%", round((col("Trips") / total_trips.value) * 100, 2))           .select("payment_type", "Trips", "per%")           .orderBy(col("Trips").desc())
df_payment


# #### passenger count per trip 

# In[54]:


# the passenger count shows that most of the trips are done by one or two passenger
passenger_per_trip = trips.select("passenger_count").groupBy("passenger_count")             .agg(count("*").alias("Trips"))             .withColumn("per%", round((col("Trips") / total_trips.value) * 100, 2))             .orderBy(col("Trips").desc())

passenger_per_trip


# #### Categorize the trips by distance

# In[46]:



df_trip_by_distance = trips.select("trip_distance")                         .withColumn("range",
                                    when(col("trip_distance") <= 2, "Short trip <2 mile") \
                                   .when((col("trip_distance") > 2) & (col("trip_distance") <= 30) ,"Medium trip 2-30 miles") \
                                    .when( col("trip_distance") > 30 ,"Long trip >30 mile") \
                                    ) \
                        .groupBy("range").agg(count("*").alias("Trips")) \
                        .withColumn("per%", round((col("Trips")/total_trips.value)*100 )  ) \
                        .select( "range","Trips","per%")
df_trip_by_distance


# In[71]:




results_dir =  os.path.join('/','NYC','results','')
trip_per_month.repartition(1).write.options(header=True,encoding="UTF-8",delimiter=',').mode("overwrite").csv(results_dir+"Trip_per_month.csv")
trips_per_day_of_week.repartition(1).write.options(header=True,encoding="UTF-8",delimiter=',').mode("overwrite").csv(results_dir+"Trip_per_day_of_week.csv")
trips_per_borough.repartition(1).write.options(header=True,encoding="UTF-8",delimiter=',').mode("overwrite").csv(results_dir+"Trip_per_borough.csv")
trips_in_manhattan.repartition(1).write.options(header=True,encoding="UTF-8",delimiter=',').mode("overwrite").csv(results_dir+"Trips_in_manhattan.csv")
df_payment.repartition(1).write.options(header=True,encoding="UTF-8",delimiter=',').mode("overwrite").csv(results_dir+"df_payment.csv")
passenger_per_trip.repartition(1).write.options(header=True,encoding="UTF-8",delimiter=',').mode("overwrite").csv(results_dir+"passenger_per_trip.csv")
df_trip_by_distance.repartition(1).write.options(header=True,encoding="UTF-8",delimiter=',').mode("overwrite").csv(results_dir+"df_trip_by_distance.csv")
trips.repartition(1).write.options(header=True,encoding="UTF-8",delimiter=',').mode("overwrite").csv(results_dir+"Trips.csv")

