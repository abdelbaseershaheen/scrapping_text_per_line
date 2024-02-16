import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Create a Spark session
spark = SparkSession.builder.appName("ListFiles").getOrCreate()

# HDFS directory path
hdfs_directory = "/abdelbasser/Pemiblanc.com_Pemiblanc_Credential_Stuffing_List/Pemiblanc_RF/Data/"

# List all files in the directory
folder_list = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
    .listStatus(spark._jvm.org.apache.hadoop.fs.Path(hdfs_directory))

# Extract file names
folder_names = [folder.getPath().getName() for folder in folder_list]

print(folder_names)

file_path = f"{hdfs_directory}/{str(folder_names[1])}"


file_list = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
    .listStatus(spark._jvm.org.apache.hadoop.fs.Path(file_path))

# Extract file names
file_names = [file.getPath().getName() for file in file_list]
print(file_names)

for folder_name in folder_names:
    file_path = f"{hdfs_directory}/{str(folder_name)}"
    file_list = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
    .listStatus(spark._jvm.org.apache.hadoop.fs.Path(file_path))
    file_names = [file.getPath().getName() for file in file_list]
    for file_name in file_names:
        nested_file_path = f"{file_path}/{str(file_name)}"  
        df2 = spark.read.format("text").load(nested_file_path)
        filtered_df = df2.filter(length("value") <= 1000)
        split_data = filtered_df.withColumn("new_value", split("value", ":"))
        result_data = split_data.drop("value")
        df3=result_data.selectExpr("size(new_value) as size","new_value")
        Datafram1=df3.where("size  = 2" )
        Datafram1 = Datafram1.withColumn("email", split_data["new_value"].getItem(0))
        Datafram1 = Datafram1.withColumn("password", split_data["new_value"].getItem(1))
        columns_to_drop = ["size", "new_value", "password1","unknown0","unknown03","hash_password1" , "password2" ,"password3" ,"salt_password1","hash_password1","salt_password2","misc1"]
        result_data = Datafram1.drop(*columns_to_drop)
        result_data.write.mode("append").saveAsTable("leaks_sql120.PemiblanccomPemiblancCredentialStuffingList")
        
    
#    for file_name in file_lists:
#        print(file_name)