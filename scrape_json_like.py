from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
import json

# Create Spark session
spark = SparkSession.builder.appName("ExtractUserInfo").getOrCreate()

hdfs_file_path = 'hdfs://nodename1:9000/abdelbasser/Modbsolutions.com/temp_cleaned_file2.txt'

data = spark.sparkContext.textFile(hdfs_file_path)

# Define the schema for the DataFrame
schema = StringType()

# Create a DataFrame from the text data with the specified schema
df = spark.createDataFrame(data.map(lambda x: (x,)), ["value"])

# Define a UDF to parse the JSON and extract first_name and last_name
def extract_names(json_str):
    try:
        data = json.loads(json_str)
        first_name = data.get("first_name", "")
        last_name = data.get("last_name", "")
        email = data.get("email", "")
        gender = data.get("gender", "")
        city = data.get("city", "")
        state= data.get("state", "")
        return first_name, last_name , email , gender, city,state 
    except json.JSONDecodeError:
        return None, None , None

# Define the schema for the struct
struct_schema = StructType([
    StructField("first_name", StringType(),nullable=True),
    StructField("last_name", StringType(),nullable=True) ,
    StructField("email", StringType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True)
])

# Apply the UDF to the "value" column containing JSON data
extract_names_udf = spark.udf.register("extract_names", extract_names, struct_schema)
df = df.withColumn("names", extract_names_udf(col("value")))

# Extract "first_name" and "last_name" from the struct
df = df.withColumn("first_name", col("names.first_name"))
df = df.withColumn("last_name", col("names.last_name"))
df = df.withColumn("email", col("names.email"))
df = df.withColumn("gender", col("names.gender"))
df = df.withColumn("city", col("names.city"))
df = df.withColumn("state", col("names.state"))

# Drop the intermediate column "names" if needed
df_filtered = df.drop("names","value")

# Show the result
df_filtered.show(n=1000,truncate=False)
