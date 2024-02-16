from pyspark.sql import SparkSession
import re

# Create a Spark session
spark = SparkSession.builder.appName("ExtractUserInfo").getOrCreate()

# Replace 'your_file_path' with the HDFS path to your file
hdfs_file_path = 'hdfs://nodename1:9000/abdelbasser/Youporn.com/YouPorn.txt'

# Read the file from HDFS as an RDD
data = spark.sparkContext.textFile(hdfs_file_path)

# Define a function to process each line and filter relevant information
def filter_and_extract(line):
    # Check if the line contains relevant information
    if line.startswith("username=") or line.startswith("email=") or line.startswith("password=") \
            or line.startswith("country=") or line.startswith("msisdn=") or line.startswith("dob="):
        return True
    return False

# Filter relevant lines
relevant_lines = data.filter(filter_and_extract)

# Define a function to process relevant lines and extract required fields
def extract_fields(line):
    # Split the line based on ":" and extract the second part
    parts = line.split("=", 1)
    # Return the extracted field
    return parts[1].strip()

# Extract Username, Pass, E-mail, Phone, and Address
usernames = relevant_lines.filter(lambda line: line.startswith("username=")).map(extract_fields)
passwords = relevant_lines.filter(lambda line: line.startswith("email=")).map(extract_fields)
emails = relevant_lines.filter(lambda line: line.startswith("password=")).map(extract_fields)
phones = relevant_lines.filter(lambda line: line.startswith("country=")).map(extract_fields)
addresses = relevant_lines.filter(lambda line: line.startswith("msisdn=")).map(extract_fields)
dob = relevant_lines.filter(lambda line: line.startswith("dob=")).map(extract_fields)

# Combine the extracted information into a DataFrame
df = spark.createDataFrame(zip(usernames.collect(), passwords.collect(), emails.collect(),
                               phones.collect(), addresses.collect(), dob.collect()), ["username", "email", "password", "country", "phone_number", "birth_date"])

# Show the DataFrame
filter_df = df.drop_duplicates()
filter_df.show()

filter_df.write.mode("overwrite").saveAsTable("leaks_restore.Youporncom")