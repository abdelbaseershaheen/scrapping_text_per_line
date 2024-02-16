%%bash

input_file="hdfs://nodename1:9000/abdelbasser/NetEase163.com126.com/NetEase_(126.com&163.com)_plain+MD5_October_2015_.txt"
temp_file="hdfs://nodename1:9000/abdelbasser/NetEase163.com126.com/temp_cleaned_file.txt"
temp_file2="hdfs://nodename1:9000/abdelbasser/NetEase163.com126.com/temp_cleaned_file2.txt"

# Remove null values using sed
hdfs dfs -text "$input_file" | sed 's/\x0//g' | hdfs dfs -put - "$temp_file"

# Remove invalid characters using tr
hdfs dfs -text "$temp_file" | tr -cd '[:print:]\t\n' | hdfs dfs -put - "$temp_file2"

# Remove the temporary file
hdfs dfs -rm "$temp_file"

echo "Cleanup complete for: $temp_file2"

