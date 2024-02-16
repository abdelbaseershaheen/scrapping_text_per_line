%%bash

input_directory="hdfs://nodename1:9000/abdelbasser/Id.zing.vn/RaidForums_VNG/"
output_directory="hdfs://nodename1:9000/abdelbasser/Id.zing.vn/RaidForums_VNG/cleaned_files/"

# Create the output directory if it doesn't exist
hdfs dfs -mkdir -p "$output_directory"

for i in {10..43}; do
    input_file="$input_directory/0000{$i}_0.csv"
    temp_file="$output_directory/temp_cleaned_file$i.csv"
    cleaned_file="$output_directory/cleaned_file$i.csv"

    # Remove null values using sed
    hdfs dfs -text "$input_file" | sed 's/\x0//g' | hdfs dfs -put - "$temp_file"

    # Remove invalid characters using tr
    hdfs dfs -text "$temp_file" | tr -cd '[:print:]\t\n' | hdfs dfs -put - "$cleaned_file"

    # Remove the temporary file
    hdfs dfs -rm "$temp_file"

    echo "Cleanup complete for: $cleaned_file"
done