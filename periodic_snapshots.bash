snapshot_path="/path/to/snapshots"

retention_days=7

cutoff_date=$(date -d "$retention_days days ago" +%Y%m%d%H%M%S)

snapshots=$(hdfs dfs -lsSnapshot "$snapshot_path" | awk '{print $6" "$7}' | grep -E "^[0-9]+" | sed 's/\([0-9]\{4\}\)\([0-9]\{10\}\)/\1 \2/g')
while read -r snapshot_date; do
    if [[ $snapshot_date < $cutoff_date ]]; then
        hdfs dfs -deleteSnapshot "$snapshot_path" "$snapshot_date"
        echo "Deleted snapshot $snapshot_date"
    fi
done <<< "$snapshots"
