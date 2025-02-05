# Optionally reset the entire file
if [ "$1" = "reset" ]; then
    if [ -f "tpch.duckdb" ]; then
        rm tpch.duckdb
        echo "tpch.duckdb has been reset"

    fi
fi

duckdb tpch.duckdb < generate_tpch.sql
uv run sqlmesh create_external_models
yes | uv run sqlmesh plan prod

# Wait until the next 5 min intercal
current_time=$(date +%s)
seconds_since_interval=$((current_time % 300))
seconds_to_wait=$((300 - seconds_since_interval))

minutes=$((seconds_to_wait / 60))
seconds=$((seconds_to_wait % 60))
echo "Waiting for $minutes minutes and $seconds seconds until performing the initial run to populate all incremental models..."

total_seconds=$((seconds_to_wait + 1))
while [ $total_seconds -gt 0 ]; do
    printf "\rTime remaining: %02d:%02d" $((total_seconds/60)) $((total_seconds%60))
    sleep 1
    total_seconds=$((total_seconds - 1))
done
echo 

uv run sqlmesh run prod