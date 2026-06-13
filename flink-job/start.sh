#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "Waiting for Flink JobManager to be ready..."
until wget -q -O - http://flink-jobmanager:8081/overview >/dev/null 2>&1; do
  echo "Flink JobManager is not reachable yet. Retrying in 2 seconds..."
  sleep 2
done

echo "Flink JobManager is ready!"
echo "Submitting Flink job: com.pipeline.FeaturePipeline..."

# Execute flink run in the foreground to keep the container running
exec flink run -c com.pipeline.FeaturePipeline -m flink-jobmanager:8081 /app/flink-job.jar
