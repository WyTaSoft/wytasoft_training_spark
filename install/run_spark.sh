#!/bin/bash

# Exit script if any command fails
set -e

# Variables for the Spark job
SPARK_MASTER=${SPARK_MASTER:-local[*]}  # Default to local[*] if not set
APP_FILE=${1:-src/main/py/com/wts/training/app/job/MainDriver.py}  # Corrected file path
APP_ARGS=${2:-"prd /wytasoft_training_academy/resources/application.conf"}  # Default arguments to pass to the application

# Print the environment information
echo "Running Spark job with the following parameters:"
echo "Spark Master: $SPARK_MASTER"
echo "Application File: $APP_FILE"
echo "Application Arguments: $APP_ARGS"

# Run the Spark job
spark-submit \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
  --master $SPARK_MASTER \
  $APP_FILE \
  $APP_ARGS
