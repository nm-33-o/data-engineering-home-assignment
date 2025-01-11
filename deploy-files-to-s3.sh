#!/bin/bash
# Exit on any error
set -e

# Load environment variables from the .env file
if [ -f .env ]; then
  source .env
else
  echo "Error: .env file not found!"
  exit 1
fi

# Check if necessary variables are defined
if [ -z "$GLUE_BUCKET_NAME" ] || [ -z "$AWS_DEFAULT_REGION" ] ; then
  echo "Error: Ensure GLUE_BUCKET_NAME and AWS_DEFAULT_REGION are defined in your .env file."
  exit 1
fi

# Variables
bucket_name=$GLUE_BUCKET_NAME
region=$AWS_DEFAULT_REGION
workspace_path=$(pwd)

# Check if the bucket exists
echo "Checking if bucket $bucket_name exists in region $region..."
if aws s3api head-bucket --bucket "$bucket_name" 2>/dev/null; then
  echo "Bucket $bucket_name already exists."
else
  echo "Bucket $bucket_name does not exist. Creating..."
  aws s3api create-bucket --bucket "$bucket_name" --region "$region" --create-bucket-configuration LocationConstraint="$region"
  echo "Bucket $bucket_name created successfully in region $region."
fi

# Check if required files exist in the workspace
if [ ! -f "$workspace_path/stocks_data.csv" ] || [ ! -f "$workspace_path/glue-job-ori.py" ]; then
  echo "Error: Required files (stocks_data.csv, glue-job-ori.py) not found in the workspace."
  exit 1
fi

# Upload files to S3 bucket
echo "Uploading files to S3 bucket: $bucket_name"
aws s3 cp "$workspace_path/stocks_data.csv" "s3://$bucket_name/stocks_data.csv"
aws s3 cp "$workspace_path/glue-job-ori.py" "s3://$bucket_name/glue-job-ori.py"

# Success message
echo "Files uploaded successfully to bucket: $bucket_name in region: $region"
