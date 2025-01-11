#!/bin/bash
# shellcheck disable=SC2006

# Exit immediately if a command exits with a non-zero status
set -e

# Load environment variables from .env file
if [ -f .env ]; then
  # Using source
  source .env
fi

# Now you can use the environment variables
echo "AWS_ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"
echo "AWS_SECRET_ACCESS_KEY: $AWS_SECRET_ACCESS_KEY"
echo "STACK_NAME: $STACK_NAME"
echo "AWS_DEFAULT_REGION: $AWS_DEFAULT_REGION"

# Set AWS credentials as environment variables
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION

# Notify the user that the stack update process is starting
printf '\nUpdating stack...\n\n'

# Define the CloudFormation template file name and file path
stack_yml="stack.yml"
stack=$STACK_NAME
region=$AWS_DEFAULT_REGION

# Display the stack name and region being processed
echo "Stack: $stack"
echo "Region: $region"

# Check if the stack exists by attempting to describe it
# If the stack doesn't exist, the command will return an error, so we use '|| echo -1' to handle it
stack_exists=`aws cloudformation describe-stacks --stack-name "$stack" --region "$region" || echo -1`

# If the stack does not exist (indicated by -1), create a new one
if test "$stack_exists" = "-1"
then
    echo "Creating a new stack: $stack"
    aws cloudformation create-stack --stack-name "$stack" \
        --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
        --template-body file://"$stack_yml" \
        --region "$region"

    # Wait for the stack creation to complete
    echo "Waiting for stack creation to complete: $stack"
    aws cloudformation wait stack-create-complete --stack-name "$stack" --region "$region"
    status=$?
else
    # If the stack exists, update it with the new template
    echo "Updating the stack: $stack"
    aws cloudformation update-stack --stack-name "$stack" \
        --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
        --template-body file://"$stack_yml" \
        --region "$region"

    # Wait for the stack update to complete
    echo "Waiting for stack update to complete: $stack"
    aws cloudformation wait stack-update-complete --stack-name "$stack" --region "$region"
    status=$?
fi

# Check the status of the operation (creation or update)
if [[ $status -ne 0 ]]; then
    # If the command failed, output an error message and exit with the same status code
    echo "$stack operation failed with AWS error code: $status."
    exit $status
else
    # If the command succeeded, notify the user
    echo "$stack operation completed successfully."
fi