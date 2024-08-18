#!/bin/bash
# shellcheck disable=SC2006

set -e
printf '\nUploading Templates...\n\n'
aws s3 cp cloudformation/ s3://vi-cf-templates/vi-integrations/ --recursive

printf '\nUpdating stack...\n\n'

stack_yml="stack.yml"

echo "stack: $stack"

stack_exists=`aws cloudformation describe-stacks --stack-name "$stack" || echo -1`

if test "$stack_exists" = "-1"
then
    echo "Creating a new stack: $stack"
    aws cloudformation create-stack --stack-name "$stack" \
        --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
        --template-body file://"$stack_yml"
    echo "Waiting for stack creation to complete: $stack"
    aws cloudformation wait stack-create-complete --stack-name "$stack"
    status=$?
else
    echo "Updating the stack: $stack"
    aws cloudformation update-stack --stack-name "$stack" \
        --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
        --template-body file://"$stack_yml"
    echo "Waiting for stack update to complete: $stack"
    aws cloudformation wait stack-update-complete --stack-name "$stack"
    status=$?
fi

if [[ $status -ne 0 ]]; then
    # Waiter encountered a failure state.
    echo "$stack update failed. AWS error code is $status."
    exit $status
else
    echo "$stack update completed successfully."
fi
