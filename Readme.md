# Serverless Threat Intel Pipeline

## Description
This project implements a serverless threat-intelligence ingestion backend on AWS. When JSON or CSV files containing threat indicators are uploaded to S3, a Lambda function automatically processes the data, normalizes the indicators, and stores them in DynamoDB. A summary of each ingestion is published to SNS for alerting, and custom metrics are emitted to CloudWatch for monitoring and visibility into pipeline activity.

## Deployment Steps
1. From project root:
   ```bash
   sam build
   sam deploy --guided
