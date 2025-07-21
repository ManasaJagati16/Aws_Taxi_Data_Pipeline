\# AWS Taxi Data Pipeline – Setup Guide



This guide will help you deploy and configure the serverless AWS Taxi Data Pipeline from this repository.



---



\## Prerequisites



\- AWS account with permissions to create and manage Lambda, Glue, S3, CloudWatch, SNS, and EventBridge resources.

\- IAM roles for Glue jobs and Lambda functions (see \[IAM Policies](../IAM%20Policies/))

\- (Optional) AWS CLI and Python 3.x for local interactions



---



\## Setup Steps



1\. \*\*Clone the Repository\*\*



&nbsp;   ```bash

&nbsp;   git clone https://github.com/your-username/aws-taxi-data-pipeline.git

&nbsp;   cd aws-taxi-data-pipeline

&nbsp;   ```



2\. \*\*Upload Lambda Scripts\*\*



&nbsp;   - Go to the AWS Lambda Console.

&nbsp;   - Create two Lambda functions:

&nbsp;       - `Taxi\_data\_pipeline\_lambda`

&nbsp;       - `retry\_glue\_job\_for\_exception\_lambda`

&nbsp;   - Copy and paste the code from `lambda/Taxi\_data\_pipeline\_lambda.py` and `lambda/retry\_glue\_job\_for\_exception\_lambda.py` into the respective functions, or upload as .zip if required.

&nbsp;   - Assign the correct IAM role to each function (see \[IAM Policies](../IAM%20Policies/)).



3\. \*\*Create and Configure Glue Jobs and Crawlers\*\*



&nbsp;   - In the AWS Glue Console:

&nbsp;       - Create a \*\*Glue Job\*\* and upload `glue/glue\_job\_script.py` as the script.

&nbsp;       - Assign the Glue job role (see IAM policies).

&nbsp;   - Create two Glue Crawlers:

&nbsp;       - \*\*Raw Glue Crawler:\*\* Use the config in `glue/glue\_crawler\_configs/Raw Glue Crawler Config.txt`.

&nbsp;       - \*\*Processed Glue Crawler:\*\* Use the config in `glue/glue\_crawler\_configs/Processed Glue Crawler Config.txt`.

&nbsp;   - Assign the correct IAM role to each crawler.



4\. \*\*Set Up S3 Buckets\*\*



&nbsp;   - Create S3 buckets and folders for raw input and processed data.

&nbsp;       - Example:  

&nbsp;           - `s3://your-bucket/nyc-taxi/raw/<YYYY>/<MM>/<DD>/`

&nbsp;           - `s3://your-bucket/nyc-taxi/processed/<YYYY>/<MM>/<DD>/`

&nbsp;   - Upload your sample or real data to the raw input folder.



5\. \*\*Configure CloudWatch Alarms and SNS\*\*



&nbsp;   - In the AWS CloudWatch Console:

&nbsp;       - Create alarms to monitor Glue job failures.

&nbsp;   - In SNS:

&nbsp;       - Create an SNS topic and subscribe your email (or SMS).

&nbsp;       - Configure CloudWatch Alarms to publish to this SNS topic.

&nbsp;   - Set the alarm to trigger the `retry\_glue\_job\_for\_exception\_lambda` when needed.



6\. \*\*Set Up EventBridge Scheduler\*\*



&nbsp;   - In the EventBridge Console, create a new rule (cron or rate expression) to trigger the `Taxi\_data\_pipeline\_lambda` every day or as needed.



7\. \*\*Test the Pipeline\*\*



&nbsp;   - Place a sample file in the S3 raw input bucket.

&nbsp;   - Trigger the pipeline manually or wait for the EventBridge schedule.

&nbsp;   - Monitor Lambda, Glue, and CloudWatch logs and verify SNS notifications for both success and failure cases.



---



\## Further References



\- \[Main README](../README.md)

\- \[IAM Policies](../IAM%20Policies/)

\- \[Troubleshooting Guide](./troubleshooting.md)



---



\*For questions, open an issue or connect on \[LinkedIn](https://www.linkedin.com/in/manasa-goud-j-800589135/).\*



