import boto3
import json
import time

glue = boto3.client('glue')
sns = boto3.client('sns')

GLUE_JOB_NAME = "nyc_taxi_etl_job"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:451702994321:glue-failure-topic"
RETRY_LIMIT = 1

# Track number of retries
retries_done = 0

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    try:
        # Extract actual message payload from SNS
        sns_message_raw = event['Records'][0]['Sns']['Message']
        print("SNS raw message:", sns_message_raw)

        # Parse SNS message string safely
        try:
            sns_message = json.loads(sns_message_raw)
        except json.JSONDecodeError:
            # If not a dict, treat message as simple string
            sns_message = {"message": sns_message_raw}

        alarm_name = sns_message.get("AlarmName", "Unknown")
        reason = sns_message.get("AlarmDescription", sns_message.get("Reason", "Unknown"))

        print(f"Triggered by alarm: {alarm_name}")
        print(f"Detected failure reason: {reason}")

        # Check last job status
        job_runs = glue.get_job_runs(JobName=GLUE_JOB_NAME, MaxResults=1)
        last_run = job_runs['JobRuns'][0]
        job_state = last_run['JobRunState']
        print("Last job state:", job_state)

        if job_state == "RUNNING":
            print("Glue job is still running. Skipping retry.")
            return

        # Retry only once
        global retries_done
        if retries_done >= RETRY_LIMIT:
            print("Max retries reached. Not retrying again.")
            return

        print("Waiting 60 seconds to ensure job status is final...")
        time.sleep(60)

        retry = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={"--triggered_by": "LambdaRetry"}
        )
        retry_id = retry["JobRunId"]
        retries_done += 1
        print(f"Started retry job: {retry_id}")

        # Wait for job to finish
        final_state = poll_job_status(retry_id)

        if final_state == "SUCCEEDED":
            message = f"Retried Glue job succeeded.\nJobRunId: {retry_id}, Final Status: {final_state}"
            subject = "Retried Glue Job is Successful"
        else:
            message = (
                f"Retried Glue job also failed. Manual intervention required.\n"
                f"JobRunId: {retry_id}, Final Status: {final_state}"
            )
            subject = "Retried Glue Job Failed"

        # Send final SNS update
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )

    except Exception as e:
        error_msg = f"Lambda failed to retry Glue job due to: {str(e)}"
        print(error_msg)
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Lambda Error During Glue Job Retry",
            Message=error_msg
        )

# Polls status of retried Glue job
def poll_job_status(job_run_id, timeout=600, poll_interval=20):
    elapsed = 0
    while elapsed < timeout:
        response = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_run_id)
        state = response['JobRun']['JobRunState']
        print(f"Current retry run state: {state}")
        if state in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"]:
            return state
        time.sleep(poll_interval)
        elapsed += poll_interval
    return "TIMEOUT"
