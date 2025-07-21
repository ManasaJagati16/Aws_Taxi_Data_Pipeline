import boto3
import time
from datetime import datetime
glue = boto3.client('glue')

RAW_CRAWLER = "nyc-taxi-raw-crawler"
PROCESSED_CRAWLER = "nyc_taxi_processed_crawler"
GLUE_JOB_NAME = "nyc_taxi_etl_job"
today_date = datetime.today().strftime('%Y-%m-%d')
input_s3_path = f"s3://nyc-taxi-data-lake-manasa/raw/nyc_taxi/{today_str}/"
output_s3_path = f"s3://nyc-taxi-data-lake-manasa/processed/nyc_taxi/{today_str}/"

# function to run crawlers
def wait_for_crawler(crawler_name,max_wait=300):
    print(f"Waiting for crawler {crawler_name} to finish...")
    start_time = time.time()
    while True:
        status = glue.get_crawler(Name=crawler_name)['Crawler']['State']
        print(f"{crawler_name} status: {status}")
        if status == 'READY':
            break
        if time.time() - start_time > max_wait:
            print(f"Max wait time exceeded for {crawler_name}, exiting.")
            return False
        time.sleep(45)
    return True

# Function to run Glue job
def wait_for_glue_job(job_run_id):
    print(f"Waiting for Glue job run {job_run_id} to finish...")
    while True:
        response = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_run_id)
        state = response['JobRun']['JobRunState']
        print(f"Job status: {state}")
        if state in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
            return state
        time.sleep(100)

# Pipeline Function

def lambda_handler(event, context):
    # 1. Start raw crawler
    print("Lambda started at:", time.ctime())
    print("Event:", event)
    print("RequestId:", context.aws_request_id)

    glue.update_crawler(
    Name=RAW_CRAWLER,
    Targets={'S3Targets': [{'Path': input_s3_path}]}
    )

    try:
        glue.start_crawler(Name=RAW_CRAWLER)
        print("Started raw crawler...")
    except glue.exceptions.CrawlerRunningException:
        print(f"Raw crawler '{RAW_CRAWLER}' is already running.")
    except Exception as e:
        print(f"Error starting raw crawler: {e}")
        sys.exit(1)

    wait_for_crawler(RAW_CRAWLER)

    # 2. Start Glue job
    try:
        print("Starting Glue job...")
        job_response = glue.start_job_run(JobName=GLUE_JOB_NAME)
        job_run_id = job_response['JobRunId']
        final_status = wait_for_glue_job(job_run_id)
    except Exception as e:
        print(f"Error starting or monitoring Glue job: {e}")
        sys.exit(1)

    # 3. If Glue job succeeded, run processed crawler
    if final_status == "SUCCEEDED":

        glue.update_crawler(
        Name=PROCESSED_CRAWLER,
        Targets={'S3Targets': [{'Path': output_s3_path}]},
        TablePrefix=f'processed_nyc_taxi_{today_date}'
        )
        try:
            print("Glue job succeeded. Starting processed crawler...")
            glue.start_crawler(Name=PROCESSED_CRAWLER)
        except glue.exceptions.CrawlerRunningException:
            print(f"Processed crawler '{PROCESSED_CRAWLER}' is already running.")
        except Exception as e:
            print(f"Error starting processed crawler: {e}")
            sys.exit(1)

        wait_for_crawler(PROCESSED_CRAWLER)
        print("Processed crawler completed.")
    else:
        print(f"Glue job failed with status: {final_status}")