import boto3
import time 
import os

def start_crawler(crawler_name):
    print(f"Starting crawler: {crawler_name}")
    glue_client.start_crawler(Name=crawler_name)

def start_glue_job(job_name):
    print(f"Starting Glue job: {job_name}")
    response = glue_client.start_job_run(JobName=job_name)
    return response['JobRunId']

def run_step(step):
    running_jobs = {}
    for resource_name, resource_type in step.items():
        if resource_type == 'crawler':
            start_crawler(resource_name)
        elif resource_type == 'glue_job':
            job_run_id = start_glue_job(resource_name)
            running_jobs[resource_name] = job_run_id
        else:
            print(f"Unsupported resource type: {resource_type}")
    while running_jobs or step:
        for resource_name, resource_type in list(step.items()):
            if resource_type == 'crawler':
                response = glue_client.get_crawler(Name=resource_name)
                state = response['Crawler']['State']
                if 'LastCrawl' not in response['Crawler']:
                    last_crawl_status = 'FIRST_RUN'
                else:
                    last_crawl_status = response['Crawler']['LastCrawl']['Status']
                if state == 'READY':
                    if last_crawl_status == 'SUCCEEDED':
                        print(f"Crawler {resource_name} has completed successfully.")
                    elif last_crawl_status in ['FAILED', 'CANCELLED']:
                        raise Exception(f"Crawler {resource_name} has failed or cancelled.")
                    step.pop(resource_name) 
                else:
                    print(f"Crawler {resource_name} is still running...")
            elif resource_type == 'glue_job':
                job_run_id = running_jobs[resource_name]
                response = glue_client.get_job_run(JobName=resource_name, RunId=job_run_id)
                state = response['JobRun']['JobRunState']
                if state in ['SUCCEEDED']:
                    print(f"Glue job {resource_name} has completed with status: {state}.")
                    step.pop(resource_name)  
                    running_jobs.pop(resource_name)
                elif state in ['FAILED', 'STOPPED']:
                    raise Exception(f"Glue job {resource_name} has failed or stopped - state {state}.")
                else:
                    print(f"Glue job {resource_name} is still running... Current state: {state}")
        time.sleep(10)  

if __name__ == "__main__":
    STEPS = [
        {'stocks_data_crawler': 'crawler'},
        {'assignment_glue_job_yuval_dror': 'glue_job'},
        {'avg_daily_returns_crawler': 'crawler', 'highest_worth_crawler': 'crawler',
        'annualized_volatility_crawler': 'crawler', '30_days_returns_crawler': 'crawler'}
    ]
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        glue_client = boto3.client(
            'glue',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    else:
        glue_client = boto3.client('glue')
    for step in STEPS:
        run_step(step)