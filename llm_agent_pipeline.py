import argparse
import boto3
import json
import os
import time
from uuid import uuid4
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
import pytz
import random
from botocore.config import Config
import pandas as pd
import re


def get_reports_directory(user_id):
    """Helper function to create and return reports directory path."""
    base_name = "-".join(user_id.split("-")[:-2])  # Remove timestamp and random string
    reports_dir = f"~/reports/{base_name}/{user_id}"
    parent_dir = f"~/reports/{base_name}"
    os.makedirs(parent_dir, exist_ok=True)
    os.makedirs(reports_dir, exist_ok=True)
    return reports_dir


def unpack_question(question):
    """Helper function to consistently unpack question tuple/list."""
    if isinstance(question, (tuple, list)) and len(question) == 2:
        uuid, question_text = question
    else:
        uuid, question_text = None, question

    question_value = (
        question_text[1]
        if isinstance(question_text, list) and len(question_text) > 1
        else question_text
    )
    return uuid, question_text, question_value


def classify_error_and_save(
    error_msg, question_value, uuid, plan, time_tracker, reports_dir, job_id
):
    """
    Unified function to classify errors and save to appropriate subdirectory only.
    Returns the classification (status).
    """
    # Normalize error message for classification
    if isinstance(error_msg, list):
        error_msg = "; ".join(str(e) for e in error_msg)
    else:
        error_msg = str(error_msg)

    error_lower = error_msg.lower()

    # Classify error type
    if "timed out" in error_lower or "timeout" in error_lower:
        subdir = "timeout"
    elif "no matching concept" in error_lower or "no matching concepts" in error_lower:
        subdir = "no_matching_concept"
    elif (
        "unable to be answered due to an error" in error_lower
        or "list index out of range" in error_lower
    ):
        subdir = "failed"
    else:
        subdir = "failed"  # Default to failed for unknown errors

    # Create subdirectory and save file
    subdir_path = os.path.join(reports_dir, subdir)
    os.makedirs(subdir_path, exist_ok=True)

    error_record = {
        "question": question_value,
        "uuid": uuid,
        "error": error_msg,
        "plan": plan,
        "times": time_tracker,
        "status": subdir,
    }

    # Only write to subdirectory (not main directory)
    with open(os.path.join(subdir_path, f"{job_id}.json"), "w") as f:
        json.dump(error_record, f, indent=2)

    print(f"  * Error classified as '{subdir}' and saved to {subdir}/ subdirectory")
    return subdir


def invoke_researcher_agent(payload):
    """
    Invokes an AWS Lambda function with the given ARN and payload.
    Retries are disabled (only 1 attempt).
    """
    # Create a Config with max_attempts=1 to disable retries
    no_retry_config = Config(
        region_name="us-east-2", retries={"max_attempts": 1}, read_timeout=180
    )
    lambda_client = boto3.client("lambda", config=no_retry_config)
    if isinstance(payload, dict):
        payload = json.dumps(payload)
    if "Payload" in response:
        return json.loads(response["Payload"].read().decode("utf-8"))
    return response


def process_step(
    payload, step_name, step_num, time_tracker, plan, user_id, job_id, question
):
    # Unpack uuid and question_text using helper
    uuid, question_text, question_value = unpack_question(question)

    payload["query"]["currentStep"] = step_name
    s = time.time()
    response = invoke_researcher_agent(payload)
    e = time.time()

    # Track time for this step
    time_tracker[f"step_{step_num}"] = e - s

    # Handle error case
    if "errorMessage" in response:
        print(f"{job_id} errored out at step {step_name}!")

        # Create directory structure using helper
        reports_dir = get_reports_directory(user_id)

        error_msg = response["errorMessage"]
        classify_error_and_save(
            error_msg, question_value, uuid, plan, time_tracker, reports_dir, job_id
        )
        return False, error_msg

    # Extract and store response data
    response_data = response["body"]["response"]
    plan[step_name] = response_data

    return True, response_data


def run_analytics(question, user_id):  # prompt_versions="v0"
    # Unpack uuid and question_text using helper
    uuid, question_text, question_value = unpack_question(question)

    job_id = None
    client = OpenAI()
    case_title = client.responses.create(
        model="gpt-4.1",
        input=f'Generate a folder name for the research topic: "{question_text}". Only return the folder name in plain text without any additional text. All lowercase with underlines.',
    ).output_text.strip()

    job_id = generate_unique_job_id(question, user_id)

    time_tracker = {}
    plan = {}

    # Main steps execution
    s = time.time()
    success, query_validation = process_step(
        payload, "QUERY_VALIDATION", 1, time_tracker, plan, user_id, job_id, question
    )
    if not success:
        return job_id

    success, data_extraction = process_step(
        payload, "DATA_EXTRACTION", 2, time_tracker, plan, user_id, job_id, question
    )
    if not success:
        return job_id

    success, stats_analysis = process_step(
        payload, "STATS_ANALYSIS", 3, time_tracker, plan, user_id, job_id, question
    )
    if not success:
        return job_id

    success, output_plan = process_step(
        payload, "VISUALIZATION", 4, time_tracker, plan, user_id, job_id, question
    )
    if not success:
        return job_id

    # perform analysis
    payload["query"]["currentStep"] = "RESULTS"
    payload["query"]["runJob"] = True
    response = invoke_researcher_agent(payload)
    if response.get("statusCode", 400) != 200:
        raise Exception("Error submitting job to ECS!")
    else:
        print("Job submitted successfully! Job ID:", job_id)

    # wait for the job to complete by waiting listening to the job_id in s3
    object_key = f"reports/{user_id}/{job_id}.json"

    # Create S3 client
    s3_client = boto3.client("s3", region_name="us-east-2")

    # Create reports directory using helper
    reports_dir = get_reports_directory(user_id)

    # Poll for the file with a 1-hour time limit
    print(f"  - Polling for results at {job_id}.json...")
    start_time = datetime.now()
    end_time = start_time + timedelta(hours=1)
    wait_time = 1  # seconds between attempts

    while datetime.now() < end_time:
        try:
            # Check if the file exists in S3
            s3_client.head_object(Bucket=bucket_name, Key=object_key)
            elapsed_time = datetime.now() - start_time
            elapsed_time = elapsed_time.total_seconds()
            print(f"  - Results found after {elapsed_time:.1f} seconds!")
            time_tracker["job"] = elapsed_time

            # Download the file
            download_path = f"{reports_dir}/{job_id}.json"
            s3_client.download_file(bucket_name, object_key, download_path)
            report_json = json.load(open(download_path, "r"))

            # Check if this is a failed case (report is a list with error message)
            if (
                isinstance(report_json, list)
                and len(report_json) > 1
                and "unable to be answered due to an error"
                in str(report_json[1]).lower()
            ):
                # This is a failed case - use unified error handler
                error_msg = report_json[1]
                classify_error_and_save(
                    error_msg,
                    question_value,
                    uuid,
                    plan,
                    time_tracker,
                    reports_dir,
                    job_id,
                )
                # Remove the file from main directory since we only want it in subdirectory
                if os.path.exists(download_path):
                    os.remove(download_path)
            else:
                # This is a successful case - write to main directory only
                final_report = {
                    "question": question_value,
                    "uuid": uuid,
                    "report": report_json,
                    "plan": plan,
                    "times": time_tracker,
                    "status": "success",
                }
                if "image" in plan:
                    final_report["image"] = plan["image"]
                elif isinstance(report_json, dict) and "image" in report_json:
                    final_report["image"] = report_json["image"]
                with open(download_path, "w") as f:
                    json.dump(final_report, f, indent=2)
                print(f"  * Results downloaded successfully to {download_path}")

            return job_id

        except Exception:
            time.sleep(wait_time)

    # If we reach here, the job timed out after 1 hour
    print(f"❌ Job {job_id} timed out after 1 hr")
    if job_id:
        # Save timeout error using unified error handler (only to subdirectory)
        reports_dir = get_reports_directory(user_id)
        error_msg = "Job timed out after 1 hour"
        classify_error_and_save(
            error_msg, question_value, uuid, plan, time_tracker, reports_dir, job_id
        )
    return job_id  # Return job_id even on timeout

    # Exception block removed; network errors are only logged in the polling loop above.


def batch_run_analytics(
    questions, user_id, num_concurrent=10  # prompt_versions="v0",
):  # Added num_concurrent parameter
    job_ids = []
    # Fix: Get the correct reports_dir for this batch
    reports_dir = get_reports_directory(user_id)
    print(f"[INFO] Output directory for this job: {reports_dir}")
    successful_count = 0
    failed_count = 0

    print(min(len(questions), num_concurrent), "concurrent jobs will be run.")
    with ThreadPoolExecutor(
        max_workers=min(len(questions), num_concurrent)
    ) as executor:
        # Remove redundant import - random is already imported at top
        futures = {}
        for uuid_and_question in questions:
            print(f"Submitting job for question: {uuid_and_question}")
            # Add a random lag between 5 and 10 seconds before submitting each job
            time.sleep(random.randint(5, 10))
            future = executor.submit(run_analytics, uuid_and_question, user_id)
            futures[future] = uuid_and_question
        for future in as_completed(futures):
            print("Collecting job result...")
            try:
                job_id = future.result()
                if job_id is not None:
                    job_ids.append(job_id)
                    successful_count += 1
                    print(f"✅ Job completed: {job_id}")
                else:
                    failed_count += 1
                    print(
                        f"❌ Job failed for question: {futures[future]} (returned None)"
                    )
            except Exception as e:
                failed_count += 1
                print(f"❌ Error processing question '{futures[future]}': {e}")

    print(
        f"\nAll jobs processed. {successful_count} successful, {failed_count} failed."
    )

    # Filter out None values before any operations
    valid_job_ids = [job_id for job_id in job_ids if job_id is not None]

    if valid_job_ids:
        print("Successful Job IDs:")
        for job_id in valid_job_ids:
            print(f"  - {job_id}")
    else:
        print("No jobs completed successfully.")

    return valid_job_ids


def generate_unique_job_id(question, user_id, max_attempts=10):
    # Unpack using helper function
    _, question_text, _ = unpack_question(question)

    client = OpenAI()
    case_title_base = client.responses.create(
        model="gpt-4.1",
        input=f'Generate a folder name for the research topic: "{question_text}". Only return the folder name in plain text without any additional text. All lowercase with underlines.',
    ).output_text.strip()

    # Use PT timezone for human-readable timestamp
    pt_timezone = pytz.timezone("US/Pacific")
    pt_time = datetime.now(pt_timezone)
    # Use UUID for guaranteed uniqueness (keeping same length for consistency)
    unique_id = str(uuid4()).replace("-", "")[:12]  # 12 characters for more uniqueness
    # Format: -{case_title}-{YYYYMMDD_HHMMSS}-{unique_id}
    job_id = f"-{case_title_base}-{pt_time.strftime('%Y%m%d_%H%M%S')}-{unique_id}"
    return job_id


## shell script run
if __name__ == "__main__":
    # Add argument parsing
    parser = argparse.ArgumentParser(description="Run analytics batch")
    parser.add_argument(
        "--batch-name",
        type=str,
        required=True,
        help="Batch name for this run (required)",
    )
    parser.add_argument(
        "--questions-file",
        type=str,
        required=True,
        help="Path to JSON file with questions",
    )
    # parser.add_argument('--log-file', type=str, help='Path to log file for output (for nohup usage)')
    parser.add_argument(
        "--num-concurrent",
        type=int,
        default=10,
        help="Number of concurrent jobs to run (default: 10)",
    )

    args = parser.parse_args()
    pt_timezone = pytz.timezone("US/Pacific")
    print(f"Starting analytics batch at {datetime.now(pt_timezone)} (PT)")
    print(f"Batch name: {args.batch_name}")
    print(f"Questions file: {args.questions_file}")
    user_id = args.batch_name

    # Always create the batch subdirectory before running jobs using helper
    reports_dir = get_reports_directory(user_id)

    if args.questions_file:
        with open(args.questions_file, "r") as f:
            questions_data = json.load(f)
            if isinstance(questions_data, dict):
                questions = list(questions_data.items())  # List of (UUID, question)
            elif isinstance(questions_data, list):
                questions = [(None, q) for q in questions_data]
            else:

                raise ValueError("Questions file must be a list or dict.")
        print(f"Loaded {len(questions)} questions from {args.questions_file}")
    else:
        print("No questions file provided, using default questions.")

    batch_run_analytics(questions, user_id, num_concurrent=args.num_concurrent)
    pt_timezone = pytz.timezone("US/Pacific")
    print(f"Analytics batch completed at {datetime.now(pt_timezone)} (PT)")
