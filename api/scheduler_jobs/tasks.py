import os
import random

from google.cloud import scheduler
from google.protobuf.duration_pb2 import Duration
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.scheduler_v1.types import job as gcs_job
from flask import Response

from api.utils import message_resp


def create_scheduler_job(user_id: str, schedule: str = None) -> gcs_job.Job:
    """Create a job with an App Engine target via the Cloud Scheduler API"""
    client = scheduler.CloudSchedulerClient()
    tz = os.environ.get("TZ")
    project_id = os.environ.get("GCP_PROJECT_ID")
    location_id = os.environ.get("GCP_LOCATION")
    base_url = os.environ.get("DEV_SCIENCE_URL")
    url = f"{base_url}/daily-report?user_id={user_id}"
    parent = f"projects/{project_id}/locations/{location_id}"

    duration = Duration()
    duration.seconds = 1800
    r1 = random.randint(0, 30)
    schedule = schedule if schedule else f"{r1} 7 * * *"

    # Construct the request body.
    job = {
        "http_target": {"uri": url, "http_method": 2,},
        "schedule": schedule,
        "time_zone": tz,
        "retry_config": {"retry_count": 5, "min_backoff_duration": duration},
        "attempt_deadline": duration,
    }

    # Use the client to send the job creation request.
    response = client.create_job(request={"parent": parent, "job": job})
    return response


def delete_scheduler_job(job_id: str) -> Response:
    """Delete a job via the Cloud Scheduler API"""
    client = scheduler.CloudSchedulerClient()
    project_id = os.environ.get("GCP_PROJECT_ID")
    location_id = os.environ.get("GCP_LOCATION")
    job = f"projects/{project_id}/locations/{location_id}/jobs/{job_id}"

    # Use the client to send the job deletion request.
    try:
        client.delete_job(name=job)
        message_resp()
    except GoogleAPICallError as e:
        message_resp(e)
