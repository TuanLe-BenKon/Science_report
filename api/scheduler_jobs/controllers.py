import os

from google.cloud import scheduler
from google.protobuf.duration_pb2 import Duration
from google.api_core.exceptions import GoogleAPICallError
from flask import Response

from api.utils import message_resp


def create_scheduler_job() -> Response:
    """Create a job with an App Engine target via the Cloud Scheduler API"""
    client = scheduler.CloudSchedulerClient()
    tz = os.environ.get("TZ")
    project_id = os.environ.get("GCP_PROJECT_ID")
    location_id = os.environ.get("GCP_LOCATION")
    url = os.environ.get("DEV_SCIENCE_URL")
    parent = f"projects/{project_id}/locations/{location_id}"

    duration = Duration()
    duration.seconds = 1800
    # Construct the request body.
    job = {
        "http_target": {"uri": url, "http_method": 2,},
        "schedule": "0 7 * * *",
        "time_zone": tz,
        "retry_config": {"retry_count": 1},
        "attempt_deadline": duration,
    }

    # Use the client to send the job creation request.
    response = client.create_job(request={"parent": parent, "job": job})

    print("Created job: {}".format(response.name))
    return message_resp()


def delete_scheduler_job(job_id):
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
