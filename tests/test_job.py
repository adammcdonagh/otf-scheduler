import os
import time
from datetime import datetime, timedelta

import pytest

from otfscheduler.job import Job

os.environ["TZ"] = "Europe/London"
time.tzset()


@pytest.fixture
def job_instance():
    # Instantiate the class
    return Job(None, None)


def test_job_runs_on_day():
    # Get the current day of the week
    current_day = datetime.now().strftime("%a").upper()

    # Create a job config with a cron schedule that runs every minute
    job_config = {"schedule": {"type": "cron", "value": f"* * * * {current_day}"}}

    # Create a job that runs every minute
    job = Job("test-job", job_config)

    # Check that the job runs on the current date and time
    assert job.runs_on_day(datetime.now())

    # Check that the job doesn't run on the next day
    assert not job.runs_on_day(datetime.now() + timedelta(days=1))


def test_get_next_run_time_from_cron_single_value(job_instance):
    # Test the function with a single cron value
    # Set the config for the function
    job_instance.config = {"schedule": {"type": "cron", "value": "0 0 * * *"}}
    # Set the from_time for the function
    from_time = datetime(2023, 4, 11, 0, 0, 0)
    # Call the function
    next_run_time = job_instance.get_next_run_time_from(from_time)
    # Check if the next_run_time is the expected value
    assert next_run_time == datetime(2023, 4, 11, 0, 0, 0)


def test_get_next_run_time_from_cron_multiple_values(job_instance):
    # Test the function with multiple cron values
    # Set the config for the function
    job_instance.config = {
        "schedule": {"type": "cron", "value": ["0 0 * * *", "0 12 * * *"]}
    }
    # Set the from_time for the function
    from_time = datetime(2023, 4, 11, 0, 0, 0)
    # Call the function
    next_run_time = job_instance.get_next_run_time_from(from_time)
    # Check if the next_run_time is the expected value
    assert next_run_time == datetime(2023, 4, 11, 0, 0, 0)


def test_get_next_run_time_from_cron_invalid_definition(job_instance, capsys):
    # Test the function with an invalid cron definition
    # Set the config for the function
    job_instance.config = {"schedule": {"type": "cron", "value": "0 0 * * * * *"}}
    # Set the from_time for the function
    from_time = datetime(2023, 4, 11, 0, 0, 0)
    # Call the function
    # Expect a ValueError exception to be thrown
    with pytest.raises(ValueError):
        job_instance.get_next_run_time_from(from_time)


def test_get_next_run_time_from_invalid_schedule_type(job_instance):
    # Test the function with an invalid schedule type
    # Set the config for the function
    job_instance.config = {"schedule": {"type": "invalid", "value": "0 0 * * *"}}
    # Set the from_time for the function
    from_time = datetime(2023, 4, 11, 0, 0, 0)
    # Call the function and check if it raises a ValueError for the invalid schedule type
    with pytest.raises(ValueError):
        job_instance.get_next_run_time_from(from_time)
