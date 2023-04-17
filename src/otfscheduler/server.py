# Define a server class
import copy
import json
import os
import signal
import sys
import time
from datetime import datetime, timedelta
from multiprocessing import Process
from uuid import uuid4

from flask import Flask
from redis import Redis
from rq_scheduler import Scheduler

from otfscheduler.job import Job
from otfscheduler.job import run as job_decorator
from otfscheduler.web.app import App
from rq.job import JobStatus

# TODO: Use waitress around Flask


class Server:
    # Load job configuration
    job_config = dict()
    scheduler_state = None
    webapp_state = None

    def __init__(self, config_file, debug=False):
        # Check config file exists, otherwise error
        if not os.path.exists(config_file):
            raise Exception(f"Config file {config_file} does not exist")

        # Load config file
        with open(config_file, "r") as f:
            self.config = json.load(f)

        os.environ["TZ"] = self.config["timezone"]
        time.tzset()
        time.tzname

        self.app = Flask(App.__name__)
        self.debug = debug

    def run(self):
        # Load today's job config, or validate that this is already loaded
        # Name the scheduler
        scheduler_name = self.config["name"]
        self._init_scheduler(name=scheduler_name)
        # Once this is done, we can fork and create the scheduler process

        # Create the RQ scheduler
        rq_scheduler = Process(target=self.run_scheduler)
        rq_scheduler.start()

        # Same for the webserver
        flask = Process(target=self.web)
        flask.start()

        def kill_handler(sig, frame):
            # Check the signal
            print("Killing scheduler process")
            rq_scheduler.terminate()
            rq_scheduler.join()
            print("Killing web server process")
            flask.terminate()
            flask.join()

            sys.exit(0)

        # Register the use of the signal_handler
        signal.signal(signal.SIGINT, kill_handler)

        while True:
            # Sleep 1 second forever
            try:
                # Check to see if the child process has died
                if not rq_scheduler.is_alive():
                    raise Exception("Scheduler process stopped unexpectedly")

                # Check to see if the web process has died
                if not flask.is_alive():
                    raise Exception("Web process stopped unexpectedly")
                time.sleep(5)

                print("Server thread running")

                # Print out a list of jobs and their next expected run time
                # Get the list of jobs from the scheduler
                jobs = self.scheduler.get_jobs()
                for job in jobs:
                    next_runtime = job.meta["job_obj"].get_next_run_time_from(
                        datetime.now()
                    )
                    print(f"{job.id} - {next_runtime}")

            except KeyboardInterrupt:
                print("Server thread stopping")

                break

    def web(self):
        self.app.run(
            debug=self.debug,
            host="0.0.0.0",
            port=self.config["listen"]["port"],
            use_reloader=False,
        )
        print("Web thread stopping")

    def run_scheduler(self):
        print(f"Starting scheduler process in process {os.getpid()}")
        print("Triggering schduler")
        self.scheduler.run()

    def _init_scheduler(self, name="otfscheduler"):
        # Create a scheduler, configured to schedule jobs against each valid queue
        queues = self.config["valid_worker_queues"]
        self.connection = Redis()
        self.scheduler = Scheduler(
            queues, connection=self.connection, interval=5, name=name
        )

        # Loop through JSON files inside config["job_definitions"] directory
        job_def_directory = self.config["job_definitions"]["path"]
        for file in os.listdir(job_def_directory):
            if file.endswith(".json"):
                # Get the filename without the extension
                job_id = os.path.splitext(file)[0]
                with open(os.path.join(job_def_directory, file), "r") as f:
                    self.job_config[job_id] = json.load(f)

        print(f"Loaded {len(self.job_config)} job definitions")

        # Set the new day start time based on the config
        new_day_time = datetime.now().replace(
            hour=self.config["new_day"]["hour"],
            minute=self.config["new_day"]["minute"],
            second=0,
            microsecond=0,
        )
        # End of day is 1 day after new day
        end_of_day_time = new_day_time + timedelta(days=1)

        # Check if we need to run the new day procedure. Usually this would be no, unless this is the first time the scheduler has started up on this day
        # Check for a key in Redis named otfscheduler:state:new_day with a value of the current day (Will be yesterday if new day time is after the current time)
        # If the key exists, then we don't need to run the new day procedure
        do_new_day = True
        if self.connection.exists("otfscheduler:state:new_day"):
            # Check the value
            if self.connection.get(
                "otfscheduler:state:new_day"
            ).decode() == new_day_time.strftime("%Y%m%d"):
                do_new_day = False
                return

        if do_new_day:
            print("New day procedure required")

        # Current job run date string DDMMYYYY
        job_run_date = datetime.now().strftime("%Y%m%d")

        # We need to look at the existing jobs in the scheduler and move any for the previous day into the archive, and adjust their TTL to 2 days (so we have 3 days of history)
        # If any jobs are currently executing, or paused for whatever reason, then they should be left as they are
        # List everything in rq:job that doesnt start with job_run_date
        # Use hscan to find the hashes

        for item in self.connection.keys("rq:job:*"):
            # Get the job id
            job_id = item.decode().split(":")[-1]
            # Get the job_run_date for this job
            this_job_run_date = job_id.split("-")[0]

            # Get the actual hash
            job_hash = self.connection.hgetall(f"rq:job:{job_id}")
            # Check to see if it's status is started or not
            if (
                b"status" not in job_hash
                or job_hash[b"status"].decode() != JobStatus.STARTED
            ):
                # Move it to the archive
                new_key = f"otf:job:archive:{this_job_run_date}:{job_id}"
                self.connection.rename(f"rq:job:{job_id}", new_key)
                # Set the TTL to 2 days
                self.connection.expire(new_key, 172800)

        # Loop through each job definition
        for job_id in self.job_config.keys():
            # Deterimine whether the job is meant to run today or not
            # Create a Job object for the job
            job_obj = Job(job_id, self.job_config[job_id])

            # We want a unique ID, but only 6 characters long
            unique_run_id = uuid4().hex[:6]

            scheduled_job_instance_id_prefix = (
                f"{job_run_date}-{unique_run_id}-{job_id}"
            )

            # Determine if the job is meant to run today
            if not job_obj.runs_on_day(new_day_time, new_day_end=end_of_day_time):
                print(f"{job_id} - Does not run today")
                continue

            # Add the job to the scheduler
            print("Runs today")

            # If the job is cyclic, then we create an hourly or minutely schedule
            if job_obj.is_cyclic(new_day_time):
                # Create a schedule for each cron string that applies for today
                cron_strings = job_obj.get_cron_strings_for_today(
                    new_day_start=new_day_time
                )
                job_instance = 0
                for cron_string in cron_strings:
                    # Alter the job object so that the cron expression is just this one
                    # Take a copy of it first though
                    new_job_obj = copy.deepcopy(job_obj)
                    new_job_obj.CONFIG["schedule"]["value"] = cron_string

                    job_metadata = {
                        "job_id": job_id,
                        "job_run_date": job_run_date,
                        "unique_run_id": unique_run_id,
                        "job_obj": new_job_obj,
                    }

                    self.scheduler.cron(
                        cron_string=cron_string,
                        func=job_decorator,
                        kwargs=job_obj.CONFIG,
                        meta=job_metadata,
                        id=f"{scheduled_job_instance_id_prefix}-{job_instance}",
                        queue_name=job_obj.queue,
                        ttl=60 * 60 * 24 * 3,  # 3 days
                        use_local_timezone=True,
                    )
                    job_instance += 1
            else:
                # Otherwise we just create a one-off schedule
                # schedule.once(
                #     job_obj.get_next_run_time_from(new_day_time), self.run_job, job_obj
                # )
                print(
                    f"Scheduled next run for {job_obj.get_next_run_time_from(new_day_time)}"
                )

        # Write the new day key to Redis
        self.connection.set(
            "otfscheduler:state:new_day", new_day_time.strftime("%Y%m%d")
        )
