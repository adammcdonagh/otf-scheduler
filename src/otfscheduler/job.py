import subprocess
from datetime import datetime, timedelta

from cron_converter import Cron


# Create a basic Job class
class Job:
    def __init__(self, job_id, job_config):
        self.id = job_id
        self.CONFIG = job_config  # Make this a constant

        self.queue = self.CONFIG["queue"]

    def _get_cron_schedules(self) -> list:
        # Get the cron schedules from the config
        # Get the schedule type from the config
        schedule_type = self.CONFIG["schedule"]["type"]
        if schedule_type == "cron":
            # Get the cron schedule
            cron_schedule = self.CONFIG["schedule"]["value"]
            result = []
            # If value is an array then loop through each value, otherwise just use the value
            if isinstance(cron_schedule, list):
                # Loop through each value and convert to a cron schedule
                for _, cron_value in enumerate(cron_schedule):
                    result.append(Cron(cron_value))
                return result
            else:
                return [Cron(cron_schedule)]
        else:
            raise ValueError("Invalid schedule type: " + schedule_type)

    # def is_hourly(self, new_day_start: datetime) -> bool:
    #     # Check if the job is hourly
    #     # Get the cron schedules from the config
    #     cron_schedules = self._get_cron_schedules()
    #     # Loop through each cron schedule and check if it is hourly
    #     # Get the next 2 run times based on new day start, if they are within the same hour then they're minutely, otherwise it's hourly
    #     schedule_type = self.CONFIG["schedule"]["type"]
    #     if schedule_type == "cron":
    #         # Get the next run time
    #         next_run_time = self.get_next_run_time_from(new_day_start)
    #         # Get the next run time after that
    #         next_next_run_time = self.get_next_run_time_from(next_run_time)

    #         # Check if the next run time is within 1hr of

    #     else:
    #         raise ValueError("Invalid schedule type: " + schedule_type)

    #     return False

    # Define a function to return a boolean if this is a cyclic job
    def is_cyclic(self, new_day_start: datetime) -> bool:
        # Job is cyclic if the schedule defines multiple runs per day
        # Get the schedule type from the config
        schedule_type = self.CONFIG["schedule"]["type"]
        if schedule_type == "cron":
            # Get the next 2 run times based on the schedule, if they are within the same "day" then the job is cyclic
            # Get the next run time
            next_run_time = self.get_next_run_time_from(new_day_start)
            # Get the next run time after that
            next_next_run_time = self.get_next_run_time_from(next_run_time)

            # Check if the next run time is within 24hrs of new_day_start
            if next_next_run_time - next_run_time < timedelta(days=1):
                return True
            else:
                return False
        else:
            raise ValueError("Invalid schedule type: " + schedule_type)

    def runs_on_day(
        self, new_day_start: datetime, new_day_end: datetime = None
    ) -> bool:
        # Check if the job is meant to run on the given date
        # Determine the next run time for the job
        next_run_time = self.get_next_run_time_from(new_day_start)

        # Check if the next run time is on the given date
        if new_day_end is None:
            if next_run_time.date() == new_day_start.date():
                return True
            else:
                return False
        else:
            # Check next_run_time is between date and before
            if next_run_time >= new_day_start and next_run_time < new_day_end:
                return True
            else:
                return False

    def get_cron_strings_for_today(self, new_day_start: datetime) -> list:
        # Returns a list of cron strings that have run times from new_day_start until 24hrs later
        # Get the cron schedules from the config
        cron_schedules = self._get_cron_schedules()
        todays_cron_strings = []
        for cron_schedule in cron_schedules:
            # Get the next run time
            next_run_time = cron_schedule.schedule(new_day_start).next()
            # Check if the next run time is within 24hrs of new_day_start
            if next_run_time - new_day_start < timedelta(days=1):
                # Add the cron string to the list
                todays_cron_strings.append(cron_schedule.to_string())

        return todays_cron_strings

    def get_next_run_time_from(self, from_time: datetime) -> datetime:
        # Get the next time the job should run, starting from the given time
        # Parse the schedule for the job. If it's a cron format, then it's nice and simple
        # Get the schedule type from the config
        schedule_type = self.CONFIG["schedule"]["type"]
        if schedule_type == "cron":
            # Get the cron schedule
            cron_schedule = self.CONFIG["schedule"]["value"]
            # If value is an array then loop through each value, otherwise just use the value
            if isinstance(cron_schedule, list):
                min_run_time = None
                # Loop through each value
                for cron_definition in cron_schedule:
                    # Parse the cron schedule
                    try:
                        next_run_time = Cron(cron_definition).schedule(from_time).next()
                    except Exception:
                        print("Invalid cron definition: " + cron_definition)
                        continue

                    # Check if the next run time is after the from_time
                    if next_run_time >= from_time:
                        # Check if the next run time is before the current min_run_time
                        if min_run_time is None or next_run_time < min_run_time:
                            # Set the min_run_time to the next_run_time
                            min_run_time = next_run_time
                    else:
                        # If the next run time is before the from_time, then continue to the next value
                        continue
                return min_run_time
            else:
                # Get the next run time
                next_run_time = Cron(cron_schedule).schedule(from_time).next()

                return next_run_time
        else:
            # If the schedule type is not cron, then raise an error
            raise ValueError(f"Invalid schedule type: {schedule_type}")


def run(**kwargs):
    # Run the command in the job definition, and return the output
    command = kwargs["command"]
    print(f"Running command: {command}")

    # Run the command
    try:
        output = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        output = e.output

    # Check if the output is a string, if not then convert it to a string
    if not isinstance(output, str):
        output = output.decode("utf-8")

    # Print the output for debugging
    print(f"Output: {output}")

    # Return the output
    return output
