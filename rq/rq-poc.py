import sys
import time
from datetime import datetime, timedelta

from redis import Redis
from rq_scheduler import Scheduler
from task import func

from rq import Queue, SimpleWorker

queue = Queue("foo", connection=Redis())
queue2 = Queue("bar", connection=Redis())

# redis-server
# python rq-poc.py worker
# python rq-poc.py scheduler


def run_scheduler():
    scheduler = Scheduler(
        ["foo", "bar"], connection=Redis(), interval=1
    )  # Get a scheduler for the "foo" queue
    print("Triggering schduler")
    scheduler.run()


def setup_schedule():
    # queue.enqueue_at(datetime(2020, 1, 1), func, job_id="j1")
    # queue.enqueue_at(datetime(2020, 1, 1, 3, 4), func, job_id="j2")
    # # Run something every minute
    # queue.cron("* * * * *", func, job_id="j3")

    scheduler = Scheduler(
        ["foo", "bar"], connection=Redis(), interval=1
    )  # Get a scheduler for the "foo" queue

    # Puts a job into the scheduler. The API is similar to RQ except that it
    # takes a datetime object as first argument. So for example to schedule a
    # job to run on Jan 1st 2020 we do:
    scheduler.enqueue_at(
        datetime(2020, 1, 1), func, job_id="j1", queue_name="foo"
    )  # Date time should be in UTC

    # Here's another example scheduling a job to run at a specific date and time (in UTC),
    # complete with args and kwargs.
    scheduler.enqueue_at(
        datetime(2020, 1, 1, 3, 4), func, job_id="j2", queue_name="foo"
    )

    # You can choose the queue type where jobs will be enqueued by passing the name of the type to the scheduler
    # used to enqueue
    scheduler.enqueue_at(
        datetime(2020, 1, 1), func, job_id="j3", queue_name="foo"
    )  # The job will be enqueued at the queue named "foo" using the queue type "rq.Queue"

    scheduler.cron(
        "* * * * *",  # A cron string (e.g. "0 0 * * 0")
        func=func,  # Function to be queued
        # args=[arg1, arg2],          # Arguments passed into function when executed
        id="j4",
        # kwargs={
        #     "foo": "bar",
        # },  # Keyword arguments passed into function when executed
        repeat=15,  # Repeat this number of times (None means repeat forever)
        result_ttl=300,  # Specify how long (in seconds) successful jobs and their results are kept. Defaults to -1 (forever)
        ttl=200,  # Specifies the maximum queued time (in seconds) before it's discarded. Defaults to None (infinite TTL).
        queue_name="foo",  # In which queue the job should be put in
        meta={"foo": "bar"},  # Arbitrary pickleable data on the job itself
        use_local_timezone=False,  # Interpret hours in the local timezone
    )

    # Create a new job that runs in 10 seconds
    scheduler.enqueue_in(
        timedelta(seconds=10), func, kwargs={"sleep": 5}, job_id="j5", queue_name="foo"
    )

    # Now create another job that also in 10 seconds, but is dependent on the first job, and in the bar queue instead
    scheduler.enqueue_in(
        timedelta(seconds=10),
        func,
        kwargs={"sleep": 2},
        job_id="j6",
        depends_on="j5",
        queue_name="bar",
    )

    # Make 5 more jobs with names starting with dep
    # Depenedencies as follows:
    # dep1, dep2 -> dep3, dep4, dep1 -> dep5

    scheduler.enqueue_in(
        timedelta(seconds=10),
        func,
        kwargs={"sleep": 2},
        job_id="dep1",
        queue_name="bar",
    )
    scheduler.enqueue_in(
        timedelta(seconds=10),
        func,
        kwargs={"sleep": 2},
        job_id="dep2",
        queue_name="bar",
    )
    scheduler.enqueue_in(
        timedelta(seconds=10),
        func,
        kwargs={"sleep": 2},
        job_id="dep3",
        depends_on=["dep1", "dep2"],
        queue_name="bar",
    )

    scheduler.enqueue_in(
        timedelta(seconds=10),
        func,
        kwargs={"sleep": 2},
        job_id="dep4",
        depends_on=["dep3"],
        queue_name="bar",
    )

    scheduler.enqueue_in(
        timedelta(seconds=10),
        func,
        kwargs={"sleep": 2},
        job_id="dep5",
        depends_on=["dep1", "dep4"],
        queue_name="bar",
        job_ttl=10,
    )

    scheduler.enqueue_in(
        timedelta(seconds=1),
        func,
        kwargs={"sleep": 2},
        job_id="timeout_job",
        depends_on=["dep1", "dep4"],
        queue_name="bar",
        job_ttl=2,
    )

    # Run a job that takes 60 seconds to run, but has as TTL of 5 seconds to see what happens to it while its running
    # Worker seems to set the TTL to -1 while the job is running, so it won't disappear while the job is in progress. Then the TTL gets set to the result TTL, so it doesnt disappear for a while (defualts to 500secs)
    scheduler.enqueue_in(
        timedelta(seconds=1),
        func,
        kwargs={"sleep": 60},
        job_id="expires_while_running_job?",
        queue_name="bar",
        job_ttl=10,
    )

    # Print out the jobs in the scheduler
    # loop through jobs and print them out
    for job in scheduler.get_jobs():
        print(f"{job} {job.dependency_ids}")


def redis_timeout_checker():
    connection = Redis()
    # Subscribe to EXPIRE events in Redis
    pubsub = connection.pubsub()
    pubsub.psubscribe(  # subscribe to the expire event
        "__keyevent@0__:expired"
    )  # 0 is the database number, use * for all databases
    for msg in pubsub.listen():
        print(f"{time.time()}, {msg}")


def setup_worker():
    # Run the worker
    worker = SimpleWorker(["queue1", "foo", "bar"], connection=queue.connection)
    worker.work(with_scheduler=True)  # Runs enqueued job


if __name__ == "__main__":
    # If arg[0] is "worker" then run the worker
    # If arg[0] is "scheduler" then run the scheduler

    if len(sys.argv) > 1 and sys.argv[1] == "worker":
        setup_worker()
        sys.exit(0)

    if len(sys.argv) > 1 and sys.argv[1] == "scheduler":
        setup_schedule()
        run_scheduler()
        sys.exit(0)

    if len(sys.argv) > 1 and sys.argv[1] == "checker":
        redis_timeout_checker()
        sys.exit(0)
