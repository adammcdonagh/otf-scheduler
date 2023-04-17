import json
import os
import time

from redis import Redis

from rq import Worker


class Client:
    def __init__(self, config_file, runner_id, debug=False):
        self.runner_id = runner_id
        self.debug = debug

        # Load config file
        with open(config_file, "r") as f:
            self.config = json.load(f)

        os.environ["TZ"] = self.config["timezone"]
        time.tzset()
        time.tzname

        queues = self.config["queues"]

        self.connection = Redis()

        self.worker = Worker(queues, connection=self.connection)

    def run(self):
        self.worker.work(with_scheduler=True)  # Runs enqueued job
