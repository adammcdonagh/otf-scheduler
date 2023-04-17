#!/bin/env python3
import argparse
import logging

from otfscheduler.client import Client
from otfscheduler.server import Server


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v", "--verbose", help="Set verbose logging", action="store_true"
    )
    parser.add_argument("-c", "--config", help="Configuration file", type=str)

    parser.add_argument("-s", "--server", help="Run the server", action="store_true")

    # Argument if a client, the id of the runner
    parser.add_argument(
        "-r",
        "--runnerId",
        help="The id of the runner, if not running in server mode",
        type=str,
    )

    args = parser.parse_args()

    # Validate arguments. You cannot specify -s if -r is specified
    if args.server and args.runnerId:
        raise Exception("Cannot specify -s and -r")

    # If neither are specified, then also error
    if not args.server and not args.runnerId:
        raise Exception("Must specify -s or -r")

    logging_level = logging.INFO

    if args.verbose:
        logging_level = logging.DEBUG

    logging.getLogger().setLevel(logging_level)

    # Determine if we are running in server mode or client mode
    if args.server:
        # Create the server instance
        print("Starting server")
        server = Server(args.config, debug=args.verbose)
        server.run()
    else:
        print("Starting client")
        client = Client(args.config, args.runnerId, debug=args.verbose)
        client.run()


if __name__ == "__main__":
    main()
