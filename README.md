# otf-scheduler

This is a mostly working scheduler. It will job commands defined in each job's JSON config file.

Not sure whether to continue development on this yet. The main issue I've seen so far is that if jobs are scheduled (e.g. a cron job that runs once a minute), and the worker isn't up. Once the worker starts up, it seems to run every missed run, rather than just continuing.

This requires a default redis running to store the job queue and history.

Jobs currently report OK regardless of their return code. This also needs fixing.

The rq directory contains a few proof of concept configs for playing around with rq scheduler package.
