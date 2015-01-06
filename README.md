Roustabout
==========

This is heavily inspired by and in some parts based on
[coffee-resque](https://github.com/technoweenie/coffee-resque). The main
difference is that it is based on a redis sorted set and not a list.

It tries to handle duplicate tasks so that "idempotent" (in a loose sense and
for lack of a better word) tasks that enter the queue at the same time will
only be executed once and it tries to behave well if multiple consecutive
"identical" task executions might fail and be retried and succeed in all sorts
of overlapping ways. Great care is being taken to try and be as thread and
transaction safe as possible.

Furthermore the way things are stored is arguably a bit simpler, making it
easier to build a user interface for seeing and managing what's going on in the
task queue. It doesn't concern itself with registering and tracking task
runners and it doesn't care about multiple queues, so no prefixing. Just use
multiple different Noticebords that use different redis databases.

Failed tasks can be requeued to be retried or cleared if they aren't relevant
anymore and timing out tasks that probably died can be handled by a cron job.
