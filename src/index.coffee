EventEmitter = require('events').EventEmitter

connectToRedis = (options) ->
  options.port ?= 6379
  options.host ?= '127.0.0.1'
  redis = require('redis').createClient options.port, options.host
  redis.auth options.password if options.password?
  redis.select options.database if options.database?
  redis

class Noticeboard
  constructor: (options) ->
    @redis = options.redis ? connectToRedis(options)

  join: (options, callbacks) ->
    # return a Roustabout that checks this Noticeboard
    new Roustabout @, options, callbacks

  end: ->
    @redis.quit()

  ###
  The naming convention:

  * task is the json that goes in the queue's soorted set. This forms the "id"
    of sorts. The part that doesn't change between similar / duplicate jobs.
    This would contain the task name and probably the thing you're performing
    the task on's identifier. That sort of thing. The only required attribute
    is "name" which should be the task/callback to be performed's name.

  * data is the obtional json that goes in a separate key and contains extra
    info that might change between similar / duplicate jobs. More recent calls
    to perform the same task might have different data in here because the
    thing has changed since older calls. But that's OK because the newer call
    replaces the older ones which are now obsolete. So basically any extra data
    for the task runner that's not some unique key.

  * result is optional extra json that might be returned when a task succeeds.
    This can be used to communicate back to the thing that posted the job.

  * error is extra json for when a task fails.

  callback will always receive err, task, method but task and method might be
  null.
  ###

  postJob: (task, data, callback) ->
    # post a new job to this noticeboard
    taskText = JSON.stringify task
    dataText = JSON.stringify(data) if data

    date = new Date()
    timestampInteger = date.getTime() # to use as score
    timestampText = date.toISOString() # to use as queued time

    multi = @redis.multi()
    multi.zadd "queuedJobs", timestampInteger, taskText
    multi.incr "queuedTotal"
    multi.set("job:#{taskText}:data", dataText) if data
    multi.set "job:#{taskText}:queued", timestampText

    multi.exec (err, results) =>
      return callback(err, task) if err
      callback()

  checkAndStart: (callback) ->
    # try and start working on the next job in the queue if there is one
    @redis.watch "queuedJobs"
    @redis.zrange "queuedJobs", 0, 0, (err, results) =>
      return callback(err) if err

      if results.length == 0
        # we're not going to do multi, so must manually unwatch
        # no error, no job, 5 kids to feed
        return @redis.unwatch (err) ->
          return callback(err) if err
          return callback()

      taskText = results[0]

      date = new Date()
      timestampInteger = date.getTime() # to use as score
      timestampText = date.toISOString() # to use as started time

      multi = @redis.multi()
      multi.get "job:#{taskText}:data" # first to make it easy to retrieve
      multi.zrem "queuedJobs", taskText
      multi.zadd "workingJobs", timestampInteger, taskText
      multi.incr "workingTotal"
      multi.set "job:#{taskText}:started", timestampText
      multi.exec (err, results) =>
        task = JSON.parse(taskText)
        return callback(err, task) if err

        data = undefined
        data = JSON.parse(results[0]) if results[0]
        callback(null, task, data)

  succeedJob: (task, result, callback) ->
    # mark a working job as successful

    taskText = JSON.stringify task
    resultText = JSON.stringify(result) if result

    @redis.watch "job:#{taskText}:queued"
    @redis.watch "job:#{taskText}:started"

    # TODO: what if the task was queued or started again since we started executing
    # this task? We could compare queued and started with the timestamps at the
    # time somehow.

    date = new Date()
    timestampInteger = date.getTime() # to use as score
    #timestampText = date.toISOString() # to use as queued time

    multi = @redis.multi()
    multi.zrem "failedJobs", taskText # successes clear failures
    multi.zrem "workingJobs", taskText
    multi.zadd "successfulJobs", timestampInteger, taskText
    multi.incr "successfulTotal"
    multi.set("job:#{taskText}:result", resultText) if result
    multi.del "job:#{taskText}:queued"
    multi.del "job:#{taskText}:started"
    multi.del "job:#{taskText}:data"
    multi.del "job:#{taskText}:error" # successes clear errors

    multi.exec (err, results) =>
      return callback err, task

  failJob: (task, error, callback) ->
    # mark a working job as failed

    taskText = JSON.stringify task
    errorText = JSON.stringify(error) if error

    @redis.watch "job:#{taskText}:started"

    # not so sure about this check anymore..
    abort = =>
      #console.log "job posted again or not started, so aborting"
      return @redis.unwatch callback

    @redis.exists "job:#{taskText}:started", (err, startedExists) =>
      return callback(err, task) if err
      return abort() unless startedExists

      date = new Date()
      timestampInteger = date.getTime() # to use as score
      #timestampText = date.toISOString() # to use as queued time

      multi = @redis.multi()
      multi.zrem "workingJobs", taskText
      multi.zadd "failedJobs", timestampInteger, taskText
      multi.incr "failedTotal"
      multi.del "job:#{taskText}:started"
      multi.set("job:#{taskText}:error", errorText) if error

      multi.exec (err, results) =>
        return callback err, task

  repostJob: (task, callback) ->
    # re-post/queue a failed job

    taskText = JSON.stringify task
    date = new Date()
    timestampInteger = date.getTime() # to use as score
    timestampText = date.toISOString() # to use as queued time

    multi = @redis.multi()

    multi.zrem "failedJobs", taskText
    multi.zadd "queuedJobs", timestampInteger, taskText
    multi.set "job:#{taskText}:queued", timestampText
    multi.del "job:#{taskText}:error"

    multi.exec (err, results) =>
      return callback err, task

  clearFailure: (task, callback) ->
    # remove a failed job

    taskText = JSON.stringify task

    @redis.watch "queuedJobs"
    @redis.watch "workingJobs"

    abort = =>
      #console.log "aborting because job queued or started again"
      return @redis.unwatch callback

    # see if the task is in queuedJobs or workingJobs
    multi = @redis.multi()

    multi.zscore "queuedJobs", taskText
    multi.zscore "workingJobs", taskText

    multi.exec (err, results) =>
      return callback(err) if err

      isQueued = results[0]
      isWorking = results[1]

      #console.log isQueued, isWorking

      # You shouldn't be allowed to clear a failure that's been queued again or
      # started in the meantime because we're going to delete the task's data.
      return abort() if isQueued or isWorking

      multi = @redis.multi()

      multi.zrem "failedJobs", taskText
      multi.del "job:#{taskText}:queued"
      multi.del "job:#{taskText}:started"
      multi.del "job:#{taskText}:data"
      multi.del "job:#{taskText}:error"

      multi.exec (err, results) =>
        return callback err, task

  # TODO: some utility methods for interfaces and cron jobs
  # * list queue details
  # * list waiting details
  # * list success details
  # * list failed details

class Roustabout extends EventEmitter
  constructor: (noticeboard, options, callbacks) ->
    @running = false
    @noticeboard = noticeboard
    @redis = @noticeboard.redis
    @callbacks = callbacks

    # this is how long to wait before polling if there's nothing in the queue
    @timeout = options.timeout or 1000

    # bind / close over for setTimeout
    self = @
    @_poll = -> self.poll()

  start: ->
    return if @started
    @running = true
    @poll()

  end: ->
    return unless @running
    @running = false # don't poll again

  poll: ->
    return unless @running
    @emit 'poll', @
    @noticeboard.checkAndStart (err, task, data) =>
      # If the queue is empty or the redis transaction got discarded due to a
      # watch it shouldn't count as an error. That's just blank err and task.
      @handleRedisError(err, task, data) if err

      if task
        # perform the job if we received one
        @perform(task, data)

      else
        # wait a bit before polling again because the queue is empty
        setTimeout(@_poll, @timeout) if @running

  perform: (task, data) ->
    @emit 'job', @, task, data
    if cb = @callbacks[task.name]
      try
        cb task, data, (result) =>
          try
            if result instanceof Error
              @fail task, data, result, @handleRedisError
            else
              @succeed task, data, result, @handleRedisError
          finally
            process.nextTick @_poll
      catch error
        err = new Error(error)
        @fail task, data, err, @handleRedisError
        process.nextTick @_poll
    else
      err = new Error("Missing Job: #{task.name}")
      @fail task, data, err, @handleRedisError
      process.nextTick @_poll

  succeed: (task, data, result, callback) ->
    @noticeboard.succeedJob task, result, callback
    @emit 'success', @, task, data, result

  fail: (task, data, error, callback) ->
    @noticeboard.failJob task, error, callback
    @emit 'failure', @, task, data, error

  handleRedisError: (err, task, method) ->
    # This is for when any of the methods we call on Noticeboard causes an
    # error. Not failed jobs, but redis errors specifically where we probably
    # can't mark the job as failed because the problem is actually our
    # connection to redis. The working job will probably just be timed out by
    # something externally at a later stage.
    return unless err
    @emit 'error', @, err, task, method
    console.error(err, task, method)

connect = (options) ->
  # connect to redis and return a new Noticeboard
  options ?= {}
  new Noticeboard options


module.exports =
  connect: connect
  Noticeboard: Noticeboard
  Roustabout: Roustabout
