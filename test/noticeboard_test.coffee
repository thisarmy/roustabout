require './helper'
calls = 0
conn = makeDB()

# TODO: also optionally return info about a specific task
getSummary = (callback) ->
  summary = {}

  multi = conn.redis.multi()

  multi.zcard "queuedJobs"
  multi.zcard "workingJobs"
  multi.zcard "successfulJobs"
  multi.zcard "failedJobs"

  multi.get "queuedTotal"
  multi.get "workingTotal"
  multi.get "successfulTotal"
  multi.get "failedTotal"

  multi.exec (err, results) ->
    return callback(err) if err

    summary =
      queuedJobs: results[0]
      workingJobs: results[1]
      successfulJobs: results[2]
      failedJobs: results[3]
      queuedTotal: parseInt(results[4], 10)
      workingTotal: parseInt(results[5], 10)
      successfulTotal: parseInt(results[6], 10)
      failedTotal: parseInt(results[7], 10)

    callback(null, summary)

tasks = []

# BASICS

# queue one
tasks.push (cb) ->
  task = name: 'do-thing', id: 'some-id'
  data = foo: "bar"
  conn.postJob task, data, (err) ->
    return cb(err) if err
    getSummary (err, s) ->
      return cb(err) if err
      return errBack(cb, "queuedJobs != 1") unless s.queuedJobs == 1
      return errBack(cb, "queuedTotal != 1") unless s.queuedTotal == 1
      cb()

startedTask = null
startedData = null

# start the job
tasks.push (cb) ->
  conn.checkAndStart (err, task, data) ->
    return cb(err) if err
    return errBack(cb, "no task or data") unless task and data
    return errBack(cb, "no task.name or data.foo") unless task.name and data.foo
    getSummary (err, s) ->
      return cb(err) if err
      return errBack(cb, "queuedJobs != 0") unless s.queuedJobs == 0
      return errBack(cb, "workingJobs != 1") unless s.workingJobs == 1
      return errBack(cb, "workingTotal != 1") unless s.workingTotal == 1
      # keep this so we can fail it later
      startedTask = task
      startedData = data
      cb()

# fail the job
tasks.push (cb) ->
  error = new Error("This is a fake error for testing.")
  conn.failJob startedTask, error, (err) ->
    return cb(err) if err
    getSummary (err, s) ->
      return cb(err) if err
      return errBack(cb, "workingJobs != 0") unless s.workingJobs == 0
      return errBack(cb, "failedJobs != 1") unless s.failedJobs == 1
      return errBack(cb, "failedTotal != 1") unless s.failedTotal == 1
      cb()

# repost the job
tasks.push (cb) ->
  conn.repostJob startedTask, (err) ->
    return cb(err) if err
    getSummary (err, s) ->
      return cb(err) if err
      return errBack(cb, "failedJobs != 0") unless s.failedJobs == 0
      return errBack(cb, "queuedJobs != 1") unless s.queuedJobs == 1
      cb()

# start the requeued job
tasks.push (cb) ->
  conn.checkAndStart (err, task, data) ->
    return cb(err) if err

    # make sure it is the same task and data
    os = JSON.stringify(startedTask)
    od = JSON.stringify(startedData)
    ns = JSON.stringify(task)
    nd = JSON.stringify(data)
    return errBack(cb, "task doesn't match") unless ns == os
    return errBack(cb, "data doesn't match") unless nd == od

    getSummary (err, s) ->
      return cb(err) if err
      return errBack(cb, "queuedJobs != 0") unless s.queuedJobs == 0
      return errBack(cb, "workingJobs != 1") unless s.workingJobs == 1
      return errBack(cb, "workingTotal != 2") unless s.workingTotal == 2
      cb()

# succeed the job
tasks.push (cb) ->
  result = monkey: 'banana'
  conn.succeedJob startedTask, result, (err) ->
    return cb(err) if err
    getSummary (err, s) ->
      return cb(err) if err
      return errBack(cb, "workingJobs != 0") unless s.workingJobs == 0
      return errBack(cb, "successfulJobs != 1") unless s.successfulJobs == 1
      return errBack(cb, "successfulTotal != 1") unless s.successfulTotal == 1
      cb()

# SCENARIOS

# TODO: checkAndStart on an empty queue

# queue a job, start it, fail it, clear the failure
tasks.push (cb) ->
  conn.redis.flushdb()

  task = name: 'do-thing', id: 'some-id'
  data = foo: "bar"
  conn.postJob task, data, (err) ->
    return cb(err) if err
    conn.checkAndStart (err, task, data) ->
      return cb(err) if err
      return errBack(cb, "no task or data") unless task and data
      error = new Error("This is a fake error for testing.")
      conn.failJob task, error, (err) ->
        return cb(err) if err
        conn.clearFailure task, (err) ->
          return cb(err) if err
          getSummary (err, s) ->
            return cb(err) if err
            return errBack(cb, "failedJobs != 0") unless s.failedJobs == 0
            return errBack(cb, "failedTotal != 1") unless s.failedTotal == 1
            cb()

# queue the same task twice to simulate a redundant action
tasks.push (cb) ->
  conn.redis.flushdb()

  task = name: "duplicate-task"
  conn.postJob task, null, (err) ->
    return cb(err) if err
    conn.postJob task, null, (err) ->
      return cb(err) if err
      getSummary (err, s) ->
        return cb(err) if err
        return errBack(cb, "queuedJobs != 1") unless s.queuedJobs == 1
        return errBack(cb, "queuedTotal != 2") unless s.queuedTotal == 2
        cb()

# queue a task that's already failed again. The failure will stay there until
# it succeeds or gets cleared.
tasks.push (cb) ->
  conn.redis.flushdb()

  task = name: 'do-thing', id: 'some-id'
  data = foo: "bar"
  conn.postJob task, data, (err) ->
    return cb(err) if err
    conn.checkAndStart (err, task, data) ->
      return cb(err) if err
      return errBack(cb, "no task or data") unless task and data
      error = new Error("This is a fake error for testing.")
      conn.failJob task, error, (err) ->
        return cb(err) if err
        conn.postJob task, data, (err) ->
          return cb(err) if err
          getSummary (err, s) ->
            return cb(err) if err
            return errBack(cb, "failedJobs != 1") unless s.failedJobs == 1
            return errBack(cb, "failedTotal != 1") unless s.failedTotal == 1
            return errBack(cb, "queuedJobs != 1") unless s.queuedJobs == 1
            return errBack(cb, "queuedTotal != 2") unless s.queuedTotal == 2
            cb()

# Succeed a failed task to simulate a very slow task completing or a duplicate
# task that was queued again and then succeeded. Succeeding a cleared task
# shouldn't be much of a problem either because afterwards it is only the
# result (if any) that's important anyway. All this assumes that your job
# workers are written in a safe way, of course.
tasks.push (cb) ->
  conn.redis.flushdb()

  task = name: 'do-thing', id: 'some-id'
  conn.postJob task, null, (err) ->
    return cb(err) if err
    conn.checkAndStart (err, task, data) ->
      return cb(err) if err
      return errBack(cb, "no task") unless task
      error = new Error("This is a fake error for testing.")
      conn.failJob task, error, (err) ->
        return cb(err) if err
        conn.succeedJob task, null, (err) ->
          return cb(err) if err
          getSummary (err, s) ->
            return cb(err) if err
            return errBack(cb, "failedJobs != 0") unless s.failedJobs == 0
            return errBack(cb, "failedTotal != 1") unless s.failedTotal == 1
            return errBack(cb, "successfulJobs != 1") unless s.successfulJobs == 1
            return errBack(cb, "successfulTotal != 1") unless s.successfulTotal == 1
            cb()

# try to clear a failed task that's in been queued (or in progress) again
tasks.push (cb) ->
  conn.redis.flushdb()

  task = name: 'some-task'
  conn.postJob task, null, (err) ->
    return cb(err) if err
    conn.checkAndStart (err, task, data) ->
      return cb(err) if err
      return errBack(cb, "no task") unless task
      error = new Error("This is a fake error for testing.")
      conn.failJob task, error, (err) ->
        return cb(err) if err
        conn.postJob task, null, (err) ->
          return cb(err) if err
          conn.clearFailure task, (err) ->
            return cb(err) if err
            console.log "done clearing failure"
            getSummary (err, s) ->
              return cb(err) if err
              return errBack(cb, "failedJobs != 1") unless s.failedJobs == 1
              return errBack(cb, "failedTotal != 1") unless s.failedTotal == 1
              return errBack(cb, "queuedJobs != 1") unless s.queuedJobs == 1
              return errBack(cb, "queuedTotal != 2") unless s.queuedTotal == 2
              cb()


# TODO: queue a task, then start it, then queue it again, then succeed (or fail
# and clear?) the first one, then succeed (or fail) the second one.

# TODO: test both with and without task data or result


async.series tasks, (err, results) ->
  if err
    throw err
  else
    console.log '.'
  conn.redis.end()
