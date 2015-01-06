GLOBAL.async ?= require 'async'
#GLOBAL.assert ?= require 'assert'
GLOBAL.Roustabout ?= require '../src'
GLOBAL.makeDB = (options) ->
  options ?= {}
  options.database = 1 # slightly less likely to drop your db by accident
  options.password = process.env.REDIS_AUTH if process.env.REDIS_AUTH
  conn = Roustabout.connect options
  conn.redis.flushdb()
  conn

GLOBAL.errBack = (callback, message) ->
  # USAGE: return errorBack(cb, message) unless value
  callback(new Error(message))


