#!/usr/bin/env tarantool

local tap = require('tap')
local test = tap.test('cluster')
local urilib = require('uri')

local uri = urilib.parse(os.getenv('LISTEN'))
local HOST, PORT = uri.host or '*', uri.service

test:plan(8)

local config = {
	listen = PORT,
	logger="tarantool.log",
	cluster = { shard1 = {} }
}
local status, result = pcall(box.cfg, config)
test:ok(not status and result:match('Incorrect'), 'empty shard in cluster list')

config.cluster = { { uri = 'localhost:'..(PORT + 1) } }
status, result = pcall(box.cfg, config)
test:ok(not status and result:match('Incorrect'), 'no name specified for host')

config.cluster = { shard1 = {} }
status, result = pcall(box.cfg, config)
test:ok(not status and result:match('Incorrect'), 'no uri specified for host')

config.cluster = { shard1 = 'localhost:'..(PORT + 1) }
status, result = pcall(box.cfg, config)
test:ok(not status and result:match('Incorrect'), 'shard is not array')

config.cluster = { shard1 = { uri = (PORT + 1) } }
status, result = pcall(box.cfg, config)
test:ok(not status and result:match('Incorrect'), 'uri is not string')

config.cluster = { shard1 = { uri = 'Incorrect uri --- ,,, ;;;' } }
status, result = pcall(box.cfg, config)
test:ok(not status and result:match('Incorrect'), 'incorrect uri')

config.cluster = { shard1 = {
	key1 = 'val', key2 = 'val', uri = 'localhost:'..(PORT + 1)
} }
status, result = pcall(box.cfg, config)
test:ok(not status and result:match('Incorrect'), 'unknown keys')

config.cluster = {
	shard1 = { uri = 'localhost:'..(PORT + 1) },
	shard2 = { uri = 'localhost:'..(PORT + 2) },
	shard3 = { uri = 'localhost:'..(PORT + 3) },
}
status, result = pcall(box.cfg, config)
test:ok(status, 'cluster ok')

os.exit(test:check() == true and 0 or -1)
