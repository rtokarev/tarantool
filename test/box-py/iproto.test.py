import os
import sys
import struct
import socket
import msgpack
from tarantool.const import *
from tarantool import Connection
from tarantool.request import (
    Request, RequestInsert, RequestSelect, RequestUpdate,
    RequestUpsert, RequestBegin, RequestCommit, RequestRollback,
    RequestReplace, RequestDelete, RequestCall
)
from tarantool.response import Response
from lib.tarantool_connection import TarantoolConnection

admin("box.schema.user.grant('guest', 'read,write,execute', 'universe')")

print """
#
# iproto packages test
#
"""

# opeing new connection to tarantool/box
conn = TarantoolConnection(server.iproto.host, server.iproto.port)
conn.connect()
s = conn.socket

print """
# Test bug #899343 (server assertion failure on incorrect packet)
"""
print "# send the package with invalid length"
invalid_request = struct.pack('<LLL', 1, 4294967290, 1)
print s.send(invalid_request)
print "# check that is server alive"
print iproto.py_con.ping() > 0

# closing connection
s.close()

key_names = {}
for (k,v) in globals().items():
    if type(k) == str and k.startswith('IPROTO_') and type(v) == int:
        key_names[v] = k

def repr_dict(todump):
    d = {}
    for (k, v) in todump.items():
        k_name = key_names.get(k, k)
        d[k_name] = v
    return repr(d)

def test(header, body):
    # Connect and authenticate
    c = Connection('localhost', server.iproto.port)
    c.connect()
    print 'query', repr_dict(header), repr_dict(body)
    header = msgpack.dumps(header)
    body = msgpack.dumps(body)
    query = msgpack.dumps(len(header) + len(body)) + header + body
    # Send raw request using connectred socket
    s = c._socket
    try:
        s.send(query)
    except OSError as e:
        print '   => ', 'Failed to send request'
    c.close()
    print iproto.py_con.ping() > 0

def execute_and_print_request(connection, request, *args):
    data = bytes(request)
    for r in args:
        data += bytes(r)
    try:
        connection._socket.send(data)
    except OSError as e:
        print '   => ', 'Failed to send request'
    for i in range(len(args) + 1):
        response = Response(connection, connection._read_response())
        print response.__str__()

print """
#  Test gh-206 "Segfault if sending IPROTO package without `KEY` field"
"""

print "IPROTO_SELECT"
test({ IPROTO_CODE : REQUEST_TYPE_SELECT }, { IPROTO_SPACE_ID: 280 })
print "\n"

print "IPROTO_DELETE"
test({ IPROTO_CODE : REQUEST_TYPE_DELETE }, { IPROTO_SPACE_ID: 280 })
print "\n"

print "IPROTO_UPDATE"
test({ IPROTO_CODE : REQUEST_TYPE_UPDATE }, { IPROTO_SPACE_ID: 280 })
test({ IPROTO_CODE : REQUEST_TYPE_UPDATE },
     { IPROTO_SPACE_ID: 280, IPROTO_KEY: (1, )})
print "\n"

print "IPROTO_REPLACE"
test({ IPROTO_CODE : REQUEST_TYPE_REPLACE }, { IPROTO_SPACE_ID: 280 })
print "\n"

print "IPROTO_CALL"
test({ IPROTO_CODE : REQUEST_TYPE_CALL }, {})
test({ IPROTO_CODE : REQUEST_TYPE_CALL }, { IPROTO_KEY: ('procname', )})
print "\n"

# gh-434 Tarantool crashes on multiple iproto requests with WAL enabled
admin("box.cfg.wal_mode")
admin("space = box.schema.space.create('test', { id = 567 })")
admin("index = space:create_index('primary', { type = 'hash' })")
admin("box.schema.user.grant('guest', 'read,write,execute', 'space', 'test')")

c = Connection('localhost', server.iproto.port)
c.connect()
request1 = RequestInsert(c, 567, [1, "baobab"])
request2 = RequestInsert(c, 567, [2, "obbaba"])
s = c._socket
try:
    s.send(bytes(request1) + bytes(request2))
except OSError as e:
    print '   => ', 'Failed to send request'
response1 = Response(c, c._read_response())
response2 = Response(c, c._read_response())
print response1.__str__()
print response2.__str__()

request1 = RequestInsert(c, 567, [3, "occama"])
request2 = RequestSelect(c, 567, 0, [1], 0, 1, 0)
s = c._socket
try:
    s.send(bytes(request1) + bytes(request2))
except OSError as e:
    print '   => ', 'Failed to send request'
response1 = Response(c, c._read_response())
response2 = Response(c, c._read_response())
print response1.__str__()
print response2.__str__()

request1 = RequestSelect(c, 567, 0, [2], 0, 1, 0)
request2 = RequestInsert(c, 567, [4, "ockham"])
s = c._socket
try:
    s.send(bytes(request1) + bytes(request2))
except OSError as e:
    print '   => ', 'Failed to send request'
response1 = Response(c, c._read_response())
response2 = Response(c, c._read_response())
print response1.__str__()
print response2.__str__()

request1 = RequestSelect(c, 567, 0, [1], 0, 1, 0)
request2 = RequestSelect(c, 567, 0, [2], 0, 1, 0)
s = c._socket
try:
    s.send(bytes(request1) + bytes(request2))
except OSError as e:
    print '   => ', 'Failed to send request'
response1 = Response(c, c._read_response())
response2 = Response(c, c._read_response())
print response1.__str__()
print response2.__str__()

c.close()

admin("space:drop()")

#
# gh-522: Broken compatibility with msgpack-python for strings of size 33..255
#
admin("space = box.schema.space.create('test')")
admin("index = space:create_index('primary', { type = 'hash', parts = {1, 'string'}})")

class RawInsert(Request):
    request_type = REQUEST_TYPE_INSERT
    def __init__(self, conn, space_no, blob):
        super(RawInsert, self).__init__(conn)
        request_body = "\x82" + msgpack.dumps(IPROTO_SPACE_ID) + \
            msgpack.dumps(space_id) + msgpack.dumps(IPROTO_TUPLE) + blob
        self._bytes = self.header(len(request_body)) + request_body

class RawSelect(Request):
    request_type = REQUEST_TYPE_SELECT
    def __init__(self, conn, space_no, blob):
        super(RawSelect, self).__init__(conn)
        request_body = "\x83" + msgpack.dumps(IPROTO_SPACE_ID) + \
            msgpack.dumps(space_id) + msgpack.dumps(IPROTO_KEY) + blob + \
            msgpack.dumps(IPROTO_LIMIT) + msgpack.dumps(100);
        self._bytes = self.header(len(request_body)) + request_body

c = iproto.py_con
space = c.space('test')
space_id = space.space_no

TESTS = [
    (1,     "\xa1", "\xd9\x01", "\xda\x00\x01", "\xdb\x00\x00\x00\x01"),
    (31,    "\xbf", "\xd9\x1f", "\xda\x00\x1f", "\xdb\x00\x00\x00\x1f"),
    (32,    "\xd9\x20", "\xda\x00\x20", "\xdb\x00\x00\x00\x20"),
    (255,   "\xd9\xff", "\xda\x00\xff", "\xdb\x00\x00\x00\xff"),
    (256,   "\xda\x01\x00", "\xdb\x00\x00\x01\x00"),
    (65535, "\xda\xff\xff", "\xdb\x00\x00\xff\xff"),
    (65536, "\xdb\x00\x01\x00\x00"),
]

for test in TESTS:
    it = iter(test)
    size = next(it)
    print 'STR', size
    print '--'
    for fmt in it:
        print '0x' + fmt.encode('hex'), '=>',
        field = '*' * size
        c._send_request(RawInsert(c, space_id, "\x91" + fmt + field))
        tuple = space.select(field)[0]
        print len(tuple[0])== size and 'ok' or 'fail',
        it2 = iter(test)
        next(it2)
        for fmt2 in it2:
            tuple = c._send_request(RawSelect(c, space_id,
                "\x91" + fmt2 + field))[0]
            print len(tuple[0]) == size and 'ok' or 'fail',
        tuple = space.delete(field)[0]
        print len(tuple[0]) == size and 'ok' or 'fail',
        print
    print


print 'Test of schema_id in iproto.'
c = Connection('localhost', server.iproto.port)
c.connect()
s = c._socket

def test_request(req_header, req_body):
    query_header = msgpack.dumps(req_header)
    query_body = msgpack.dumps(req_body)
    packet_len = len(query_header) + len(query_body)
    query = msgpack.dumps(packet_len) + query_header + query_body
    try:
        s.send(query)
    except OSError as e:
        print '   => ', 'Failed to send request'
    resp_len = ''
    resp_headerbody = ''
    resp_header = {}
    resp_body = {}
    try:
        resp_len = s.recv(5)
        resp_len = msgpack.loads(resp_len)
        resp_headerbody = s.recv(resp_len)
        unpacker = msgpack.Unpacker(use_list = True)
        unpacker.feed(resp_headerbody)
        resp_header = unpacker.unpack()
        resp_body = unpacker.unpack()
    except OSError as e:
        print '   => ', 'Failed to recv response'
    res = {}
    res['header'] = resp_header
    res['body'] = resp_body
    return res

header = { IPROTO_CODE : REQUEST_TYPE_SELECT}
body = { IPROTO_SPACE_ID: space_id,
    IPROTO_INDEX_ID: 0,
    IPROTO_KEY: [],
    IPROTO_ITERATOR: 2,
    IPROTO_OFFSET: 0,
    IPROTO_LIMIT: 1 }
resp = test_request(header, body)
print 'Normal connect done w/o errors:', resp['header'][0] == 0
print 'Got schema_id:', resp['header'][5] > 0
schema_id = resp['header'][5]

header = { IPROTO_CODE : REQUEST_TYPE_SELECT, 5 : 0 }
resp = test_request(header, body)
print 'Zero-schema_id connect done w/o errors:', resp['header'][0] == 0
print 'Same schema_id:', resp['header'][5] == schema_id

header = { IPROTO_CODE : REQUEST_TYPE_SELECT, 5 : schema_id }
resp = test_request(header, body)
print 'Normal connect done w/o errors:', resp['header'][0] == 0
print 'Same schema_id:', resp['header'][5] == schema_id

header = { IPROTO_CODE : REQUEST_TYPE_SELECT, 5 : schema_id + 1 }
resp = test_request(header, body)
print 'Wrong schema_id leads to error:', resp['header'][0] != 0
print 'Same schema_id:', resp['header'][5] == schema_id

admin("space2 = box.schema.create_space('test2')")

header = { IPROTO_CODE : REQUEST_TYPE_SELECT, 5 : schema_id }
resp = test_request(header, body)
print 'Schema changed -> error:', resp['header'][0] != 0
print 'Got another schema_id:', resp['header'][5] != schema_id

c.close()

admin("space:drop()")
admin("space2:drop()")

#
# gh-1280 Segmentation fault on space.select(tuple()) or space.select([2])
#
admin("space = box.schema.create_space('gh1280', { engine = 'vinyl' })")
admin("index = space:create_index('primary')")
admin("space:insert({1})")
admin("space:insert({2, 'Music'})")
admin("space:insert({3, 'Length', 93})")

iproto.py_con.space('gh1280').select([])
iproto.py_con.space('gh1280').select(list())


admin("space:drop()")

admin("box.schema.user.revoke('guest', 'read,write,execute', 'universe')")

#
# gh-272 if the packet was incorrect, respond with an error code
# gh-1654 do not close connnection on invalid request
#
print """
# Test bugs gh-272, gh-1654 if the packet was incorrect, respond with
# an error code and do not close connection
"""

c = Connection('localhost', server.iproto.port)
c.connect()
s = c._socket
header = { "hello": "world"}
body = { "bug": 272 }
resp = test_request(header, body)
print 'sync=%d, %s' % (resp['header'][IPROTO_SYNC], resp['body'])
header = { IPROTO_CODE : REQUEST_TYPE_SELECT }
header[IPROTO_SYNC] = 1234
resp = test_request(header, body)
print 'sync=%d, %s' % (resp['header'][IPROTO_SYNC], resp['body'])
header[IPROTO_SYNC] = 5678
body = { IPROTO_SPACE_ID: 304, IPROTO_KEY: [], IPROTO_LIMIT: 1 }
resp = test_request(header, body)
print 'sync=%d, %s' % (resp['header'][IPROTO_SYNC], resp['body'])
c.close()


admin("space = box.schema.space.create('test_index_base', { id = 568 })")
admin("index = space:create_index('primary', { type = 'hash' })")
admin("box.schema.user.grant('guest', 'read,write,execute', 'space', 'test_index_base')")

c = Connection('localhost', server.iproto.port)
c.connect()
s = c._socket

request = RequestInsert(c, 568, [1, 0, 0, 0])
try:
    s.send(bytes(request))
except OSError as e:
    print '   => ', 'Failed to send request'
response = Response(c, c._read_response())
print response.__str__()

request = RequestUpdate(c, 568, 0, [1], [['+', 2, 1], ['-', 3, 1]])
try:
    s.send(bytes(request))
except OSError as e:
    print '   => ', 'Failed to send request'
response = Response(c, c._read_response())
print response.__str__()

request = RequestUpsert(c, 568, 0, [1, 0, 0, 0], [['+', 2, 1], ['-', 3, 1]])
try:
    s.send(bytes(request))
except OSError as e:
    print '   => ', 'Failed to send request'
response = Response(c, c._read_response())

request = RequestSelect(c, 568, 0, [1], 0, 1, 0)
try:
    s.send(bytes(request))
except OSError as e:
    print '   => ', 'Failed to send request'
response = Response(c, c._read_response())
print response.__str__()

c.close()
admin("space:drop()")

#
# gh-2016 in tarantool - check working of remote transactions.
#
admin("space = box.schema.create_space('gh2016', { id = 569, engine = 'vinyl' })")
admin("index = space:create_index('primary')")
admin("box.schema.user.grant('guest', 'read,write,execute', 'universe')")
admin("fiber = require('fiber')")
admin("function long_f() fiber.sleep(0.5) space:replace({1, 1, 1, 1}) end ")

c = Connection('localhost', server.iproto.port)
c.connect()

#
# Test remote transactions.
#
execute_and_print_request(c, RequestBegin(c, 22))

execute_and_print_request(c, RequestReplace(c, 569, [1], tx_id=22))
execute_and_print_request(c, RequestReplace(c, 569, [2], tx_id=22))
# Select with the read view returns transaction changes.
execute_and_print_request(c, RequestSelect(c, 569, 0, [], 0, 1000, ITERATOR_GE,
                          tx_id=22))
# Select without transaction id returns nothing, because changes
# are not commited.
execute_and_print_request(c, RequestSelect(c, 569, 0, [], 0, 1000, ITERATOR_GE))

execute_and_print_request(c, RequestCommit(c, 22))

execute_and_print_request(c, RequestSelect(c, 569, 0, [], 0, 1000, ITERATOR_GE))

# Try to send batches of data in one transaction.

execute_and_print_request(c, RequestBegin(c, 23))

replace = RequestReplace(c, 569, [1, 1], tx_id=23)
select = RequestSelect(c, 569, 0, [], 0, 1000, ITERATOR_GE, tx_id=23)
delete = RequestDelete(c, 569, 0, [1], tx_id=23)

print 'Start of the batching'

execute_and_print_request(c, select, replace, select, delete, select, replace,
                          select, delete, select)

print 'Next batch'

# First request in the batch is long, but the second doesn't
# start until the first finished.
execute_and_print_request(c, RequestCall(c, 'long_f', [], tx_id=23), replace)

execute_and_print_request(c, RequestSelect(c, 569, 0, [], 0, 1000, ITERATOR_GE,
                          tx_id=23))

print 'End of the batching'

execute_and_print_request(c, RequestCommit(c, 23))

c.close()
admin("space:drop()")

