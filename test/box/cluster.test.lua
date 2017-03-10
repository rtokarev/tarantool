fiber = require('fiber')
test_run = require('test_run').new()
fio = require('fio')

server_id = box.info().server.id
tx_id = fiber.id()

test_run:cmd("setopt delimiter ';'")
function create_script(name, code)
    local path = fio.pathjoin(fio.tempdir(), name)
    local script = fio.open(path, {'O_CREAT', 'O_WRONLY'},
        tonumber('0777', 8))
    assert(script ~= nil, ("assertion: Failed to open '%s' for writing"):format(path))
    script:write(code)
    script:close()
    return path
end;

code_template = "box.cfg{ listen = %s, server_id = %s } "..
		"box.schema.user.grant('guest', 'read,write,execute', 'universe') "..
		"space = box.schema.space.create('test', { engine = 'vinyl' })"..
		"pk = space:create_index('primary')"..
		"require('console').listen(os.getenv('ADMIN'))";
test_run:cmd("setopt delimiter ''");

tmp1 = create_script('host1.lua', code_template:format(33130, 1))
tmp2 = create_script('host2.lua', code_template:format(33131, 2))
tmp3 = create_script('host3.lua', code_template:format(33132, 3))

--
-- Create shard instances.
--

test_run:cmd(("create server host1 with script='%s'"):format(tmp1))
test_run:cmd(("create server host2 with script='%s'"):format(tmp2))
test_run:cmd(("create server host3 with script='%s'"):format(tmp3))

test_run:cmd("start server host1")
test_run:cmd("start server host2")
test_run:cmd("start server host3")

--
-- Connect one to each other.
--

---------------- Host 1 ----------------

test_run:cmd('switch host1')
test_run:cmd("setopt delimiter ';'")

box.cfg{
	cluster = {
		shard1 = { uri = 'localhost:33130' },
		shard2 = { uri = 'localhost:33131' },
		shard3 = { uri = 'localhost:33132' },
	}
};

test_run:cmd("setopt delimiter ''");

box.cfg.server_id == box.info.server.id
box.cfg.server_id
box.cfg.cluster.shard1.state
box.cfg.cluster.shard2.state
box.cfg.cluster.shard3.state

---------------- Host 2 ----------------

test_run:cmd('switch host2')
test_run:cmd("setopt delimiter ';'")

box.cfg{
	cluster = {
		shard1 = { uri = 'localhost:33130' },
		shard2 = { uri = 'localhost:33131' },
		shard3 = { uri = 'localhost:33132' },
	}
};

test_run:cmd("setopt delimiter ''");

box.cfg.server_id == box.info.server.id
box.cfg.server_id
box.cfg.cluster.shard1.state
box.cfg.cluster.shard2.state
box.cfg.cluster.shard3.state

---------------- Host 3 ----------------

test_run:cmd('switch host3')
test_run:cmd("setopt delimiter ';'")

box.cfg{
	cluster = {
		shard1 = { uri = 'localhost:33130' },
		shard2 = { uri = 'localhost:33131' },
		shard3 = { uri = 'localhost:33132' },
	}
};

test_run:cmd("setopt delimiter ''");

box.cfg.server_id == box.info.server.id
box.cfg.server_id
box.cfg.cluster.shard1.state
box.cfg.cluster.shard2.state
box.cfg.cluster.shard3.state

---------------- Make two-phase transaction ----------------

test_run:cmd('switch host1')

cluster = box.cfg.cluster
cluster.shard1:begin_two_phase(box.cfg.server_id)
cluster.shard2:begin_two_phase(box.cfg.server_id)
cluster.shard3:begin_two_phase(box.cfg.server_id)

cluster.shard1.space.test:replace({1})
cluster.shard2.space.test:replace({2})
cluster.shard3.space.test:replace({3})

cluster.shard1:prepare(box.cfg.server_id)
cluster.shard2:prepare(box.cfg.server_id)
cluster.shard3:prepare(box.cfg.server_id)

cluster.shard1:commit()
cluster.shard2:commit()
cluster.shard3:commit()

cluster.shard1.space._transaction:select{}
cluster.shard2.space._transaction:select{}
cluster.shard3.space._transaction:select{}
cluster.shard1.space.test:select{}
cluster.shard2.space.test:select{}
cluster.shard3.space.test:select{}

test_run:cmd('switch default')

test_run:cmd("stop server host1")
test_run:cmd("cleanup server host1")
test_run:cmd("stop server host2")
test_run:cmd("cleanup server host2")
test_run:cmd("stop server host3")
test_run:cmd("cleanup server host3")
