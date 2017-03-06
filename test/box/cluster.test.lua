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

code_template = "box.cfg{ listen = %s } "..
		"box.schema.user.grant('guest', 'read,write,execute', 'universe') "..
		"require('console').listen(os.getenv('ADMIN'))";
test_run:cmd("setopt delimiter ''");

tmp1 = create_script('host1.lua', code_template:format(33130))
tmp2 = create_script('host2.lua', code_template:format(33131))
tmp3 = create_script('host3.lua', code_template:format(33132))

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

box.cfg.cluster.shard1.state
box.cfg.cluster.shard2.state
box.cfg.cluster.shard3.state

test_run:cmd('switch default')

test_run:cmd("stop server host1")
test_run:cmd("cleanup server host1")
test_run:cmd("stop server host2")
test_run:cmd("cleanup server host2")
test_run:cmd("stop server host3")
test_run:cmd("cleanup server host3")
