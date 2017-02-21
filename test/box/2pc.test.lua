fiber = require('fiber')
test_run = require('test_run').new()

box.begin_two_phase()
box.commit() -- error, because can't commit before prepare

box.prepare_two_phase()
box.commit() -- ok, the transaction is prepared

space = box.schema.space.create('test', { engine = 'vinyl' } )
pk = space:create_index('pk')

-- Make two-phase commit of some tuples.
box.begin_two_phase()
space:replace({1})
space:replace({2})
space:select{}
box.prepare_two_phase()
box.commit()
space:select{}

-- Prepare not two-phase transaction.
box.begin()
space:replace({3})
space:replace({4})
box.prepare_two_phase()
box.rollback()
space:select{}

-- Rollback the two-phase transaction.
box.begin_two_phase()
space:replace({3})
space:replace({4})
space:select{}
box.prepare_two_phase()
box.rollback()
space:select{}

-- Rollback before prepare.
box.begin_two_phase()
space:replace({3})
space:replace({4})
space:select{}
box.rollback()
space:select{}

-- Try to change the prepared transaction.
box.begin_two_phase()
space:replace({3})
space:replace({4})
space:select{}
box.prepare_two_phase()
space:replace({5})
box.commit()
space:select{}

space:truncate()

-- Prepare can fail with read conflict

space:replace({1})
test_run:cmd("setopt delimiter ';'")
function good_tx()
    box.begin_two_phase()
    fiber.sleep(1000)
    space:update(1, {{'=', 2, 11}})
    fiber.sleep(1000)
    box.prepare_two_phase()
    fiber.sleep(1000)
    box.commit()
end;
function bad_tx()
    box.begin_two_phase()
    fiber.sleep(1000)
    space:update(1, {{'=', 2, 12}})
    fiber.sleep(1000)
    box.prepare_two_phase()
    fiber.sleep(1000)
    box.commit()
end;
test_run:cmd("setopt delimiter ''");
f1 = fiber.create(good_tx) -- begin goot_tx
f2 = fiber.create(bad_tx)  -- begin bad_tx
f1:wakeup() -- good_tx update
f2:wakeup() -- bad_tx update
f1:wakeup() -- goot_tx prepare
f2:wakeup() -- bad_tx prepare FAILED
test_run:grep_log("default", "ER_TRANSACTION_CONFLICT", 500) ~= nil
space:select{}
f1:wakeup() -- good_tx commit
f2:wakeup() -- bad_tx fiber is dead
space:select{}

space:drop()
