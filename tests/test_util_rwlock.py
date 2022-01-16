# pylint: disable=protected-access

import pytest
import asyncio

from flurry.util import RWLock


@pytest.mark.asyncio
async def test_rw_lock_ordering():
    val = []
    rwl = RWLock()

    async def do_read(delay: float):
        async with rwl.read:
            await asyncio.sleep(delay)
            assert len(val) == 0

    async def do_read_upgrade(delay: float, *, expect):
        async with rwl.read as handle:
            assert len(val) == 0
            await asyncio.sleep(delay)
            await handle.upgrade()
            assert val == expect

    async def do_write(delay: float, new_value):
        nonlocal val
        await asyncio.sleep(delay)
        async with rwl.write:
            val = new_value

    async def do_write_downgrade(delay: float, new_value, *, expect):
        nonlocal val
        await asyncio.sleep(delay)
        async with rwl.write as handle:
            val = new_value
            await handle.downgrade()
            assert val == expect

    writes = asyncio.gather(
        do_write_downgrade(0.15, [1, 2, 3], expect=[4, 5, 6]),
        do_write(0.25, [4, 5, 6]),
    )
    reads = asyncio.gather(
        do_read(0),
        do_read(0.1),
        do_read(0.2),
        do_read_upgrade(0.3, expect=[4, 5, 6]),
        do_read(0.6),
    )
    await asyncio.gather(reads, writes)
    # make sure we can still lock for reads
    async with rwl.read:
        assert True
    # make sure we can still lock for writes
    async with rwl.write:
        assert True


@pytest.mark.asyncio
async def test_rwlock_rlock_failure():
    loop = asyncio.get_running_loop()
    rwl = RWLock()

    async def block_write(delay: float):
        async with rwl.write:
            await asyncio.sleep(delay)
            assert True

    async def do_read():
        async with rwl.read:
            assert True

    w_task = loop.create_task(block_write(1))
    r_task = loop.create_task(do_read())
    loop.call_later(0.1, r_task.cancel)
    done, pending = await asyncio.wait([w_task, r_task], timeout=2)
    assert not pending
    assert w_task in done
    assert r_task in done
    assert r_task.cancelled()
    assert not w_task.cancelled()
    assert rwl._reading == 0
    assert rwl._writing == 0
    assert not rwl._writers
    assert not rwl._readers


@pytest.mark.asyncio
async def test_rwlock_lock_failure():
    loop = asyncio.get_running_loop()
    rwl = RWLock()

    async def block_read(delay: float):
        async with rwl.read:
            await asyncio.sleep(delay)
            assert True

    async def do_write():
        async with rwl.write:
            assert True

    r_task = loop.create_task(block_read(1))
    w_task = loop.create_task(do_write())
    loop.call_later(0.1, w_task.cancel)
    done, pending = await asyncio.wait([r_task, w_task], timeout=2)
    assert not pending
    assert w_task in done
    assert r_task in done
    assert w_task.cancelled()
    assert not r_task.cancelled()
    assert rwl._reading == 0
    assert rwl._writing == 0
    assert not rwl._writers
    assert not rwl._readers
