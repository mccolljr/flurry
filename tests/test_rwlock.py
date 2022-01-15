import pytest
import asyncio

from fete.postgres.postgres import _rwlock


@pytest.mark.asyncio
async def test_rw_lock_ordering():
    val = []
    rwl = _rwlock()

    async def do_read(delay: float):
        async with rwl.rlock():
            await asyncio.sleep(delay)
            assert len(val) == 0

    async def do_read_upgrade(delay: float, *, expect):
        async with rwl.rlock() as handle:
            assert len(val) == 0
            await asyncio.sleep(delay)
            await handle.upgrade()
            assert val == expect

    async def do_write(delay: float, new_value):
        nonlocal val
        await asyncio.sleep(delay)
        async with rwl.lock():
            val = new_value

    async def do_write_downgrade(delay: float, new_value, *, expect):
        nonlocal val
        await asyncio.sleep(delay)
        async with rwl.lock() as handle:
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
    async with rwl.rlock():
        assert True
    # make sure we can still lock for writes
    async with rwl.lock():
        assert True


@pytest.mark.asyncio
async def test_rwlock_rlock_failure():
    loop = asyncio.get_running_loop()
    rwl = _rwlock()

    async def block_write(delay: float):
        async with rwl.lock():
            await asyncio.sleep(delay)
            assert True

    async def do_read():
        async with rwl.rlock():
            assert True

    w_task = loop.create_task(block_write(1))
    r_task = loop.create_task(do_read())
    loop.call_later(0.1, lambda: r_task.cancel())
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
    rwl = _rwlock()

    async def block_read(delay: float):
        async with rwl.rlock():
            await asyncio.sleep(delay)
            assert True

    async def do_write():
        async with rwl.lock():
            assert True

    r_task = loop.create_task(block_read(1))
    w_task = loop.create_task(do_write())
    loop.call_later(0.1, lambda: w_task.cancel())
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
