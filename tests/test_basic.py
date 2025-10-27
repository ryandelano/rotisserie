import pytest

from rotisserie import AsyncKeyPool, KeyConfig, KeyPool


def test_construct_sync():
    KeyPool([KeyConfig(name="k1", token="t1"), KeyConfig(name="k2", token="t2")])


@pytest.mark.asyncio
async def test_construct_async():
    AsyncKeyPool([KeyConfig(name="k1", token="t1")])
