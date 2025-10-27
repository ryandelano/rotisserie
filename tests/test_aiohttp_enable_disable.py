import pytest


@pytest.mark.asyncio
async def test_aiohttp_enable_disable(monkeypatch):
    import aiohttp  # noqa: PLC0415

    from rotisserie import AsyncKeyPool, KeyConfig  # noqa: PLC0415

    pool = AsyncKeyPool([KeyConfig("k1", "T1")])
    async with (
        pool.auth(endpoint="ep", reserve=1, priority=1) as auth,
        aiohttp.ClientSession() as s,
    ):  # noqa: E501
        auth.enable(s)
        assert hasattr(s, "_rotisserie_orig_get")
        auth.disable(s)
        assert not hasattr(s, "_rotisserie_orig_get")
