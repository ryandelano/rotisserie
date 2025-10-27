# rotisserie

Key pooling, rotation, and **priority-aware** distribution that works with your existing HTTP stack (`requests`, `httpx`, `aiohttp`).

- Pool many API keys for the same vendor
- Reserve a baseline per endpoint (`reserve`), then redistribute freed keys by priority
- Auto-rotate on `429` via `Retry-After` or exponential cooldown
- Choose how extras are allocated: even across priorities, or funneled to the highest

> This does not help you circumvent vendor TOS. Only use multi-key if allowed.

## Install

```bash
pip install rotisserie
# Optional extras for convenience wrappers
pip install "rotisserie[requests]" "rotisserie[httpx]" "rotisserie[aiohttp]"
```

## 1) One auth to rule them all (start here)

The simplest and most flexible way to use rotisserie is the universal `auth` object.

### requests (sync)

```python
from rotisserie import KeyPool, KeyConfig
import requests

pool = KeyPool([KeyConfig("k1", "KEY_1"), KeyConfig("k2", "KEY_2")])

with pool.auth(endpoint="orders", reserve=2, priority=1) as auth:
    r = requests.get("https://api.example.com/orders", auth=auth)
    print(r.status_code)
```

### httpx (sync and async)

```python
import httpx
from rotisserie import KeyPool, KeyConfig

pool = KeyPool([KeyConfig("k1", "KEY_1"), KeyConfig("k2", "KEY_2")])

with pool.auth(endpoint="users", reserve=2, priority=1) as auth:
    with httpx.Client(auth=auth) as client:
        r = client.get("https://api.example.com/users")

# async
import asyncio, httpx
from rotisserie import AsyncKeyPool, KeyConfig

apool = AsyncKeyPool([KeyConfig("a", "A"), KeyConfig("b", "B")])

async def main():
    async with apool.auth(endpoint="events", reserve=2, priority=1) as auth:
        async with httpx.AsyncClient(auth=auth) as client:
            r = await client.get("https://api.example.com/events")
            print(r.status_code)

asyncio.run(main())
```

### aiohttp (async)

Two options:

- Minimal changes: add a trace config and patch the session for transparent retries
- Decorator style: explicit request helpers as async context managers

```python
import asyncio, aiohttp
from rotisserie import AsyncKeyPool, KeyConfig

apool = AsyncKeyPool([KeyConfig("a","A"), KeyConfig("b","B")])

async def main():
    async with apool.auth(endpoint="events", reserve=2, priority=1) as auth:
        async with aiohttp.ClientSession(trace_configs=[auth.trace_config()]) as s:
            auth.wrap_session(s)  # optional: enables automatic 429 retries using same session
            async with s.get("https://api.example.com/events") as resp:
                print(resp.status)

asyncio.run(main())
```

Decorator style (no patching):

```python
import asyncio, aiohttp
from rotisserie import AsyncKeyPool, KeyConfig

apool = AsyncKeyPool([KeyConfig("a","A"), KeyConfig("b","B")])

async def main():
    async with apool.auth(endpoint="events", reserve=2, priority=1) as auth:
        async with aiohttp.ClientSession() as s:
            async with auth.get(s, "https://api.example.com/events?limit=1000") as resp:
                print(resp.status, await resp.text())
            async with auth.request(s, "GET", "https://api.example.com/users") as resp:
                print(resp.status)

asyncio.run(main())
```

## 2) Configure how auth and retries behave

You can set defaults at the pool level and override per `auth` context.

### Global defaults via the pool

```python
from rotisserie import KeyPool, KeyConfig, AuthConfig, RetryConfig

pool = KeyPool(
    [KeyConfig("k1","K1"), KeyConfig("k2","K2")],
    # how extras are allocated across priorities if not explicitly set later
    distribute=True,
    # Auth defaults (header vs query)
    auth_config=AuthConfig(header="Authorization", scheme="Bearer", in_="header", query_param="api_key"),
    # or the shorthand fields:
    # auth_header="Authorization", auth_scheme="Bearer", auth_in="header", auth_query_param="api_key",
    # Retry defaults
    retry_config=RetryConfig(
        retry_after_base=0.5, retry_after_growth=2.0, retry_after_cap=30.0,
        error_base=0.25, error_growth=2.0, error_cap=5.0,
        retry_attempts=8, retry_for_methods=["GET", "HEAD", "OPTIONS"],
    ),
)
```

### Per-auth overrides (per block, per endpoint)

```python
from rotisserie import AuthConfig, RetryConfig

with pool.auth(
    endpoint="search", reserve=2, priority=1,
    # override how the token is sent for this endpoint
    auth_config=AuthConfig(in_="query", query_param="api_key"),
    # or shorthand: auth_in="query", auth_query_param="api_key",
    # override retry policy for this block only
    retry_config=RetryConfig(retry_attempts=4, retry_for_methods=["GET","POST","HEAD"]),
) as auth:
    ...
```

Tip: For one-shot manual injection (no automatic retry), you can do:

```python
with pool.auth(endpoint="users", reserve=1, priority=1) as auth:
    headers = auth.headers()  # {"Authorization": "Bearer <token>"}
    # use headers with any HTTP client; you must handle 429 yourself
```

## 3) Keys from environment and .env (simple and robust)

You can build keys from explicit names or by scanning a prefix. The loader merges the real environment and a `.env` file (if provided) without mutating `os.environ`. Values in the real environment win over the file.

### Using KeyPool.from_env (convenience)

```python
from rotisserie import KeyPool

# .env example:
#   API_KEY_ALPHA=a1,a2
#   API_KEY_BETA=b1
pool = KeyPool.from_env(
    prefix="API_KEY_",          # scan by prefix
    env_path=".env",            # optional
    per_window=(60, 60),         # optional per-key local window
    distribute=True,
    to_lower_names=True,        # optional kwargs
    split_commas=True,
    strip_prefix=True,
)

# Or explicit names (comma-splitting also supported when flagged)
pool2 = KeyPool.from_env(
    names=["EXAMPLE_API_KEY"],
    env_path=".env",
    to_lower_names=True,
    split_commas=True,
)
```

### Using the loader directly

```python
from rotisserie import load_keyconfigs_from_env, KeyPool

cfgs = load_keyconfigs_from_env(
    names=["A_KEY"],
    per_window=(100, 60),
    env_path=".env",
    to_lower_names=True,
    split_commas=True,
)
pool = KeyPool(cfgs)
```

Flags you can pass (as kwargs keywords):

- "to_lower_names": lowercases config names
- "split_commas": splits comma-separated tokens into multiple keys
- "strip_prefix": when scanning by prefix, drop the prefix from the resulting names

## 4) Priorities, reserves, and distribution (how keys move)

- `priority=1` is highest; lower numbers first
- `reserve` assigns a baseline of keys per endpoint
- When an endpoint exits, its keys are redistributed to others:
  - `distribute=True`: round-robin across priorities
  - `distribute=False`: funnel to highest priority first

```python
from rotisserie import KeyPool, KeyConfig

pool = KeyPool([KeyConfig(f"k{i}", f"T{i}") for i in range(6)], distribute=True)
with (
    pool.endpoint("e1", reserve=1, priority=1),
    pool.endpoint("e2", reserve=1, priority=1),
    pool.endpoint("e3", reserve=1, priority=2),
):
    ...  # keys are assigned and rebalanced automatically
```

## 5) Policies: even, weighted, or custom

Pass `policy="even"` (default), `policy="weighted"`, or a callable.

```python
import time
from rotisserie import KeyPool, KeyConfig

# Selection function: (available, n[, endpoint]) -> List[KeyState]
def pick_by_eta(available, n, endpoint):
    now = time.time()
    return sorted(
        available,
        key=lambda k: (k.next_available_at(now), -(k.remaining or 0), k.failures, k.successes),
    )[:n]

pool = KeyPool([KeyConfig("k1","T1"), KeyConfig("k2","T2")], policy=pick_by_eta)

# Key function: (key[, endpoint]) -> comparable
def eta_score(key, endpoint):
    return key.next_available_at(time.time())

pool2 = KeyPool([KeyConfig("k1","T1"), KeyConfig("k2","T2")], policy=eta_score)
```

## 6) Convenience client wrappers (optional)

Prefer the universal `auth`. If you want context-managed clients:

```python
from rotisserie import KeyPool, AsyncKeyPool, KeyConfig

pool = KeyPool([KeyConfig("k1","T1"), KeyConfig("k2","T2")])
with pool.requests_client(endpoint="users", reserve=2, priority=1) as client:
    r = client.get("https://api.example.com/users")

apool = AsyncKeyPool([KeyConfig("a","A"), KeyConfig("b","B")])
async def main():
    async with apool.httpx_client(endpoint="orders", reserve=2, priority=1) as client:
        r = await client.get("https://api.example.com/orders")
    async with apool.aiohttp_client(endpoint="events", reserve=2, priority=1) as client:
        async with client.get("https://api.example.com/events") as resp:
            print(resp.status)
```

## 7) Direct integration (custom clients)

```python
from rotisserie import KeyPool, KeyConfig
import some_custom_http

pool = KeyPool([KeyConfig("k1","T1"), KeyConfig("k2","T2")], distribute=True)

with pool.endpoint("my_endpoint", reserve=2, priority=1):
    for url in urls:
        key = pool.take_key("my_endpoint")
        try:
            resp = some_custom_http.get(url, headers={"Authorization": f"Bearer {key.token}"})
            pool.mark_result(key, getattr(resp, "status", 200), getattr(resp, "headers", {}), None)
        except Exception as e:
            pool.mark_result(key, None, {}, e)
```

## License

MIT
