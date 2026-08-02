# rotisserie

Key pooling, rotation, and priority-aware distribution for `requests`, `httpx`, and
`aiohttp`.

Rotisserie is useful when one vendor gives you several API keys and you need to share
them across endpoints without writing key-selection, cooldown, or retry plumbing in
every client. Use it only where the vendor permits multiple keys.

## Install

Install the core package, then add the extra for the HTTP client you use:

```bash
pip install rotisserie
pip install "rotisserie[requests]"   # requests integration
pip install "rotisserie[httpx]"       # httpx integration
pip install "rotisserie[aiohttp]"     # aiohttp integration
```

## Quick start

`from_tokens()` is the shortest path when you already have token values. Names are
generated automatically, or you can pass a `{name: token}` mapping.

```python
import requests
from rotisserie import KeyPool

pool = KeyPool.from_tokens({"primary": "KEY_1", "backup": "KEY_2"})

with pool.auth(endpoint="orders", reserve=1) as auth:
    response = requests.get("https://api.example.com/orders", auth=auth)
    response.raise_for_status()
```

The `auth` context registers the endpoint, applies its reserve and priority, selects a
key for each request, records the response, and rotates on retryable `429` responses.
Keep one auth context open for the lifetime of the client or request batch that uses
that endpoint.

## Choose your integration

| HTTP client | Recommended entry point | Request style |
| --- | --- | --- |
| `requests` | `pool.auth(...)` | `requests.get(..., auth=auth)` |
| `httpx` sync | `pool.auth(...)` | `httpx.Client(auth=auth)` |
| `httpx` async | `async_pool.auth(...)` | `httpx.AsyncClient(auth=auth)` |
| `aiohttp` | `async_pool.auth(...)` | `auth.get(...)`, `auth.request(...)`, or `wrap_session(...)` |
| custom client | `pool.endpoint(...)` | `take_key()` + `mark_result()` |

### requests

```python
import requests
from rotisserie import KeyPool

pool = KeyPool.from_tokens(["KEY_1", "KEY_2"])

with pool.auth("orders", reserve=2, priority=1) as auth:
    response = requests.get(
        "https://api.example.com/orders",
        auth=auth,
        timeout=10,
    )
```

For a client that already owns a `requests.Session`, use the convenience wrapper:

```python
session = requests.Session()
with pool.requests_client("orders", reserve=2, session=session) as client:
    response = client.get("https://api.example.com/orders", timeout=10)
```

### httpx, sync or async

The universal auth object implements HTTPX's auth protocol, including retrying
configured methods after a `429`.

```python
import httpx
from rotisserie import KeyPool

pool = KeyPool.from_tokens(["KEY_1", "KEY_2"])

with pool.auth("users") as auth, httpx.Client(auth=auth) as client:
    response = client.get("https://api.example.com/users")
```

Async usage is the same shape with `AsyncKeyPool` and `async with`:

```python
import httpx
from rotisserie import AsyncKeyPool

pool = AsyncKeyPool.from_tokens(["KEY_1", "KEY_2"])

async def fetch_users():
    async with pool.auth("users") as auth, httpx.AsyncClient(auth=auth) as client:
        return await client.get("https://api.example.com/users")
```

If you prefer an explicit managed wrapper, use `pool.httpx_client(...)`:

```python
async with pool.httpx_client("users", reserve=2) as client:
    response = await client.get("https://api.example.com/users")
```

### aiohttp

For explicit request helpers, no session monkey-patching is needed:

```python
import aiohttp
from rotisserie import AsyncKeyPool

pool = AsyncKeyPool.from_tokens(["KEY_1", "KEY_2"])

async def fetch_events():
    async with pool.auth("events") as auth, aiohttp.ClientSession() as session:
        async with auth.get(session, "https://api.example.com/events") as response:
            return await response.json()
```

For an existing session, `wrap_session()` transparently injects credentials and retries
configured methods while preserving that session's connector:

```python
async with pool.auth("events") as auth, aiohttp.ClientSession() as session:
    auth.wrap_session(session)
    async with session.get("https://api.example.com/events") as response:
        ...
    auth.unwrap_session(session)  # optional; useful before reusing the session elsewhere
```

`trace_config()` is a lightweight header-injection/accounting option when using header
auth. Aiohttp trace parameters cannot be mutated to add a query string; for query auth,
use `auth.request()`/`auth.get()` or `wrap_session()` instead.

## Configure authentication and retries

Pool constructor options are explicit and editor-friendly:

```python
from rotisserie import AuthConfig, KeyPool, RetryConfig

pool = KeyPool.from_tokens(
    ["KEY_1", "KEY_2"],
    auth_config=AuthConfig(
        in_="query",
        query_param="api_key",
    ),
    retry_config=RetryConfig(
        retry_attempts=4,
        retry_for_methods=["GET", "HEAD"],
        retry_after_cap=30,
        error_cap=5,
    ),
)
```

Supported auth fields are `header`, `scheme`, `in_` (`"header"` or `"query"`), and
`query_param`. You can use shorthand pool options such as `auth_header="X-API-Key"`
and `auth_in="query"`. Per-endpoint overrides are passed to `pool.auth(...)`:

```python
with pool.auth(
    "search",
    reserve=2,
    priority=1,
    auth_in="query",
    auth_query_param="api_key",
    retry_attempts=2,
) as auth:
    ...
```

By default only `GET`, `HEAD`, and `OPTIONS` retry on `429`. Add `POST` or another
method explicitly only when repeating that request is safe for your API.

## Load keys from environment variables

`from_env()` merges the real environment with an optional `.env` file. Real environment
values win, and the loader does not mutate `os.environ`.

```bash
# .env
VENDOR_API_KEYS=KEY_1,KEY_2
```

```python
from rotisserie import KeyPool

pool = KeyPool.from_env(
    prefix="VENDOR_",
    env_path=".env",
    split_commas=True,
    strip_prefix=True,
    to_lower_names=True,
)
```

The loader also supports explicit variables:

```python
pool = KeyPool.from_env(
    names=["VENDOR_PRIMARY", "VENDOR_BACKUP"],
    env_path=".env",
)
```

Use `per_window=(limit, seconds)` when you want local throttling before the vendor
returns a `429`:

```python
pool = KeyPool.from_env(prefix="VENDOR_", per_window=(60, 60))
```

## Reserves, priorities, and distribution

- Lower `priority` numbers are more important; `priority=1` is highest.
- `reserve` is the preferred baseline number of keys for an endpoint.
- `distribute=True` spreads extra free keys across priority groups.
- `distribute=False` funnels extra free keys to the highest-priority endpoint first.

```python
from rotisserie import KeyPool

pool = KeyPool.from_tokens([f"TOKEN_{i}" for i in range(6)], distribute=True)
with pool.endpoint("critical", reserve=1, priority=1):
    with pool.endpoint("reports", reserve=1, priority=2):
        ...
```

## Policies

Use `policy="even"` (the default), `policy="weighted"`, or a callable. A selection
callable receives `(available, n)` or `(available, n, endpoint)` and returns a list of
keys. A ranking callable receives `(key)` or `(key, endpoint)` and returns a comparable
score.

```python
import time
from rotisserie import KeyPool

def soonest_available(key, endpoint):
    return key.next_available_at(time.time())

pool = KeyPool.from_tokens(["KEY_1", "KEY_2"], policy=soonest_available)
```

## Custom HTTP clients

Use the low-level API when your client is not one of the built-in integrations:

```python
from rotisserie import KeyPool

pool = KeyPool.from_tokens({"one": "KEY_1", "two": "KEY_2"})

with pool.endpoint("orders", reserve=1, priority=1):
    key = pool.take_key("orders")
    try:
        response = my_client.get(
            "https://api.example.com/orders",
            headers={"Authorization": f"Bearer {key.token}"},
        )
    except Exception as error:
        pool.mark_result(key, None, {}, error)
    else:
        pool.mark_result(
            key,
            response.status,
            dict(response.headers),
            None,
        )
```

Always call `mark_result()` exactly once for every key acquired with `take_key()`.
The result is what releases the key and updates cooldown/work statistics.

## Troubleshooting

- `unexpected pool option(s)` means a pool option is misspelled or unsupported.
- `cannot acquire a key from an empty pool` means environment lookup returned no keys.
- Install the matching optional extra when an integration reports a missing dependency.
- Query auth with aiohttp requires `auth.request()`/`auth.get()` or `wrap_session()`;
  `trace_config()` only supports header injection.

## License

MIT
