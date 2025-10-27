import os
from collections.abc import Iterable

from .types import KeyConfig


def _parse_env_file(env_path: str) -> dict[str, str]:
    """Parse a simple .env file into a dict without modifying os.environ.

    Supports basic KEY=VALUE pairs, ignoring comments and blank lines.
    Surrounding single/double quotes are stripped if present.
    """
    values: dict[str, str] = {}
    try:
        with open(env_path) as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key:
                    values[key] = val
    except FileNotFoundError:
        # Silently ignore missing file to make the helper easy to use
        pass
    return values


def load_keyconfigs_from_env(
    names: Iterable[str] | None = None,
    prefix: str | None = None,
    per_window: tuple[int, int] | None = None,
    env_path: str | None = None,
    **kwargs,
) -> list[KeyConfig]:
    """Create KeyConfig objects from environment variables.

    - If 'names' is provided, look up each explicit env var name and create a KeyConfig
        for each found variable.
    - If 'prefix' is provided, find all env vars whose names start with the prefix and
        create a KeyConfig per match. The KeyConfig name will be the suffix after the prefix,
        optionally lowercased.
    - If both 'names' and 'prefix' are provided, results are combined.
    - If 'per_window' is provided, it will be used to set the per-window value for each KeyConfig.
    - If 'env_path' is provided, variables from the .env file will be used to augment
        lookups (without mutating the process environment). Values in the actual environment
        take precedence over the file.

    kwargs keywords:
    to_lower_names: make names lowercase (default True)
    split_commas: split comma-separated values (default True)
    strip_prefix: strip prefix from names (default True)
    """
    # Build a lookup map: actual environment takes precedence over .env file
    file_env = _parse_env_file(env_path) if env_path else {}
    env_map: dict[str, str] = {**file_env, **os.environ}

    results: list[KeyConfig] = []
    # flags with sensible defaults matching docs/tests
    split_commas = kwargs.get("split_commas", True)
    to_lower_names = kwargs.get("to_lower_names", False)
    strip_prefix = kwargs.get("strip_prefix", False)

    if names:
        for var in names:
            token = env_map.get(var)
            if not token:
                continue
            cfg_name = var.lower() if to_lower_names else var
            if split_commas and "," in token:
                for idx, part in enumerate([t.strip() for t in token.split(",") if t.strip()]):
                    results.append(
                        KeyConfig(
                            name=f"{cfg_name}_{idx + 1}",
                            token=part,
                            per_window=per_window,
                        )
                    )
            else:
                results.append(KeyConfig(name=cfg_name, token=token, per_window=per_window))

    if prefix:
        for var, token in env_map.items():
            if not (var.startswith(prefix) and token):
                continue
            name_part = var[len(prefix) :] if strip_prefix else var
            cfg_name = name_part.lower() if to_lower_names else name_part
            if split_commas and "," in token:
                for idx, part in enumerate([t.strip() for t in token.split(",") if t.strip()]):
                    results.append(
                        KeyConfig(
                            name=f"{cfg_name}_{idx + 1}",
                            token=part,
                            per_window=per_window,
                        )
                    )
            else:
                results.append(KeyConfig(name=cfg_name, token=token, per_window=per_window))

    return results
