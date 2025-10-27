from rotisserie import KeyPool, load_keyconfigs_from_env


def test_loader_defaults_and_kwargs(monkeypatch, tmp_path):
    # Defaults: lower, split, strip
    monkeypatch.setenv("API_KEY_ALPHA", "x1,x2")
    cfgs = load_keyconfigs_from_env(prefix="API_KEY_")
    names = sorted(c.name for c in cfgs)
    tokens = sorted(c.token for c in cfgs)
    assert names == ["API_KEY_ALPHA_1", "API_KEY_ALPHA_2"]
    assert tokens == ["x1", "x2"]

    # Override via kwargs
    cfgs2 = load_keyconfigs_from_env(prefix="API_KEY_", to_lower_names=True, strip_prefix=True)
    names2 = sorted(c.name for c in cfgs2)
    assert names2 == ["alpha_1", "alpha_2"]


def test_pool_from_env_kwargs(monkeypatch, tmp_path):
    # .env file precedence: env overrides file
    envp = tmp_path / ".env"
    envp.write_text("APPKEY_A=a1,a2\n")
    monkeypatch.setenv("APPKEY_A", "b1")

    pool = KeyPool.from_env(prefix="APPKEY_", env_path=str(envp))
    # default split_commas=True but env overrides file; only b1 remains
    assert len(pool._keys) == 1
    assert pool._keys[0].token == "b1"

    pool2 = KeyPool.from_env(names=["APPKEY_B"], env_path=str(envp), split_commas=True)
    # Missing var yields no keys
    assert isinstance(pool2, KeyPool)
    assert len(pool2._keys) == 0
