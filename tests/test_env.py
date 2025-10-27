from rotisserie import load_keyconfigs_from_env


def test_env_names_and_comma_split(monkeypatch, tmp_path):
    # direct env
    monkeypatch.setenv("A_KEY", "tok1,tok2 , tok3")
    cfgs = load_keyconfigs_from_env(names=["A_KEY"], split_commas=True)
    names = [c.name for c in cfgs]
    tokens = [c.token for c in cfgs]
    assert names == ["A_KEY_1", "A_KEY_2", "A_KEY_3"]
    assert tokens == ["tok1", "tok2", "tok3"]

    # .env
    envp = tmp_path / ".env"
    envp.write_text("B_PREFIX_ALPHA=x1,x2\nB_PREFIX_BETA=y1\n")
    cfgs2 = load_keyconfigs_from_env(
        prefix="B_PREFIX_", env_path=str(envp), to_lower_names=True, strip_prefix=True
    )
    names2 = sorted(c.name for c in cfgs2)
    tokens2 = sorted(c.token for c in cfgs2)
    assert names2 == ["alpha_1", "alpha_2", "beta"]
    assert tokens2 == ["x1", "x2", "y1"]
