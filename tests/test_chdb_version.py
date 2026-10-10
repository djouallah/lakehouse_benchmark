from bench import chdb_version


def test_chdb_version_includes_both_distributions_and_selected_source(monkeypatch):
    versions = {"chdb": "4.5.0", "chdb-core": "26.9.2"}
    monkeypatch.setattr(chdb_version, "version", versions.__getitem__)
    monkeypatch.setenv("CHDB_CORE_SOURCE", "release:v26.9.2-rc.2")

    assert chdb_version.chdb_version() == ("4.5.0 / core 26.9.2 (release:v26.9.2-rc.2)")
