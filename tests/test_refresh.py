"""Credential refresh: a fresh token really is fresh, and Spark restarts only when it must."""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from bench import auth
from bench.config import TOKEN_MIN_LIFETIME_SECONDS
from bench.tpch.config import ENGINES
from bench.tpch.engines import pyspark_gluten_iceberg as gluten
from bench.tpch.engines import pyspark_iceberg as spark


@pytest.fixture(autouse=True)
def _clean_auth():
    auth.reset()
    yield
    auth.reset()


def _fake_credential(monkeypatch, expires_on):
    """Patch `auth.credential`; record whether the process-wide credential was dropped first."""
    seen = []

    def credential():
        seen.append(auth._credential)
        return SimpleNamespace(
            get_token=lambda scope: SimpleNamespace(token=f"tok{len(seen)}", expires_on=expires_on)
        )

    monkeypatch.setattr(auth, "credential", credential)
    monkeypatch.setattr(auth.scrub, "register", lambda secret: None)
    return seen


def test_a_cached_token_is_reused_until_it_runs_low(monkeypatch):
    _fake_credential(monkeypatch, expires_on=10_000)
    monkeypatch.setattr(auth.time, "time", lambda: 0)
    assert auth.onelake_token() == "tok1"
    assert auth.onelake_token() == "tok1"
    assert auth.token_expires_on() == 10_000


def test_fresh_drops_both_caches(monkeypatch):
    """azure-identity hands the same token back until 5 min out; a new credential does not."""
    seen = _fake_credential(monkeypatch, expires_on=10_000)
    monkeypatch.setattr(auth.time, "time", lambda: 0)
    auth.onelake_token()
    auth._credential = object()  # what credential() would have cached
    assert auth.onelake_token(fresh=True) == "tok2"
    assert seen[-1] is None


def test_token_expiry_is_infinite_before_any_mint():
    assert auth.token_expires_on() == float("inf")


def _spark(monkeypatch, now, expires, cls=spark.PysparkIceberg):
    engine = cls(cfg=None)
    engine._expires = expires
    events = []
    monkeypatch.setattr(spark.time, "time", lambda: now)
    monkeypatch.setattr(engine, "close", lambda: events.append("close"))
    monkeypatch.setattr(spark, "_shutdown_gateway", lambda: events.append("gateway"))
    monkeypatch.setattr(
        spark.auth, "onelake_token", lambda **kw: events.append(("token", kw.get("fresh")))
    )

    def setup():
        events.append("setup")
        engine._expires = now + 3300

    monkeypatch.setattr(engine, "setup", setup)
    return engine, events


@pytest.mark.parametrize("cls", [spark.PysparkIceberg, gluten.PysparkGlutenIceberg])
def test_spark_leaves_a_session_with_time_left_alone(monkeypatch, cls):
    engine, events = _spark(monkeypatch, now=0, expires=TOKEN_MIN_LIFETIME_SECONDS + 60, cls=cls)
    engine.refresh()
    assert events == []


@pytest.mark.parametrize("cls", [spark.PysparkIceberg, gluten.PysparkGlutenIceberg])
def test_spark_restarts_on_fresh_credentials_under_the_margin(monkeypatch, cls):
    engine, events = _spark(monkeypatch, now=0, expires=TOKEN_MIN_LIFETIME_SECONDS - 60, cls=cls)
    engine.refresh()
    assert events == ["close", "gateway", ("token", True), "setup"]
    assert engine._expires == 3300
    engine.refresh()  # the new session has 55 minutes: no second restart
    assert events.count("setup") == 1


def test_gluten_watches_whichever_of_bearer_and_sas_expires_first(monkeypatch):
    engine = gluten.PysparkGlutenIceberg(cfg=SimpleNamespace(workspace_id="w", lakehouse_id="l"))
    monkeypatch.setattr(gluten, "onelake_sas", lambda w, lh, write=False: ("sas", 1_000.0))
    engine._expires = 5_000.0  # the bearer, set by the base setup just before
    engine._storage_conf({}, "onelake.dfs.fabric.microsoft.com")
    assert engine._expires == 1_000.0
    engine._expires = 500.0
    engine._storage_conf({}, "onelake.dfs.fabric.microsoft.com")
    assert engine._expires == 500.0


@pytest.mark.parametrize("name", ENGINES)
def test_every_engine_renews_its_token(name):
    """No engine is left bounded by the hour-long token it was set up with.

    LakeSail had no `refresh`, and TPC-DS SF=60 lost Q32-Q99 to `400 Bad Request` an hour in
    (run 36658601681). One rule for all of them, so the next engine cannot repeat it.
    """
    module = importlib.import_module(f"bench.tpch.engines.{name}")
    classes = [
        c for c in vars(module).values() if isinstance(c, type) and getattr(c, "name", None) == name
    ]
    assert classes, f"{name}: no engine class"
    assert callable(getattr(classes[0], "refresh", None)), f"{name} has no refresh()"
