import pytest

from src.kvstore import KVStore


@pytest.fixture
def store():
    return KVStore()

@pytest.mark.parametrize("key, value", [
    ("a", "foo"),
    ("b", "bar"),
    ("c", "baz"),
])
def test_set_and_get(store, key, value):
    store.set(key, value)
    assert store.get(key) == value

def test_unset(store):
    store.set("a", "foo")
    store.unset("a")
    assert store.get("a") == "NULL"

def test_counts(store):
    store.set("a", "foo")
    store.set("b", "foo")
    store.set("c", "bar")
    assert store.counts("foo") == 2
    assert store.counts("bar") == 1
    assert store.counts("baz") == 0

def test_find(store):
    store.set("a", "foo")
    store.set("b", "foo")
    store.set("c", "bar")
    assert set(store.find("foo")) == {"a", "b"}
    assert set(store.find("bar")) == {"c"}
    assert store.find("baz") == []

def test_begin_and_rollback(store):
    store.set("a", "foo")
    store.begin()
    store.set("a", "bar")
    assert store.get("a") == "bar"
    store.rollback()
    assert store.get("a") == "foo"

def test_begin_and_commit(store):
    store.set("a", "foo")
    store.begin()
    store.set("a", "bar")
    store.commit()
    assert store.get("a") == "bar"

def test_rollback_without_transaction(store, capsys):
    result = store.rollback()
    captured = capsys.readouterr()
    assert not result
    assert "NO TRANSACTION" in captured.out

def test_commit_without_transaction(store, capsys):
    result = store.commit()
    captured = capsys.readouterr()
    assert not result
    assert "NO TRANSACTION" in captured.out

def test_nested_transactions_rollback(store):
    store.set("a", "foo")
    store.begin()
    store.set("a", "bar")
    store.begin()
    store.set("a", "baz")
    assert store.get("a") == "baz"
    store.rollback()  # Откатить до "bar"
    assert store.get("a") == "bar"
    store.rollback()  # Откатить до "foo"
    assert store.get("a") == "foo"

def test_nested_transactions_commit(store):
    store.set("a", "foo")
    store.begin()
    store.set("a", "bar")
    store.begin()
    store.set("a", "baz")
    store.commit()  # Закоммитить внутреннюю
    assert store.get("a") == "baz"
    store.rollback()  # Откатить внешнюю
    assert store.get("a") == "foo"
