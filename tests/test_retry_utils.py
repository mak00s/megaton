import pytest

from megaton import retry_utils


def test_expo_retry_succeeds_after_retries():
    calls = {"n": 0}
    waits = []

    def func():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("temporary")
        return "ok"

    result = retry_utils.expo_retry(
        func,
        max_retries=3,
        backoff_factor=1.5,
        exceptions=(RuntimeError,),
        sleep=waits.append,
    )

    assert result == "ok"
    assert calls["n"] == 3
    assert waits == [1.5, 3.0]


def test_expo_retry_raises_after_exhaustion():
    waits = []

    def func():
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        retry_utils.expo_retry(
            func,
            max_retries=3,
            backoff_factor=2.0,
            exceptions=(RuntimeError,),
            sleep=waits.append,
        )

    assert waits == [2.0, 4.0]

