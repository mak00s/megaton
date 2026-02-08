"""Tests for megaton.retry_utils.expo_retry."""

import pytest

from megaton.retry_utils import expo_retry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeError(Exception):
    pass


class OtherError(Exception):
    pass


def _make_func(fail_n: int, exc: type[BaseException] = FakeError, value: str = "ok"):
    """Return a callable that raises exc for the first fail_n calls, then returns value."""
    calls: list[int] = []

    def fn():
        calls.append(1)
        if len(calls) <= fail_n:
            raise exc(f"fail #{len(calls)}")
        return value

    return fn, calls


def _noop_sleep(_: float) -> None:
    """No-op sleep for fast tests."""


# ---------------------------------------------------------------------------
# Success
# ---------------------------------------------------------------------------

class TestSuccess:
    def test_returns_value_immediately(self):
        fn, calls = _make_func(0)
        result = expo_retry(fn, sleep=_noop_sleep)
        assert result == "ok"
        assert len(calls) == 1

    def test_succeeds_after_retries(self):
        fn, calls = _make_func(2)
        result = expo_retry(fn, max_retries=5, sleep=_noop_sleep, exceptions=(FakeError,))
        assert result == "ok"
        assert len(calls) == 3  # 2 fails + 1 success

    def test_succeeds_on_last_attempt(self):
        fn, calls = _make_func(2)
        result = expo_retry(fn, max_retries=3, sleep=_noop_sleep, exceptions=(FakeError,))
        assert result == "ok"
        assert len(calls) == 3


# ---------------------------------------------------------------------------
# Exhausted retries
# ---------------------------------------------------------------------------

class TestExhausted:
    def test_raises_after_max_retries(self):
        fn, calls = _make_func(10)
        with pytest.raises(FakeError, match="fail #3"):
            expo_retry(fn, max_retries=3, sleep=_noop_sleep, exceptions=(FakeError,))
        assert len(calls) == 3

    def test_single_retry_raises_immediately(self):
        fn, calls = _make_func(5)
        with pytest.raises(FakeError, match="fail #1"):
            expo_retry(fn, max_retries=1, sleep=_noop_sleep, exceptions=(FakeError,))
        assert len(calls) == 1


# ---------------------------------------------------------------------------
# Exception filtering
# ---------------------------------------------------------------------------

class TestExceptionFiltering:
    def test_non_matching_exception_not_retried(self):
        fn, calls = _make_func(2, exc=OtherError)
        with pytest.raises(OtherError):
            expo_retry(fn, max_retries=5, sleep=_noop_sleep, exceptions=(FakeError,))
        assert len(calls) == 1  # No retry

    def test_is_retryable_false_stops_retry(self):
        fn, calls = _make_func(3)
        with pytest.raises(FakeError):
            expo_retry(
                fn,
                max_retries=5,
                sleep=_noop_sleep,
                exceptions=(FakeError,),
                is_retryable=lambda _: False,
            )
        assert len(calls) == 1

    def test_is_retryable_selective(self):
        """is_retryable returns True for first fail, False for second."""
        attempt = [0]

        def fn():
            attempt[0] += 1
            raise FakeError(f"attempt {attempt[0]}")

        def retryable(exc):
            return "attempt 1" in str(exc)

        with pytest.raises(FakeError, match="attempt 2"):
            expo_retry(
                fn,
                max_retries=5,
                sleep=_noop_sleep,
                exceptions=(FakeError,),
                is_retryable=retryable,
            )
        assert attempt[0] == 2


# ---------------------------------------------------------------------------
# Backoff timing
# ---------------------------------------------------------------------------

class TestBackoff:
    def test_sleep_durations_exponential(self):
        waits: list[float] = []
        fn, _ = _make_func(3)

        expo_retry(
            fn,
            max_retries=5,
            backoff_factor=1.0,
            sleep=lambda w: waits.append(w),
            exceptions=(FakeError,),
        )
        # backoff_factor * 2^i: 1*1=1, 1*2=2, 1*4=4
        assert waits == [1.0, 2.0, 4.0]

    def test_custom_backoff_factor(self):
        waits: list[float] = []
        fn, _ = _make_func(2)

        expo_retry(
            fn,
            max_retries=3,
            backoff_factor=1.5,
            sleep=lambda w: waits.append(w),
            exceptions=(FakeError,),
        )
        assert waits == [1.5, 3.0]

    def test_max_wait_caps_sleep(self):
        waits: list[float] = []
        fn, _ = _make_func(3)

        expo_retry(
            fn,
            max_retries=5,
            backoff_factor=10.0,
            max_wait=5.0,
            sleep=lambda w: waits.append(w),
            exceptions=(FakeError,),
        )
        assert all(w <= 5.0 for w in waits)

    def test_backoff_factor_zero_means_no_wait(self):
        waits: list[float] = []
        fn, _ = _make_func(2)

        expo_retry(
            fn,
            max_retries=5,
            backoff_factor=0.0,
            sleep=lambda w: waits.append(w),
            exceptions=(FakeError,),
        )
        assert waits == [0.0, 0.0]

    def test_jitter_varies_sleep(self):
        waits: list[float] = []
        fn, _ = _make_func(20)

        try:
            expo_retry(
                fn,
                max_retries=21,
                backoff_factor=1.0,
                jitter=0.5,
                sleep=lambda w: waits.append(w),
                exceptions=(FakeError,),
            )
        except FakeError:
            pass
        # With jitter=0.5, waits are randomized — not all identical
        assert len(set(waits)) > 1


# ---------------------------------------------------------------------------
# max_elapsed
# ---------------------------------------------------------------------------

class TestMaxElapsed:
    def test_stops_when_elapsed_exceeded(self):
        clock = [0.0]

        def fake_now():
            return clock[0]

        def fake_sleep(w):
            clock[0] += w

        fn, calls = _make_func(100)

        with pytest.raises(FakeError):
            expo_retry(
                fn,
                max_retries=100,
                backoff_factor=2.0,
                max_elapsed=10.0,
                sleep=fake_sleep,
                now=fake_now,
                exceptions=(FakeError,),
            )
        # Should have stopped well before 100 retries
        assert len(calls) < 100

    def test_remaining_time_caps_wait(self):
        clock = [0.0]
        waits: list[float] = []

        def fake_now():
            return clock[0]

        def fake_sleep(w):
            waits.append(w)
            clock[0] += w

        fn, _ = _make_func(100)

        with pytest.raises(FakeError):
            expo_retry(
                fn,
                max_retries=100,
                backoff_factor=5.0,
                max_elapsed=8.0,
                sleep=fake_sleep,
                now=fake_now,
                exceptions=(FakeError,),
            )
        # All waits should be within elapsed budget
        assert sum(waits) <= 8.0


# ---------------------------------------------------------------------------
# on_retry callback
# ---------------------------------------------------------------------------

class TestOnRetry:
    def test_callback_receives_correct_args(self):
        log: list[tuple] = []
        fn, _ = _make_func(2)

        expo_retry(
            fn,
            max_retries=5,
            backoff_factor=1.0,
            sleep=_noop_sleep,
            exceptions=(FakeError,),
            on_retry=lambda attempt, mx, wait, exc: log.append((attempt, mx, wait, type(exc))),
        )
        assert len(log) == 2
        assert log[0] == (1, 5, 1.0, FakeError)
        assert log[1] == (2, 5, 2.0, FakeError)

    def test_callback_not_called_on_success(self):
        log: list[tuple] = []
        fn, _ = _make_func(0)

        expo_retry(
            fn,
            max_retries=3,
            sleep=_noop_sleep,
            on_retry=lambda *args: log.append(args),
        )
        assert len(log) == 0


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

class TestValidation:
    def test_max_retries_zero_raises(self):
        with pytest.raises(ValueError, match="must be >= 1"):
            expo_retry(lambda: None, max_retries=0)

    def test_max_retries_negative_raises(self):
        with pytest.raises(ValueError, match="must be >= 1"):
            expo_retry(lambda: None, max_retries=-1)

    def test_max_retries_non_int_raises(self):
        with pytest.raises(ValueError, match="must be an int"):
            expo_retry(lambda: None, max_retries="abc")

    def test_backoff_factor_negative_raises(self):
        with pytest.raises(ValueError, match="must be >= 0"):
            expo_retry(lambda: None, backoff_factor=-1.0)

    def test_jitter_out_of_range_raises(self):
        with pytest.raises(ValueError, match="must be in"):
            expo_retry(lambda: None, jitter=1.0)

    def test_jitter_negative_raises(self):
        with pytest.raises(ValueError, match="must be in"):
            expo_retry(lambda: None, jitter=-0.1)

    def test_max_wait_negative_raises(self):
        with pytest.raises(ValueError, match="must be >= 0"):
            expo_retry(lambda: None, max_wait=-1.0)

    def test_max_elapsed_negative_raises(self):
        with pytest.raises(ValueError, match="must be >= 0"):
            expo_retry(lambda: None, max_elapsed=-1.0)
