"""Tests for megaton.transform.traffic (promoted from megaton_lib/traffic.py)."""
import pandas as pd

from megaton.transform import (
    apply_source_normalization,
    ensure_trailing_slash,
    is_non_public_dev_source,
    normalize_domain,
    source_host,
)


class TestNormalizeDomain:
    def test_strips_scheme_www_and_path(self):
        assert normalize_domain("https://www.example.com/path?q=1") == "example.com"

    def test_plain_domain(self):
        assert normalize_domain("Example.COM") == "example.com"


class TestSourceHost:
    def test_plain(self):
        assert source_host("example.com") == "example.com"

    def test_scheme_path_query(self):
        assert source_host("https://www.example.com/a/b?q=1") == "example.com"

    def test_userinfo_and_port(self):
        assert source_host("user@example.com:8080") == "example.com"

    def test_ipv6_bracket(self):
        assert source_host("[::1]:443") == "::1"

    def test_placeholders_empty(self):
        assert source_host("(not set)") == ""
        assert source_host(None) == ""


class TestIsNonPublicDevSource:
    def test_localhost(self):
        assert is_non_public_dev_source("localhost") is True
        assert is_non_public_dev_source("app.localhost") is True

    def test_private_ip(self):
        assert is_non_public_dev_source("192.168.1.10") is True
        assert is_non_public_dev_source("http://10.0.0.5:3000/") is True

    def test_public(self):
        assert is_non_public_dev_source("example.com") is False
        assert is_non_public_dev_source("8.8.8.8") is False


class TestEnsureTrailingSlash:
    def test_appends(self):
        assert ensure_trailing_slash("/blog") == "/blog/"

    def test_preserves(self):
        assert ensure_trailing_slash("/index.html") == "/index.html"
        assert ensure_trailing_slash("/blog/") == "/blog/"


class TestApplySourceNormalization:
    def test_normalizes_with_regex_map(self):
        df = pd.DataFrame({"source": ["WWW.Google.com", "yahoo.co.jp", "other.site"]})
        out = apply_source_normalization(df, {r"google": "google", r"yahoo": "yahoo"})
        assert out["source"].tolist() == ["google", "yahoo", "other.site"]

    def test_missing_column_passthrough(self):
        df = pd.DataFrame({"x": [1]})
        assert apply_source_normalization(df, {"a": "b"}) is df

    def test_source_is_not_mutated(self):
        df = pd.DataFrame({"source": ["GOOGLE.com"]})
        apply_source_normalization(df, {r"google": "google"})
        assert df["source"].tolist() == ["GOOGLE.com"]
