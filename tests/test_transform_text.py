import pandas as pd

from megaton.transform import text


def test_map_by_regex_maps_and_defaults():
    series = pd.Series([" Foo", "bar", "baz"])
    mapping = {r"foo": "X", r"bar": "Y"}
    result = text.map_by_regex(series, mapping, default="Z")
    assert result.tolist() == ["X", "Y", "Z"]


def test_clean_url_drops_query_and_hash_and_unquotes():
    series = pd.Series(
        [
            "https://example.com/Path?utm=1#frag",
            "https://example.com/%7Euser",
        ]
    )
    result = text.clean_url(series)
    assert result.tolist() == ["https://example.com/path", "https://example.com/~user"]


def test_normalize_whitespace_modes():
    series = pd.Series(["a  b", "c\t d"])
    collapsed = text.normalize_whitespace(series, mode="collapse")
    removed = text.normalize_whitespace(series, mode="remove_all")
    assert collapsed.tolist() == ["a b", "c d"]
    assert removed.tolist() == ["ab", "cd"]


def test_force_text_if_numeric_prefixes_digits():
    series = pd.Series(["123", "abc", 123])
    result = text.force_text_if_numeric(series)
    assert result.tolist() == ["'123", "abc", "'123"]


def test_infer_site_from_url_domain_match():
    sites = [
        {"clinic": "札幌", "domain": "sapporo.example.com"},
        {"clinic": "東京", "domain": "tokyo.example.com"},
    ]
    # ドメインマッチ
    assert text.infer_site_from_url("https://sapporo.example.com/page", sites, site_key="clinic") == "札幌"
    assert text.infer_site_from_url("https://tokyo.example.com/page?id=999", sites, site_key="clinic") == "東京"
    # マッチしない
    assert text.infer_site_from_url("https://unknown.example.com/page", sites, site_key="clinic") == "不明"


def test_infer_site_from_url_id_key_match():
    sites = [
        {"clinic": "札幌", "domain": "sapporo.example.com", "dentamap_id": "123"},
        {"clinic": "東京", "domain": "tokyo.example.com", "dentamap_id": "456"},
    ]
    # 特殊IDマッチ（クエリパラメータ）
    assert text.infer_site_from_url("?id=123", sites, site_key="clinic", id_key="dentamap_id") == "札幌"
    assert text.infer_site_from_url("https://example.com?id=456", sites, site_key="clinic", id_key="dentamap_id") == "東京"
    # id=12 が id=123 に誤マッチしないことを確認（部分一致防止）
    assert text.infer_site_from_url("?id=12", sites, site_key="clinic", id_key="dentamap_id") == "不明"


def test_infer_site_from_url_id_priority():
    sites = [
        {"clinic": "dentamap", "domain": "plus.dentamap.jp", "dentamap_id": "999"},
    ]
    # id_key が優先（id=999があればそのサイトで判定）
    assert text.infer_site_from_url("https://plus.dentamap.jp?id=999", sites, site_key="clinic", id_key="dentamap_id") == "dentamap"
    # 未知のドメイン + id=999 は特殊IDマッチ
    assert text.infer_site_from_url("https://unknown.com?id=999", sites, site_key="clinic", id_key="dentamap_id") == "dentamap"
    # 未知のドメイン + 未知のID は「不明」
    assert text.infer_site_from_url("https://unknown.com?id=888", sites, site_key="clinic", id_key="dentamap_id") == "不明"


def test_infer_site_from_url_invalid_input():
    sites = [{"clinic": "test", "domain": "example.com"}]
    # 空文字列
    assert text.infer_site_from_url("", sites, site_key="clinic") == "不明"
    # None
    assert text.infer_site_from_url(None, sites, site_key="clinic") == "不明"
    # 数値
    assert text.infer_site_from_url(123, sites, site_key="clinic") == "不明"


def test_infer_site_from_url_duplicate_domains():
    # 同一ドメインが複数サイトに設定されている場合、先勝ち
    sites = [
        {"clinic": "札幌", "domain": "example.com"},
        {"clinic": "東京", "domain": "example.com"},  # 重複
    ]
    # 最初に見つかった「札幌」が返される（安定順序）
    assert text.infer_site_from_url("https://example.com/page", sites, site_key="clinic") == "札幌"
