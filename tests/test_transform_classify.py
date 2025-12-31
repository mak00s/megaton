import pandas as pd

from megaton.transform import classify


def test_classify_by_regex_assigns_labels():
    df = pd.DataFrame({"text": ["foo", "bar", "baz"]})
    mapping = {r"foo": "A", r"bar": "B"}
    result = classify.classify_by_regex(df, "text", mapping, "label", default="other")
    assert result["label"].tolist() == ["A", "B", "other"]


def test_infer_label_by_domain_matches_substrings():
    series = pd.Series(
        [
            "https://www.example.com/path",
            "test.org",
            None,
        ]
    )
    mapping = {"example.com": "Ex", "test.org": "Test"}
    result = classify.infer_label_by_domain(series, mapping, default="不明")
    assert result.tolist() == ["Ex", "Test", "不明"]
