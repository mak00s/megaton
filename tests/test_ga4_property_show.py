from types import SimpleNamespace

from megaton import ga4


def _make_property():
    prop = ga4.MegatonGA4.Property(SimpleNamespace())
    prop.api_metadata = {
        "dimensions": [
            {"customized": True, "api_name": "customUser:user_id", "display_name": "User ID"},
            {"customized": True, "api_name": "customEvent:article_id", "display_name": "Article ID"},
            {"customized": False, "api_name": "date", "display_name": "Date"},
        ],
        "metrics": [],
    }
    prop.api_custom_dimensions = [
        {
            "parameter_name": "user_id",
            "display_name": "User ID",
            "description": "User identifier",
            "scope": "USER",
        },
        {
            "parameter_name": "article_id",
            "display_name": "Article ID",
            "description": "Article identifier",
            "scope": "EVENT",
        },
    ]
    return prop


def test_user_properties_filters_scope_user():
    prop = _make_property()

    result = prop.user_properties

    assert len(result) == 1
    assert result[0]["api_name"] == "customUser:user_id"
    assert result[0]["scope"] == "USER"


def test_show_user_properties_returns_dataframe():
    prop = _make_property()

    df = prop.show("user_properties")

    assert list(df.index) == ["customUser:user_id"]
    assert df.loc["customUser:user_id", "parameter_name"] == "user_id"
    assert df.loc["customUser:user_id", "scope"] == "USER"
