import pandas as pd

from megaton.transform.ga4 import classify_channel, convert_filter_to_event_scope


def test_classify_channel_ai_map_referral_group():
    df = pd.DataFrame(
        [
            {"channel": "Referral", "medium": "cpc", "source": "chatgpt.com"},
            {"channel": "Organic Search", "medium": "map", "source": "maps.google.com"},
            {"channel": "Referral", "medium": "referral", "source": "docomo.ne.jp"},
            {"channel": "Referral", "medium": "referral", "source": "threads.net"},
            {"channel": "Referral", "medium": "referral", "source": "example.com"},
            {"channel": "Direct", "medium": "direct", "source": "example.com"},
        ]
    )

    result = classify_channel(df, group_domains={"example.com"})
    assert result.tolist() == [
        "AI",
        "Map",
        "Organic Search",
        "Organic Social",
        "Group",
        "Direct",
    ]


def test_convert_filter_to_event_scope_basic():
    assert (
        convert_filter_to_event_scope("sessionDefaultChannelGroup==Organic Social")
        == "defaultChannelGroup==Organic Social"
    )


def test_convert_filter_to_event_scope_whitespace():
    assert (
        convert_filter_to_event_scope("sessionMedium == social; sessionSource == facebook")
        == "medium==social;source==facebook"
    )


def test_convert_filter_to_event_scope_manual_mapping():
    assert (
        convert_filter_to_event_scope("sessionManualAdContent==ad")
        == "manualAdContent==ad"
    )


def test_convert_filter_to_event_scope_value_is_not_rewritten():
    assert convert_filter_to_event_scope("page=~sessionMedium") == "page=~sessionMedium"
