import pandas as pd

from megaton.transform.ga4 import classify_channel


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
