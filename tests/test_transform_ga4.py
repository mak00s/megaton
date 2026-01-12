import pandas as pd

from megaton.transform.ga4 import classify_channel, classify_source_channel, convert_filter_to_event_scope


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

    result = classify_channel(df, custom_channels={"Group": ["example.com"]})
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


def test_classify_source_channel_basic():
    """source正規化とchannel分類が統合されて動作する"""
    df = pd.DataFrame(
        [
            {"channel": "Referral", "medium": "referral", "source": "chatgpt.com"},
            {"channel": "Referral", "medium": "referral", "source": "facebook.com"},
            {"channel": "Referral", "medium": "referral", "source": "service.smt.docomo.ne.jp"},
        ]
    )

    result = classify_source_channel(df)
    
    assert result["source"].tolist() == ["ChatGPT", "Facebook", "docomo.ne.jp"]
    assert result["channel"].tolist() == ["AI", "Organic Social", "Organic Search"]


def test_classify_source_channel_missing_medium():
    """medium 列がなくても動作する"""
    df = pd.DataFrame(
        [
            {"channel": "Referral", "source": "chatgpt.com"},
            {"channel": "Referral", "source": "maps.google.com"},
        ]
    )

    result = classify_source_channel(df)

    assert result["source"].tolist() == ["ChatGPT", "maps.google.com"]
    assert result["channel"].tolist() == ["AI", "Map"]


def test_classify_source_channel_custom_channels_simple():
    """custom_channelsの簡易形式が動作する"""
    df = pd.DataFrame(
        [
            {"channel": "Referral", "medium": "referral", "source": "example.com"},
            {"channel": "Referral", "medium": "referral", "source": "sub.example.com"},
            {"channel": "Referral", "medium": "referral", "source": "other.com"},
        ]
    )

    result = classify_source_channel(
        df,
        custom_channels={"Group": ["example.com", "sub.example.com"]}
    )
    
    assert result["channel"].tolist() == ["Group", "Group", "Referral"]


def test_classify_source_channel_custom_channels_full():
    """custom_channelsの完全形式が動作する"""
    df = pd.DataFrame(
        [
            {"channel": "Referral", "medium": "referral", "source": "extra.client_x.co.jp"},
            {"channel": "Referral", "medium": "referral", "source": "sharepoint"},
            {"channel": "Referral", "medium": "referral", "source": "other.com"},
        ]
    )

    result = classify_source_channel(
        df,
        custom_channels={
            "client_x Internal": {
                "normalize": {},
                "detect": [r"extra\.client_x\.co\.jp", r"sharepoint"]
            }
        }
    )
    
    assert result["channel"].tolist() == ["client_x Internal", "client_x Internal", "Referral"]


def test_classify_source_channel_non_default_columns():
    """非デフォルト列名が動作する"""
    df = pd.DataFrame(
        [
            {"my_ch": "Referral", "my_med": "referral", "my_src": "chatgpt.com"},
            {"my_ch": "Referral", "my_med": "referral", "my_src": "facebook.com"},
        ]
    )

    result = classify_source_channel(
        df,
        channel_col="my_ch",
        medium_col="my_med",
        source_col="my_src"
    )
    
    assert list(result.columns) == ["my_src", "my_ch"]
    assert result["my_src"].tolist() == ["ChatGPT", "Facebook"]
    assert result["my_ch"].tolist() == ["AI", "Organic Social"]


def test_classify_source_channel_ai_normalization():
    """AI系サービスの正規化が動作する"""
    df = pd.DataFrame(
        [
            {"channel": "Referral", "medium": "referral", "source": "chat.openai.com"},
            {"channel": "Referral", "medium": "referral", "source": "bing.com/chat"},
            {"channel": "Referral", "medium": "referral", "source": "aistudio.google.com"},
            {"channel": "Referral", "medium": "referral", "source": "anthropic.com"},
            {"channel": "Referral", "medium": "referral", "source": "perplexity.ai"},
        ]
    )

    result = classify_source_channel(df)
    
    assert result["source"].tolist() == ["ChatGPT", "Copilot", "Gemini", "Claude", "Perplexity"]
    assert all(ch == "AI" for ch in result["channel"].tolist())


def test_classify_source_channel_sns_normalization():
    """SNS系の正規化が動作する"""
    df = pd.DataFrame(
        [
            {"channel": "Referral", "medium": "referral", "source": "facebook.com"},
            {"channel": "Referral", "medium": "referral", "source": "facebook"},
            {"channel": "Referral", "medium": "referral", "source": "t.co"},
            {"channel": "Referral", "medium": "referral", "source": "x.com"},
            {"channel": "Referral", "medium": "referral", "source": "instagram.com"},
            {"channel": "Referral", "medium": "referral", "source": "youtube.com"},
            {"channel": "Referral", "medium": "referral", "source": "youtu.be"},
            {"channel": "Referral", "medium": "referral", "source": "tiktok.com"},
            {"channel": "Referral", "medium": "referral", "source": "threads.net"},
        ]
    )

    result = classify_source_channel(df)
    
    assert result["source"].tolist() == [
        "Facebook",
        "Facebook",
        "X",
        "X",
        "Instagram",
        "YouTube",
        "YouTube",
        "TikTok",
        "Threads",
    ]
    assert all(ch == "Organic Social" for ch in result["channel"].tolist())


def test_classify_source_channel_search_normalization():
    """検索エンジン系の正規化が動作する"""
    df = pd.DataFrame(
        [
            {"channel": "Referral", "medium": "referral", "source": "service.smt.docomo.ne.jp"},
            {"channel": "Referral", "medium": "referral", "source": "bing.com"},
            {"channel": "Referral", "medium": "referral", "source": "cn.bing.com"},
            {"channel": "Referral", "medium": "referral", "source": "auone.jp"},
            {"channel": "Referral", "medium": "referral", "source": "sp-web.search.auone.jp"},
        ]
    )

    result = classify_source_channel(df)
    
    assert result["source"].tolist() == ["docomo.ne.jp", "bing", "bing", "auone.jp", "auone.jp"]
    assert all(ch == "Organic Search" for ch in result["channel"].tolist())
