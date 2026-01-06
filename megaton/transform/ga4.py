from __future__ import annotations

import re
from typing import Optional

import pandas as pd


def convert_filter_to_event_scope(filter_d: Optional[str]) -> Optional[str]:
    """session系フィルタディメンションをevent系に変換
    
    GA4 APIでは、session系ディメンション（sessionDefaultChannelGroupなど）と
    event系ディメンション（defaultChannelGroupなど）でフィルタの互換性がない。
    この関数は、session系ディメンションを使ったfilter_dをevent系クエリで
    使用できるように変換する。
    
    ディメンション名のみを変換し、値や正規表現パターンは変換しない。
    
    Args:
        filter_d: フィルタ文字列（例: "sessionDefaultChannelGroup==Organic Social"）
    
    Returns:
        event系に変換されたフィルタ文字列（例: "defaultChannelGroup==Organic Social"）
        Noneまたは空文字列が渡された場合はそのまま返す
    
    Examples:
        >>> convert_filter_to_event_scope("sessionDefaultChannelGroup==Organic Social")
        'defaultChannelGroup==Organic Social'
        >>> convert_filter_to_event_scope("sessionMedium==social;sessionSource==facebook")
        'medium==social;source==facebook'
        >>> convert_filter_to_event_scope("page=~sessionMedium")
        'page=~sessionMedium'  # 値側は変換されない
    """
    if not filter_d:
        return filter_d
    
    # session系 → event系ディメンション変換
    conversions = {
        'sessionDefaultChannelGroup': 'defaultChannelGroup',
        'sessionSourceMedium': 'sourceMedium',
        'sessionMedium': 'medium',
        'sessionSource': 'source',
        'sessionCampaignId': 'campaignId',
        'sessionCampaignName': 'campaignName',
        'sessionManualTerm': 'manualTerm',
        'sessionManualSource': 'manualSource',
        'sessionManualMedium': 'manualMedium',
        'sessionManualSourceMedium': 'manualSourceMedium',
        'sessionManualCampaignId': 'manualCampaignId',
        'sessionManualCampaignName': 'manualCampaignName',
        'sessionManualAdContent': 'manualAdContent',
    }
    
    # セミコロン区切りで分割
    parts = filter_d.split(';')
    converted_parts = []

    for part in parts:
        part = part.strip()
        # 演算子で分割 (==, =~, =@, !=, !~, !@)
        match = re.match(r'^\s*([^=!]+?)\s*(==|=~|=@|!=|!~|!@)\s*(.*)$', part)
        if match:
            dim, op, value = match.groups()
            # ディメンション名のみ変換（値は変換しない）
            dim_converted = conversions.get(dim.strip(), dim.strip())
            converted_parts.append(f"{dim_converted}{op}{value}")
        else:
            # 演算子が見つからない場合はそのまま
            converted_parts.append(part)
    
    return ';'.join(converted_parts)


def classify_channel(
    df: pd.DataFrame,
    group_domains=None,
    channel_col: str = "channel",
    medium_col: str = "medium",
    source_col: str = "source",
    ai_keywords=(
        "bard",
        "chatgpt",
        "claude",
        "copilot",
        "gemini",
        "perplexity",
    ),
) -> pd.Series:
    for col in [channel_col, medium_col, source_col]:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    group_domains = set(group_domains or [])
    organic_keywords = ["search", "docomo.ne.jp", ".jword.jp", "jp.hao123.com"]
    sns_keywords = ["threads.net", "threads"]

    def _classify(row):
        channel = str(row.get(channel_col, ""))
        medium = str(row.get(medium_col, "")).lower()
        source = str(row.get(source_col, "")).lower().replace("www.", "")

        if any(keyword in source for keyword in ai_keywords) or any(keyword in medium for keyword in ai_keywords):
            return "AI"

        if medium == "map" or "maps." in source or ".maps." in source:
            return "Map"

        if channel == "Referral":
            if any(keyword in source for keyword in organic_keywords):
                return "Organic Search"
            if any(keyword in source for keyword in sns_keywords):
                return "Organic Social"
            if group_domains and any(domain in source for domain in group_domains):
                return "Group"

        return channel

    return df.apply(_classify, axis=1)
