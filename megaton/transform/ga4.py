from __future__ import annotations

import pandas as pd


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
