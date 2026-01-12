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


def classify_source_channel(
    df: pd.DataFrame,
    channel_col: str = "channel",
    medium_col: str = "medium",
    source_col: str = "source",
    custom_channels=None,
) -> pd.DataFrame:
    """sourceとchannelを分類する（コア関数）
    
    megatonの既存ロジックをベースに、sourceとchannelの両方を返す版。
    AI判定は正規表現を使った網羅的なパターンマッチングを使用。
    
    Args:
        df: データフレーム
        channel_col: チャネル列名（default: "channel"）
        medium_col: メディア列名（default: "medium"）
        source_col: ソース列名（default: "source"）
        custom_channels: カスタムチャネル定義（default: None）
            {
                "チャネル名": {
                    "normalize": {"表示名": r"パターン", ...},
                    "detect": [r"パターン", ...]
                }
            }
            または簡易形式（正規表現リスト）:
            {
                "チャネル名": [r"パターン", ...]  # detectのみ、正規表現として扱われる
            }
        
    Returns:
        pd.DataFrame: 2列のDataFrame。列名はsource_col, channel_colパラメータに従う
        
    Examples:
        >>> result = classify_source_channel(df)
        >>> df[["source", "channel"]] = result
        
        # 非デフォルト列名
        >>> result = classify_source_channel(df, source_col="my_src", channel_col="my_ch")
        >>> df[["my_src", "my_ch"]] = result
        
        # Group（shibuya用）- 簡易形式は正規表現
        >>> result = classify_source_channel(
        ...     df,
        ...     custom_channels={"Group": [r"dentamap\.jp", r"haisha-yoyaku\.jp"]}
        ... )
        
        # Shiseido Internal（dei用）
        >>> result = classify_source_channel(
        ...     df,
        ...     custom_channels={
        ...         "Shiseido Internal": [
        ...             "extra\\.shiseido\\.co\\.jp",
        ...             "sharepoint",
        ...             "teams",
        ...         ]
        ...     }
        ... )
    """
    for col in [channel_col, source_col]:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
    
    # ======================
    # custom_channelsの正規化（簡易形式→完全形式）
    # ======================
    normalized_custom = {}
    if custom_channels:
        for channel_name, config in custom_channels.items():
            if isinstance(config, (list, tuple)):
                # 簡易形式: [patterns...] → {"detect": [patterns...]}
                normalized_custom[channel_name] = {
                    "normalize": {},
                    "detect": list(config),
                }
            elif isinstance(config, dict):
                # 完全形式
                normalized_custom[channel_name] = {
                    "normalize": config.get("normalize", {}),
                    "detect": config.get("detect", []),
                }
    
    # ======================
    # チャネル別パターン定義
    # ======================
    
    channel_patterns = {
        "AI": {
            "normalize": {
                "ChatGPT": r"chatgpt|chat\.openai\.com|openai\.com",
                "Copilot": r"copilot|bing\.com/chat|microsoftcopilot",
                "Gemini": r"gemini|bard|aistudio\.google\.com|makersuite\.google\.com",
                "Claude": r"claude|anthropic\.com",
                "Perplexity": r"perplexity|pplx\.ai",
            },
            "detect": [],
        },
        
        "Organic Search": {
            "normalize": {
                "docomo.ne.jp": r"service\.smt\.docomo\.ne\.jp|docomo\.ne\.jp",
                "bing": r"(?:^|\.)bing\.com$",
                "auone.jp": r"(?:^|\.)auone\.jp$",
            },
            "detect": [
                r"docomo\.ne\.jp",
                r"\.jword\.jp",
                r"jp\.hao123\.com",
                r"\bsearch\b",
            ],
        },
        
        "Organic Social": {
            "normalize": {
                "Facebook": r"facebook(\.com)?|fb\.com",
                "X": r"^t\.co$|twitter\.com|x\.com",
                "Instagram": r"^ig$|instagram\.com|\binstagram\b",
                "YouTube": r"youtube(\.com)?|youtu\.be",
                "TikTok": r"tiktok(\.com)?",
                "Threads": r"threads(\.com|\.net)?",
            },
            "detect": [],
        },
    }
    
    # カスタムチャネルをマージ
    channel_patterns.update(normalized_custom)
    
    # ======================
    # コンパイル
    # ======================
    
    # 各channelの判定用パターン（正規化 + 追加判定を統合）
    channel_detect_patterns = {}
    for channel_name, config in channel_patterns.items():
        all_patterns = list(config["normalize"].values()) + config["detect"]
        if all_patterns:
            channel_detect_patterns[channel_name] = re.compile(
                "|".join(all_patterns),
                re.IGNORECASE
            )
    
    # Source正規化用パターン（全channelの正規化を統合）
    source_normalizations = []
    for config in channel_patterns.values():
        source_normalizations.extend([
            (pattern, name) for name, pattern in config["normalize"].items()
        ])
    source_normalize_compiled = [
        (re.compile(pattern, re.IGNORECASE), replacement)
        for pattern, replacement in source_normalizations
    ]
    
    # 正規化済みSNS名のセット（高速判定用）
    normalized_sns_names = set(channel_patterns["Organic Social"]["normalize"].keys())
    
    # ======================
    # 判定関数
    # ======================
    
    def _normalize_source(src: str) -> str:
        """source文字列を正規化"""
        for pattern, replacement in source_normalize_compiled:
            if pattern.search(src):
                return replacement
        return src
    
    def _classify_row(row):
        """1行ごとに(source, channel)のタプルを返す"""
        channel = str(row.get(channel_col, ""))
        medium = str(row.get(medium_col, "")).lower()
        source_raw = str(row.get(source_col, ""))
        source = source_raw.lower().replace("www.", "")
        
        # 先に正規化
        normalized = _normalize_source(source_raw)
        
        # AI判定
        if channel_detect_patterns.get("AI") and (
            channel_detect_patterns["AI"].search(source) or 
            channel_detect_patterns["AI"].search(medium)
        ):
            return normalized, "AI"
        
        # Map判定（既存ロジック）
        if medium == "map" or "maps." in source or ".maps." in source:
            return normalized, "Map"
        
        # Referralの再分類
        if channel == "Referral":
            # Organic Search判定
            if channel_detect_patterns.get("Organic Search") and \
               channel_detect_patterns["Organic Search"].search(source):
                return normalized, "Organic Search"
            
            # Organic Social判定
            # 1. 正規化済みSNS名かチェック
            if normalized in normalized_sns_names:
                return normalized, "Organic Social"
            # 2. 追加のSNSパターン
            if channel_detect_patterns.get("Organic Social") and \
               channel_detect_patterns["Organic Social"].search(source):
                return normalized, "Organic Social"
            
            # カスタムチャネル判定（Groupなど）
            for channel_name, pattern in channel_detect_patterns.items():
                if channel_name not in ["AI", "Organic Search", "Organic Social"]:
                    if pattern.search(source):
                        return normalized, channel_name
        
        # fallback
        return normalized, channel
    
    result = df.apply(_classify_row, axis=1, result_type="expand")
    result.columns = [source_col, channel_col]
    return result


def classify_channel(
    df: pd.DataFrame,
    channel_col: str = "channel",
    medium_col: str = "medium",
    source_col: str = "source",
    custom_channels=None,
) -> pd.Series:
    """channelのみを分類（ラッパー関数）
    
    後方互換性のため、channel列のみを返すシンプル版。
    内部でclassify_source_channel()を呼び出す。
    
    Args:
        df: データフレーム
        channel_col: チャネル列名（default: "channel"）
        medium_col: メディア列名（default: "medium"）
        source_col: ソース列名（default: "source"）
        custom_channels: カスタムチャネル定義（default: None）
        
    Returns:
        pd.Series: channel列
        
    Examples:
        >>> df["channel"] = classify_channel(df)
        
        # Group指定（簡易形式：正規表現リスト）
        >>> df["channel"] = classify_channel(
        ...     df,
        ...     custom_channels={"Group": [r"dentamap\.jp", r"example\.com"]}
        ... )
    """
    result = classify_source_channel(
        df,
        channel_col=channel_col,
        medium_col=medium_col,
        source_col=source_col,
        custom_channels=custom_channels,
    )
    return result[channel_col]
