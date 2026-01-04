from __future__ import annotations

import re
from urllib.parse import unquote as url_unquote
from urllib.parse import urlsplit, urlunsplit, urlparse, parse_qs

import pandas as pd


def map_by_regex(series, mapping, default=None, flags=0, lower=True, strip=True):
    if series is None or not mapping:
        return series

    def _map_value(value):
        if not isinstance(value, str):
            return value
        text = value
        if strip:
            text = text.strip()
        if lower:
            text = text.lower()
        for pattern, mapped in mapping.items():
            try:
                if re.search(pattern, text, flags=flags):
                    return mapped
            except re.error:
                continue
        return value if default is None else default

    return series.apply(_map_value)


def clean_url(series, unquote=True, drop_query=True, drop_hash=True, lower=True):
    if series is None:
        return series

    def _clean(value):
        if not isinstance(value, str):
            return value
        text = value.strip()
        if not text:
            return text
        parsed = urlsplit(text)
        path = url_unquote(parsed.path) if unquote else parsed.path
        query = "" if drop_query else parsed.query
        fragment = "" if drop_hash else parsed.fragment
        cleaned = urlunsplit((parsed.scheme, parsed.netloc, path, query, fragment))
        return cleaned.lower() if lower else cleaned

    return series.apply(_clean)


def normalize_whitespace(series, mode="remove_all"):
    if series is None:
        return series
    if mode not in {"remove_all", "collapse"}:
        raise ValueError(f"Unsupported mode: {mode}")

    def _normalize(value):
        if not isinstance(value, str):
            return value
        if mode == "remove_all":
            return re.sub(r"\s+", "", value)
        return re.sub(r"\s+", " ", value).strip()

    return series.apply(_normalize)


def force_text_if_numeric(series, prefix="'"):
    if series is None:
        return series

    def _force(value):
        if pd.isna(value):
            return value
        text = str(value)
        if re.fullmatch(r"\d+", text):
            return f"{prefix}{text}"
        return value

    return series.apply(_force)


def infer_site_from_url(url_val, sites, site_key='site', id_key=None):
    """URLから所属サイトを推測（マルチサイト企業対応）
    
    Args:
        url_val: URL文字列（LP URL、page_location など）
        sites: サイト設定リスト（各要素は dict）
        site_key: サイト識別子のキー名（例: 'site', 'clinic', 'brand'）
        id_key: 特殊ID用のキー名（例: 'dentamap_id'）。Noneなら無視
    
    Returns:
        str: サイト識別子 or "不明"
    
    Example:
        >>> infer_site_from_url('https://example.com/page', sites, site_key='clinic')
        'clinic_a'
        >>> infer_site_from_url('?id=123', sites, site_key='clinic', id_key='dentamap_id')
        'dentamap'
    """
    # 値が空またはstringでなければ「不明」を返す
    if not isinstance(url_val, str) or not url_val:
        return "不明"
    
    # 特殊IDチェック（id=XXX パターン）
    if id_key:
        parsed = urlparse(url_val if "://" in url_val else f"http://{url_val}")
        query_params = parse_qs(parsed.query)
        id_values = query_params.get('id', [])
        
        for site in sites:
            special_id = site.get(id_key)
            if special_id and site.get(site_key) and str(special_id) in id_values:
                return site.get(site_key)
    
    # ドメインからサイトを推測
    # sites から domain_pairs を動的生成（長さ順にソート）
    domain_pairs = []
    seen_domains = set()
    for site in sites:
        site_id = site.get(site_key)
        if not site_id:
            continue
        raw_domain = site.get("domain")
        raw_url = site.get("url")
        candidates = []
        if raw_domain:
            candidates.append(raw_domain.lower())
        if raw_url:
            parsed = urlparse(str(raw_url))
            if parsed.netloc:
                candidates.append(parsed.netloc.lower())
            else:
                candidates.append(str(raw_url).lower())
        # 重複ドメインは先勝ち（最初に見つかったサイトを使用）
        for domain in candidates:
            if domain not in seen_domains:
                domain_pairs.append((domain, site_id))
                seen_domains.add(domain)
    
    # 長いドメインを優先（サブドメインを先にマッチさせる）
    domain_pairs = sorted(domain_pairs, key=lambda x: len(x[0]), reverse=True)
    
    parsed = urlparse(url_val if "://" in url_val else f"http://{url_val}")
    domain = parsed.netloc.lower()
    for key, name in domain_pairs:
        if domain == key or domain.endswith(key):
            return name
    
    # マッチしない場合は「不明」
    return "不明"
