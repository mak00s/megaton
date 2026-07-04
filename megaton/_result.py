"""Result objects for GA4 reports and Search Console queries.

``SearchResult`` and ``ReportResult`` are the fluent, chainable
post-processing objects returned by ``mg.search.run`` / ``mg.report.run``.
Extracted from ``start.py`` to keep that module focused; the public import
paths (``from megaton.start import SearchResult`` / ``ReportResult``) are
preserved by re-export from ``start``.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Optional, Self
from urllib.parse import urlparse

import pandas as pd

from . import errors

if TYPE_CHECKING:  # type hints only; avoids a start <-> _result import cycle
    from .start import Megaton  # noqa: F401


# Public API surface. ``_ResultBase``, ``_extract_df`` and the ``KNOWN_GA4_*``
# constants are internal.
__all__ = ["SearchResult", "ReportResult", "MappingRule", "wrap"]


# GA4の既知のメトリクス名（標準 + よく使われるカスタムメトリクス）
KNOWN_GA4_METRICS = {
    # 標準メトリクス
    'sessions', 'users', 'newUsers', 'activeUsers',
    'engagedSessions', 'engagementRate', 'totalRevenue',
    'averageSessionDuration', 'screenPageViews', 'eventCount',
    'conversions', 'sessionConversionRate', 'bounceRate',
    'totalPurchasers', 'purchaseRevenue', 'itemRevenue',
    'transactions', 'totalUsers', 'eventCountPerUser',
    # よく使われるカスタムメトリクス（エイリアス）
    'cv', 'ad_cost', 'cost', 'impressions', 'clicks',
}

# GA4の既知のディメンション名（数値化され得るディメンションを保護）
KNOWN_GA4_DIMENSIONS = {
    # 日付・時間系（数値型になり得る）
    'date', 'month', 'yearMonth', 'year', 'week', 'day',
    'dateHour', 'dateHourMinute', 'dayOfWeek', 'dayOfWeekName',
    # その他の一般的なディメンション
    'sessionSource', 'sessionMedium', 'sessionCampaignName',
    'deviceCategory', 'country', 'city', 'landingPage', 'pagePath',
}

# SearchResult / ReportResult の normalize / categorize / classify で使うマッピング型
# callable は、正規化済みの値（基本は str だが念のため object）を受け取り、置換後の値を返す想定。
MappingRule = dict[str, str] | Callable[[object], object | None]


class _ResultBase:
    """Shared chainable transforms for SearchResult/ReportResult.

    Subclasses implement ``_with_df(df, dimensions)`` to clone themselves
    with a new DataFrame; the shared methods below stay implementation-free
    of the concrete result type.
    """

    def _with_df(self, df: pd.DataFrame, dimensions: list[str]) -> Self:
        raise NotImplementedError

    def _normalize_value(self, value: object, *, lower: bool, strip: bool) -> object:
        if pd.isna(value):
            return value
        if not isinstance(value, str):
            return value
        text = value
        if strip:
            text = text.strip()
        if lower:
            text = text.lower()
        return text

    def _map_value(self, value: object, by: MappingRule, *, default: str | None = None) -> object:
        if pd.isna(value):
            return default if default is not None else value
        if callable(by):
            mapped = by(value)
            return default if mapped is None else mapped
        if isinstance(by, dict):
            if isinstance(value, str):
                for pattern, mapped in by.items():
                    try:
                        if re.search(pattern, value):
                            return mapped
                    except re.error:
                        continue
            return default if default is not None else value
        raise TypeError("by must be a dict or callable.")

    def normalize(self, dimension: str, by: MappingRule, *, lower: bool = True, strip: bool = True) -> Self:
        """
        既存ディメンションの値を正規化（上書き、集約なし）
        """
        df = self._df.copy()
        if dimension not in df.columns:
            raise ValueError(f"Column '{dimension}' not found in DataFrame")

        def _apply(value):
            normalized = self._normalize_value(value, lower=lower, strip=strip)
            return self._map_value(normalized, by, default=None)

        df[dimension] = df[dimension].apply(_apply)
        return self._with_df(df, self.dimensions)

    def categorize(self, dimension: str, by: MappingRule, *, into: str | None = None, default: str = "(other)") -> Self:
        """
        既存ディメンションからカテゴリ列を追加（集約なし）
        """
        df = self._df.copy()
        if dimension not in df.columns:
            raise ValueError(f"Column '{dimension}' not found in DataFrame")

        if into is None:
            into = f"{dimension}_category"

        df[into] = df[dimension].apply(lambda value: self._map_value(value, by, default=default))

        new_dimensions = list(self.dimensions)
        if into not in new_dimensions:
            new_dimensions.append(into)
        return self._with_df(df, new_dimensions)


class SearchResult(_ResultBase):
    """Search Console データをラップし、メソッドチェーンで処理を行うクラス"""

    def __init__(self, df: pd.DataFrame, parent: Megaton.Search, dimensions: list[str]) -> None:
        """
        Args:
            df: pandas DataFrame
            parent: Search インスタンス
            dimensions: ディメンションのリスト（例: ['query', 'page']）
        """
        self._df = df
        self.parent = parent
        self.dimensions = dimensions

    @property
    def df(self) -> pd.DataFrame:
        """DataFrame として直接アクセス（後方互換性）"""
        return self._df

    def _aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        """dimensions に基づいて集計 (位置は重み付き平均、他は合計)"""
        return self._aggregate_gsc(df, self.dimensions)

    def _aggregate_gsc(self, df: pd.DataFrame, dims: list[str]) -> pd.DataFrame:
        """GSC データを集計 (位置は重み付き平均、CTR は再計算、他は合計)"""
        if df.empty:
            return df
        
        # 位置の重み付き処理
        if 'position' in df.columns and 'impressions' in df.columns:
            df = df.copy()
            df['weighted_position'] = df['position'] * df['impressions']
        
        # 集計対象の指標列を特定（CTR は除外して後で再計算）
        metric_cols = [col for col in ['clicks', 'impressions'] if col in df.columns]
        if 'weighted_position' in df.columns:
            metric_cols.append('weighted_position')
        
        if not metric_cols:
            # 指標列がない場合はそのまま返す
            return df
        
        # 集計
        grouped = df.groupby(dims, as_index=False)[metric_cols].sum()
        
        # 位置の計算
        if 'weighted_position' in grouped.columns:
            grouped['position'] = (grouped['weighted_position'] / grouped['impressions']).round(6)
            grouped = grouped.drop(columns=['weighted_position'])
        
        # CTR の再計算（元データに ctr 列があった場合のみ）
        if 'ctr' in df.columns and 'clicks' in grouped.columns and 'impressions' in grouped.columns:
            grouped['ctr'] = (grouped['clicks'] / grouped['impressions'].replace(0, float('nan'))).fillna(0)
        
        return grouped
    
    def decode(self, group: bool = True) -> Self:
        """
        URL デコード（%xx → 文字）

        Args:
            group: True の場合、dimensions で集計（default: True）

        Returns:
            SearchResult
        """
        from urllib.parse import unquote
        
        df = self._df.copy()
        
        # query, page 列が存在する場合にデコード
        if 'query' in df.columns:
            df['query'] = df['query'].apply(lambda x: unquote(str(x)) if pd.notna(x) else x)
        if 'page' in df.columns:
            df['page'] = df['page'].apply(lambda x: unquote(str(x)) if pd.notna(x) else x)
        
        if group:
            df = self._aggregate(df)
        
        return SearchResult(df, self.parent, self.dimensions)
    
    def remove_params(self, keep: list[str] | None = None, group: bool = True) -> Self:
        """
        クエリパラメータを削除

        Args:
            keep: 保持するパラメータのリスト（例: ['utm_source']）
            group: True の場合、dimensions で集計（default: True）

        Returns:
            SearchResult
        """
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        
        df = self._df.copy()
        
        if 'page' in df.columns:
            def clean_params(url):
                if pd.isna(url):
                    return url
                parsed = urlparse(str(url))
                if keep:
                    # keep リストのパラメータのみ保持
                    params = parse_qs(parsed.query)
                    kept_params = {k: v for k, v in params.items() if k in keep}
                    new_query = urlencode(kept_params, doseq=True)
                    return urlunparse(parsed._replace(query=new_query))
                else:
                    # 全パラメータを削除
                    return urlunparse(parsed._replace(query=''))
            
            df['page'] = df['page'].apply(clean_params)
        
        if group:
            df = self._aggregate(df)
        
        return SearchResult(df, self.parent, self.dimensions)
    
    def remove_fragment(self, group: bool = True) -> Self:
        """
        # 以降のフラグメントを削除

        Args:
            group: True の場合、dimensions で集計（default: True)

        Returns:
            SearchResult
        """
        from urllib.parse import urlparse, urlunparse
        
        df = self._df.copy()
        
        if 'page' in df.columns:
            def clean_fragment(url):
                if pd.isna(url):
                    return url
                parsed = urlparse(str(url))
                return urlunparse(parsed._replace(fragment=''))
            
            df['page'] = df['page'].apply(clean_fragment)
        
        if group:
            df = self._aggregate(df)
        
        return SearchResult(df, self.parent, self.dimensions)

    def clean_url(
        self,
        dimension: str = 'page',
        *,
        unquote: bool = True,
        drop_query: bool = True,
        drop_hash: bool = True,
        lower: bool = True,
        group: bool = True,
    ) -> Self:
        """
        URL列を正規化（URLデコード、クエリ/フラグメント削除、小文字化）

        Args:
            dimension: 対象ディメンション列名（default: 'page'）
            unquote: URLデコードするか（default: True）
            drop_query: クエリパラメータを削除（default: True）
            drop_hash: フラグメントを削除（default: True）
            lower: 小文字化（default: True）
            group: True の場合、dimensions で集計（default: True）

        Returns:
            SearchResult
        """
        from megaton.transform.text import clean_url

        df = self._df.copy()

        if dimension not in df.columns:
            raise ValueError(f"Column '{dimension}' not found in DataFrame")

        df[dimension] = clean_url(
            df[dimension],
            unquote=unquote,
            drop_query=drop_query,
            drop_hash=drop_hash,
            lower=lower,
        )

        if group:
            df = self._aggregate(df)

        return SearchResult(df, self.parent, self.dimensions)

    def lower(self, columns: list[str] | None = None, group: bool = True) -> Self:
        """
        指定列を小文字化

        Args:
            columns: 小文字化する列のリスト（default: ['page']）
            group: True の場合、dimensions で集計（default: True）

        Returns:
            SearchResult
        """
        if columns is None:
            columns = ['page']
        df = self._df.copy()
        
        for col in columns:
            if col in df.columns:
                df[col] = df[col].str.lower()
        
        if group:
            df = self._aggregate(df)
        
        return SearchResult(df, self.parent, self.dimensions)
    
    def _with_df(self, df: pd.DataFrame, dimensions: list[str]) -> "SearchResult":
        return SearchResult(df, self.parent, dimensions)

    def classify(self, dimension: str, by: MappingRule, *, lower: bool = True, strip: bool = True) -> Self:
        """
        正規化 + 集約（ディメンション上書き、常に集約）
        """
        df = self._df.copy()
        if dimension not in df.columns:
            raise ValueError(f"Column '{dimension}' not found in DataFrame")

        def _apply(value):
            normalized = self._normalize_value(value, lower=lower, strip=strip)
            return self._map_value(normalized, by, default=None)

        df[dimension] = df[dimension].apply(_apply)
        df = self._aggregate_gsc(df, self.dimensions)
        return SearchResult(df, self.parent, self.dimensions)
    
    def normalize_queries(self, mode: str = 'remove_all', prefer_by: str = 'impressions', group: bool = True) -> Self:
        """
        クエリの空白を正規化して重複を排除
        
        空白バリエーション（例: "矯正歯科", "矯正 歯科"）を統一し、
        各バリエーションの中で最も指標が高い元クエリを代表値として保持します。
        
        Args:
            mode: 'remove_all'（空白削除）または 'collapse'（空白を1つに）
            prefer_by: 代表クエリを選ぶ基準（'impressions', 'clicks', 'position'）
                      - 'position': 最小値（最良順位）を選択
                      - その他: 最大値を選択
                      - group=True の場合は必須（データに列が存在する必要あり）
            group: True の場合、正規化後に集約（default: True）
                   False の場合、query_key 列のみ追加（集約なし）
        
        Returns:
            SearchResult
        
        Raises:
            TypeError: prefer_by が文字列以外の場合
            ValueError: group=True で prefer_by 列がデータに存在しない場合
        
        Example:
            # "矯正 歯科" と "矯正歯科" を統一
            result = (mg.search
                .run(dimensions=['month', 'query', 'page'])
                .normalize_queries(prefer_by='impressions')
                .classify('query', by=cfg.query_map))
        """
        from megaton.transform.text import normalize_whitespace
        from megaton.transform.table import dedup_by_key
        
        df = self._df.copy()
        
        if 'query' not in df.columns:
            return self
        
        # prefer_by は文字列のみ（単一指標での選択）
        if not isinstance(prefer_by, str):
            raise TypeError(f"prefer_by must be a string, got {type(prefer_by).__name__}")
        
        # query_key を作成（空白を正規化）
        df['query_key'] = normalize_whitespace(df['query'], mode=mode)
        
        # dimensions から query を除外し、query_key を追加したキー列を作成
        key_cols = [d for d in self.dimensions if d != 'query']
        key_cols.append('query_key')
        
        if group:
            # 各 query_key の代表クエリを取得
            # position は最小値（最良順位）、その他は最大値を選択
            prefer_ascending = (prefer_by == 'position')
            top_queries = dedup_by_key(
                df,
                key_cols=key_cols,
                prefer_by=prefer_by,
                prefer_ascending=prefer_ascending,
                keep='first',
            )
            
            # query_key で集約
            df = self._aggregate_gsc(df, key_cols)
            
            # 代表クエリを戻す
            df = df.merge(
                top_queries[key_cols + ['query']],
                on=key_cols,
                how='left',
            )
            
            # query_key 列を削除
            df = df.drop(columns=['query_key'])
        # else: query_key 列のみ追加（集約なし）
        
        # dimensions は元のまま（query を含む）
        return SearchResult(df, self.parent, self.dimensions)
    
    def filter_clicks(self, min: float | None = None, max: float | None = None, sites: list[dict[str, object]] | None = None, site_key: str = 'site') -> Self:
        """
        クリック数でフィルタリング
        
        Args:
            min: 最小クリック数
            max: 最大クリック数
            sites: サイト辞書のリスト（行ごとに閾値を適用）
            site_key: DataFrame 内でサイトを識別する列名（default: 'site'）
        
        Returns:
            SearchResult
        """
        return self._filter_metric('clicks', min, max, sites, site_key, False,
                                   'min_clicks', 'max_clicks')
    
    def filter_impressions(self, min: float | None = None, max: float | None = None, sites: list[dict[str, object]] | None = None, site_key: str = 'site', keep_clicked: bool = False) -> Self:
        """インプレッション数でフィルタリング（default: keep_clicked=False）"""
        return self._filter_metric('impressions', min, max, sites, site_key, keep_clicked,
                                   'min_impressions', 'max_impressions')

    def filter_ctr(self, min: float | None = None, max: float | None = None, sites: list[dict[str, object]] | None = None, site_key: str = 'site', keep_clicked: bool = False) -> Self:
        """CTRでフィルタリング（default: keep_clicked=False）"""
        return self._filter_metric('ctr', min, max, sites, site_key, keep_clicked,
                                   'min_ctr', 'max_ctr')

    def filter_position(self, min: float | None = None, max: float | None = None, sites: list[dict[str, object]] | None = None, site_key: str = 'site', keep_clicked: bool = False) -> Self:
        """平均順位でフィルタリング（default: keep_clicked=False）"""
        return self._filter_metric('position', min, max, sites, site_key, keep_clicked,
                                   'min_position', 'max_position')

    def _filter_metric(self, metric: str, min_val: float | None, max_val: float | None, sites: list[dict[str, object]] | None, site_key: str, keep_clicked: bool,
                       min_key: str, max_key: str) -> Self:
        """
        指標ごとのフィルタリングを実行
        
        Args:
            metric: 指標名（'clicks', 'impressions', 'ctr', 'position'）
            min_val: 最小値（明示的指定、最優先）
            max_val: 最大値（明示的指定、最優先）
            sites: サイト辞書のリスト
            site_key: DataFrame 内のサイト識別列名
            keep_clicked: clicks >= 1 の行を無条件に残すか
            min_key: sites 辞書内の最小値キー（例: 'min_clicks'）
            max_key: sites 辞書内の最大値キー（例: 'max_clicks'）
        
        Returns:
            SearchResult
        """
        df = self._df.copy()
        
        # sites リストから閾値を取得（行ごとに適用）
        if sites and site_key in df.columns:
            # sites を辞書に変換（site_key をキーに）
            site_map = {s.get(site_key): s for s in sites if s.get(site_key)}
            
            # 行ごとに閾値を取得（明示的な min/max がない場合のみ）
            if min_val is None:
                df['_min'] = df[site_key].map(
                    lambda x: site_map.get(x, {}).get(min_key)
                )
            else:
                df['_min'] = min_val
            
            if max_val is None:
                df['_max'] = df[site_key].map(
                    lambda x: site_map.get(x, {}).get(max_key)
                )
            else:
                df['_max'] = max_val
            
            # keep_clicked の処理
            if keep_clicked and 'clicks' in df.columns:
                clicked = df[df['clicks'] >= 1].copy()
                unclicked = df[df['clicks'] == 0].copy()
                nan_clicks = df[df['clicks'].isna()].copy()
                
                # unclicked にのみ閾値を適用
                mask = pd.Series(True, index=unclicked.index)
                if '_min' in unclicked.columns and unclicked['_min'].notna().any():
                    mask &= (unclicked[metric] >= unclicked['_min']) | unclicked['_min'].isna()
                if '_max' in unclicked.columns and unclicked['_max'].notna().any():
                    mask &= (unclicked[metric] <= unclicked['_max']) | unclicked['_max'].isna()
                
                unclicked = unclicked[mask]
                
                # clicked, unclicked, NaN を結合
                parts = [clicked, unclicked]
                if not nan_clicks.empty:
                    parts.append(nan_clicks)
                df = pd.concat(parts)
            else:
                # 全行に閾値を適用
                mask = pd.Series(True, index=df.index)
                if '_min' in df.columns and df['_min'].notna().any():
                    mask &= (df[metric] >= df['_min']) | df['_min'].isna()
                if '_max' in df.columns and df['_max'].notna().any():
                    mask &= (df[metric] <= df['_max']) | df['_max'].isna()
                
                df = df[mask]
            
            # 一時列を削除
            df = df.drop(columns=['_min', '_max'], errors='ignore')
        
        else:
            # sites がない場合、明示的な min/max のみ適用
            if keep_clicked and 'clicks' in df.columns:
                clicked = df[df['clicks'] >= 1]
                unclicked = df[df['clicks'] == 0]
                nan_clicks = df[df['clicks'].isna()]
                
                if min_val is not None:
                    unclicked = unclicked[unclicked[metric] >= min_val]
                if max_val is not None:
                    unclicked = unclicked[unclicked[metric] <= max_val]
                
                # clicked, unclicked, NaN を結合
                parts = [clicked, unclicked]
                if not nan_clicks.empty:
                    parts.append(nan_clicks)
                df = pd.concat(parts)
            else:
                if min_val is not None:
                    df = df[df[metric] >= min_val]
                if max_val is not None:
                    df = df[df[metric] <= max_val]
        
        return SearchResult(df, self.parent, self.dimensions)
    
    def aggregate(self, by: str | list[str] | None = None) -> Self:
        """
        手動集計

        Args:
            by: 集計するカテゴリ列。None の場合は dimensions で集計

        Returns:
            SearchResult
        """
        if by:
            group_cols = [by] if isinstance(by, str) else list(by)
            df = self._aggregate_gsc(self._df, group_cols)
            # dimensions を更新して、後続の group=True が正しく動作するようにする
            new_dimensions = group_cols
        else:
            df = self._aggregate(self._df)
            new_dimensions = self.dimensions
        
        return SearchResult(df, self.parent, new_dimensions)
    
    def apply_if(self, condition: bool | Callable[[SearchResult], bool], method_name: str, *args: object, **kwargs: object) -> Self:
        """
        条件が真の場合のみメソッドを適用

        メソッドチェーン内で条件分岐を実現し、if/else による重複を排除します。

        Args:
            condition: bool または callable(SearchResult) -> bool
                      - bool: 静的な条件（例: TARGET_MONTHS_AGO > 0）
                      - callable: 動的な条件（例: lambda sr: len(sr.df) > 100）
            method_name: str - 適用するメソッド名（例: 'filter_impressions'）
            *args, **kwargs: メソッドの引数

        Returns:
            SearchResult: チェーン継続可能

        Raises:
            AttributeError: method_name が存在しない場合
        
        Example:
            # 過去月のみフィルタを適用
            gsc_df_filtered = (
                gsc_result_mapped
                .normalize_queries(mode='remove_all', prefer_by='impressions', group=True)
                .classify('page', by=page_map)
                .apply_if(TARGET_MONTHS_AGO > 0, 'filter_impressions', 
                          sites=selected_sites, site_key='clinic', keep_clicked=True)
                .apply_if(TARGET_MONTHS_AGO > 0, 'filter_position',
                          sites=selected_sites, site_key='clinic', keep_clicked=True)
                .df
            )
            
            # 動的条件の例：データ量に応じて処理を変更
            result = (
                gsc_result
                .apply_if(lambda sr: len(sr.df) > 1000, 'filter_impressions', min=10)
                .apply_if(lambda sr: 'device' in sr.df.columns, 'aggregate', by='device')
            )
        """
        # 条件評価
        if callable(condition):
            should_apply = condition(self)
        else:
            should_apply = bool(condition)
        
        # 条件が真の場合のみメソッド適用
        if should_apply:
            method = getattr(self, method_name)
            return method(*args, **kwargs)
        
        # 条件が偽の場合はそのまま返す（チェーン継続）
        return self


class ReportResult(_ResultBase):
    """GA4 レポートデータをラップし、メソッドチェーンで処理を行うクラス"""

    def __init__(self, df: pd.DataFrame, dimensions: list[str] | None = None) -> None:
        """
        Args:
            df: pandas DataFrame
            dimensions: ディメンションのリスト（例: ['date', 'sessionSource']）
                       None の場合は自動で推定（指標以外の列）
        """
        self._df = df

        # dimensions の推定（明示指定がある場合は最優先）
        if dimensions is None:
            # 自動判定の順序:
            # 1. KNOWN_GA4_DIMENSIONS に含まれる列は dimensions として確保
            # 2. KNOWN_GA4_METRICS を除外
            # 3. 残りの数値列もメトリクスとして除外（カスタムメトリクスの自動検出）
            # 4. 最終的に dimensions = 既知dimension + 非数値列
            if len(df.columns) == 0:
                self.dimensions: list[str] = []
            else:
                # 数値列のうち既知のディメンションを除いたものをメトリクス候補とする
                numeric_cols = set(df.select_dtypes(include=['number']).columns)
                numeric_cols -= KNOWN_GA4_DIMENSIONS  # 既知ディメンションは除外
                metric_cols = KNOWN_GA4_METRICS | numeric_cols
                # dimensions = 非メトリクス列
                self.dimensions = [col for col in df.columns if col not in metric_cols]
        else:
            self.dimensions = dimensions

    @property
    def df(self) -> pd.DataFrame:
        """DataFrame として直接アクセス（後方互換性）"""
        return self._df

    @property
    def empty(self) -> bool:
        """DataFrame が空かどうか"""
        return self._df.empty

    @property
    def columns(self) -> list[str]:
        """DataFrame の列名"""
        return self._df.columns.tolist()

    def __repr__(self) -> str:
        """ReportResult オブジェクトの文字列表現"""
        return f"ReportResult({len(self._df)} rows x {len(self._df.columns)} columns)"

    def __len__(self) -> int:
        """len() でデータフレームの行数を返す（後方互換性）"""
        return len(self._df)

    def __getitem__(self, key: str) -> pd.Series:
        """df[key] として列にアクセス（後方互換性）"""
        return self._df[key]

    def _with_df(self, df: pd.DataFrame, dimensions: list[str]) -> "ReportResult":
        return ReportResult(df, dimensions)

    def classify(self, dimension: str, by: MappingRule, *, lower: bool = True, strip: bool = True) -> Self:
        """
        正規化 + 集約（ディメンション上書き、常に集約）
        """
        normalized = self.normalize(dimension, by, lower=lower, strip=strip)
        return normalized.group(by=normalized.dimensions)
    
    def group(self, by: str | list[str], metrics: str | list[str] | None = None, method: str = 'sum',
              *, dropna: bool = True, min_count: int | None = None) -> Self:
        """
        指定したディメンションで集計

        Args:
            by: 集計キーとなるディメンション列名または列名のリスト
            metrics: 集計する指標列名のリスト（または単一の列名文字列）
            method: 集計方法（'sum', 'mean', 'count', 'min', 'max'）
            dropna: False にすると集計キーの NaN もグループとして残す
                （pandas groupby の dropna に対応、default True）。
            min_count: sum/prod のときのみ有効。全要素が NaN のグループを
                0 ではなく NaN にする（旧コードの ``.sum(min_count=1)`` 互換。
                直後に ``.to_int()`` で 0 化する用途）。

        Returns:
            ReportResult（集計後のデータ）

        Example:
            # sessionSource でセッション数を集計
            result.group(by='sessionSource', metrics=['sessions'])

            # 複数ディメンションで集計
            result.group(by=['date', 'sessionSource'])

            # 旧 .groupby(..., dropna=False)[...].sum(min_count=1) 互換
            result.group(['month', 'clinic'], dropna=False, min_count=1).to_int()
        """
        df = self._df.copy()
        
        # by を list に統一
        if isinstance(by, str):
            by = [by]
        
        # metrics を list に統一
        if metrics is not None and isinstance(metrics, str):
            metrics = [metrics]
        
        # 空DataFrame対応
        if df.empty:
            # metric列を含む空DataFrameを返す（存在する列のみ）
            if metrics:
                # 既存の列のみ含める（存在しない列は除外）
                valid_metrics = [m for m in metrics if m in df.columns]
                columns = by + valid_metrics
            else:
                columns = by
            return ReportResult(pd.DataFrame(columns=columns), by)
        
        # 指標列を特定
        if metrics is None:
            # 数値列を自動検出（by に指定された列を除く）
            metrics = [col for col in df.select_dtypes(include=['number']).columns 
                      if col not in by]
        else:
            # 明示指定されたmetricsが存在しない列は除外
            metrics = [m for m in metrics if m in df.columns]
        
        if not metrics:
            # メトリクスがない場合は by 列のみの空DataFrameを返す
            return ReportResult(pd.DataFrame(columns=by), by)
        
        # 集計実行
        # min_count は sum/prod でのみ有効。全 NaN グループを 0 ではなく NaN にしたい
        # （その後 .to_int() で 0 化する）ケースで、旧コードの .sum(min_count=1) と一致させる。
        if min_count is not None and method in ("sum", "prod"):
            grouped = (
                df.groupby(by, as_index=False, dropna=dropna)[metrics]
                .agg(method, min_count=min_count)
            )
        else:
            agg_dict = {col: method for col in metrics}
            grouped = df.groupby(by, as_index=False, dropna=dropna).agg(agg_dict)

        # dimensions を更新
        new_dimensions = by

        return ReportResult(grouped, new_dimensions)

    def select(self, columns: list[str], *, strict: bool = True) -> Self:
        """列を指定順に選択（並べ替え）する。

        手書きの ``df[key_cols]`` を置き換える。dimensions は選択後の
        列のうち元 dimensions に含まれていたものへ更新される。

        Args:
            columns: 残す列名を出力順に並べたリスト。
            strict: True なら存在しない列があると ``KeyError``。
                False なら存在する列のみを残す。

        Example:
            result.group([...]).to_int().select(key_cols)
        """
        df = self._df
        if strict:
            missing = [c for c in columns if c not in df.columns]
            if missing:
                raise KeyError(f"columns not found: {missing}")
            selected = list(columns)
        else:
            selected = [c for c in columns if c in df.columns]
        new_df = df[selected].copy()
        new_dimensions = [d for d in self.dimensions if d in selected]
        return ReportResult(new_df, new_dimensions)
    
    def sort(self, by: str | list[str], ascending: bool | list[bool] = True) -> Self:
        """
        指定した列でソート

        Args:
            by: ソートキーとなる列名または列名のリスト
            ascending: 昇順（True）または降順（False）
                      列ごとに指定する場合はリスト
        
        Returns:
            ReportResult（ソート後のデータ）
        
        Example:
            # sessions で降順ソート
            result.sort(by='sessions', ascending=False)
            
            # 複数列でソート
            result.sort(by=['date', 'sessions'], ascending=[True, False])
        """
        df = self._df.copy()
        sorted_df = df.sort_values(by=by, ascending=ascending).reset_index(drop=True)
        return ReportResult(sorted_df, self.dimensions)
    
    def fill(self, to: str = '(not set)', dimensions: list[str] | None = None) -> Self:
        """
        ディメンション列の欠損値を指定した値で埋める

        Args:
            to: 埋める値（default: '(not set)'）
            dimensions: 対象のディメンション列名のリスト
                       None の場合は self.dimensions のすべての列
        
        Returns:
            ReportResult（欠損値を埋めたデータ）
        
        Example:
            # すべてのディメンションの欠損値を '(not set)' で埋める
            result.fill()
            
            # 特定のディメンションのみ埋める
            result.fill(to='Unknown', dimensions=['sessionSource'])
        """
        df = self._df.copy()
        
        # 対象列を決定
        if dimensions is None:
            target_cols = [col for col in self.dimensions if col in df.columns]
        else:
            target_cols = dimensions
        
        # 欠損値を埋める
        for col in target_cols:
            if col in df.columns:
                df[col] = df[col].fillna(to)
        
        return ReportResult(df, self.dimensions)
    
    def to_int(self, metrics: str | list[str] | None = None, *, fill_value: int = 0) -> Self:
        """
        指標列を整数型に変換（欠損値は指定した値で埋める）
        
        Args:
            metrics (str | list[str] | None): 変換する指標列名
                - str: 単一の列名
                - list[str]: 複数の列名
                - None: すべての数値列（自動推論、int64/float64/Int64/Float64のみ）
            fill_value (int): 欠損値を埋める値（default: 0、キーワード専用）
        
        Returns:
            ReportResult（整数型に変換したデータ）
        
        Note:
            metrics=None の場合、int64, float64, Int64, Float64 型の列のみが対象です。
            int32, float32, UInt64 などは対象外です。
        
        Example:
            # sessions を整数型に変換
            result.to_int('sessions')
            
            # 複数の指標を変換
            result.to_int(['sessions', 'users'])
            
            # すべての数値列を変換
            result.to_int()
            
            # 後方互換性（キーワード引数での指定）
            result.to_int(metrics=['sessions', 'users'])
            
            # fill_value はキーワード専用
            result.to_int(['sessions'], fill_value=99)
        """
        df = self._df.copy()
        
        # metrics が None の場合、すべての数値列を対象（int64/float64/Int64/Float64のみ）
        if metrics is None:
            metrics = df.select_dtypes(include=['int64', 'float64', 'Int64', 'Float64']).columns.tolist()
        # metrics を list に統一
        elif isinstance(metrics, str):
            metrics = [metrics]
        
        # 型変換実行: object/文字列混在の列でも壊れないよう pd.to_numeric で強制
        # （transform.fillna_int と同じ堅牢化。GA4 の advertiserAdCost 等は
        # object dtype で返ることがあり、素の .astype(int) では失敗する）
        for col in metrics:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(fill_value).astype(int)

        return ReportResult(df, self.dimensions)

    def month_key(self, dimension: str = 'date', *, into: str | None = None, fmt: str = '%Y-%m') -> Self:
        """Derive a month-key column from a date-like dimension.

        Standardizes month formatting (replaces hand-rolled "%Y/%m/1",
        "%Y%m", "%Y-%m-01" variants in reports).

        Args:
            dimension: source column (GA4 'YYYYMMDD' strings, datetime, or date).
            into: output column name (default: overwrite ``dimension``).
            fmt: strftime format, e.g. '%Y-%m', '%Y%m', '%Y/%m/1'.
        """
        if dimension not in self._df.columns:
            raise KeyError(f"column not found: {dimension}")
        df = self._df.copy()
        series = pd.to_datetime(df[dimension], errors='coerce')
        target = into or dimension
        df[target] = series.dt.strftime(fmt)
        new_dimensions = list(self.dimensions)
        if target not in new_dimensions:
            new_dimensions.append(target)
        return ReportResult(df, new_dimensions)

    def replace(self, dimension: str, by: dict[str, str], *, regex: bool = True) -> Self:
        """
        ディメンション列の値を辞書マッピングで置換
        
        Args:
            dimension: 置換対象のディメンション列名
            by: 置換マッピング辞書 {old_value: new_value}
                regex=True の場合、キーは正規表現として扱われる
            regex: True の場合、辞書のキーを正規表現として扱う（default: True）
        
        Returns:
            ReportResult（値を置換したデータ）
        
        Example:
            # 正規表現での置換（default）
            result.replace(
                dimension='campaign',
                by={r'\\([^)]*\\)': ''}
            )
            
            # 固定文字列での置換
            result.replace(
                dimension='sessionSource',
                by={'google': 'Google', 'yahoo': 'Yahoo'},
                regex=False
            )
        """
        df = self._df.copy()
        
        if dimension not in df.columns:
            raise ValueError(f"Column '{dimension}' not found in DataFrame")
        
        # 置換実行
        df[dimension] = df[dimension].replace(by, regex=regex)
        
        return ReportResult(df, self.dimensions)

    def clean_url(self, dimension: str, *, unquote: bool = True, drop_query: bool = True, drop_hash: bool = True, lower: bool = True) -> Self:
        """
        URL列を正規化（URLデコード、クエリ/フラグメント削除、小文字化）

        Args:
            dimension: 対象ディメンション列名
            unquote: URLデコードするか（default: True）
            drop_query: クエリパラメータを削除（default: True）
            drop_hash: フラグメントを削除（default: True）
            lower: 小文字化（default: True）

        Returns:
            ReportResult（URLを正規化したデータ）

        Example:
            result.clean_url(
                dimension='page',
                drop_query=True,
                drop_hash=True,
                lower=True
            )
        """
        from megaton.transform.text import clean_url

        df = self._df.copy()

        if dimension not in df.columns:
            raise ValueError(f"Column '{dimension}' not found in DataFrame")

        df[dimension] = clean_url(
            df[dimension],
            unquote=unquote,
            drop_query=drop_query,
            drop_hash=drop_hash,
            lower=lower,
        )

        return ReportResult(df, self.dimensions)


def _extract_df(data):
    """Return the underlying DataFrame when given a ReportResult/SearchResult.

    DataFrames and other values pass through unchanged.
    """
    if isinstance(data, (ReportResult, SearchResult)):
        return data.df
    return data


def wrap(df: pd.DataFrame, dimensions: list[str] | None = None) -> ReportResult:
    """Wrap any DataFrame in a chainable ReportResult.

    Lets data from BigQuery, Sheets, CSV, etc. use the same chainable
    vocabulary as GA4 query results::

        from megaton import wrap
        wrap(df).normalize('source', rules).group('month').to_int().sort('month')

    Args:
        df: Source DataFrame (copied; the original is not mutated).
        dimensions: Optional dimension column names. Defaults to all
            non-numeric columns (numeric columns are treated as metrics
            by group()/to_int()).
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("wrap() requires a pandas DataFrame.")
    if dimensions is None:
        dimensions = [
            col for col in df.columns
            if not pd.api.types.is_numeric_dtype(df[col])
        ]
    return ReportResult(df.copy(), list(dimensions))

