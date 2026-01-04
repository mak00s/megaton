"""Simplified tests for mg.search.run.all() and mg.report.run.all()

These tests verify the basic functionality without complex mocking.
"""

from unittest.mock import patch
import pandas as pd
import pytest
from megaton import dates
from megaton.start import Megaton


def test_search_run_all_basic():
    """Test basic Search Console multi-item run"""
    app = Megaton(None, headless=True)
    app.search.start_date = "2025-01-01"
    app.search.end_date = "2025-01-31"
    
    mock_df = pd.DataFrame({
        'query': ['test query'],
        'clicks': [10],
        'impressions': [100],
    })
    
    with patch.object(app.search.run.__class__, '__call__', return_value=mock_df):
        with patch.object(app.search, 'use'):
            sites = [
                {'site': 'siteA', 'gsc_site_url': 'https://example.com/'},
                {'site': 'siteB', 'gsc_site_url': 'https://example2.com/'},
            ]
            
            result = app.search.run.all(
                sites,
                dimensions=['query'],
                metrics=['clicks', 'impressions'],
                verbose=False,
            )
    
    assert result is not None
    result_df = result.df
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 2
    assert 'site' in result_df.columns
    assert set(result_df['site']) == {'siteA', 'siteB'}


def test_search_run_all_with_filter():
    """Test Search Console with item_filter as list"""
    app = Megaton(None, headless=True)
    app.search.start_date = "2025-01-01"
    app.search.end_date = "2025-01-31"
    
    mock_df = pd.DataFrame({'query': ['test'], 'clicks': [5]})
    
    with patch.object(app.search.run.__class__, '__call__', return_value=mock_df):
        with patch.object(app.search, 'use'):
            sites = [
                {'clinic': 'A', 'gsc_site_url': 'https://a.com/'},
                {'clinic': 'B', 'gsc_site_url': 'https://b.com/'},
                {'clinic': 'C', 'gsc_site_url': 'https://c.com/'},
            ]
            
            result = app.search.run.all(
                sites,
                dimensions=['query'],
                item_key='clinic',
                item_filter=['A', 'C'],
                verbose=False,
            )
    
    result_df = result.df
    assert len(result_df) == 2
    assert set(result_df['clinic']) == {'A', 'C'}


def test_search_run_all_url_fallback():
    """Test Search Console with gsc_site_url (no fallback to 'url')"""
    app = Megaton(None, headless=True)
    app.search.start_date = "2025-01-01"
    app.search.end_date = "2025-01-31"
    
    mock_df = pd.DataFrame({'query': ['test'], 'clicks': [5]})
    
    with patch.object(app.search.run.__class__, '__call__', return_value=mock_df):
        with patch.object(app.search, 'use'):
            # Sites must have gsc_site_url
            sites = [{'site': 'A', 'gsc_site_url': 'https://a.com/', 'url': 'https://a.com/'}]
            
            result = app.search.run.all(
                sites,
                dimensions=['query'],
                verbose=False,
            )
    
    result_df = result.df
    assert len(result_df) == 1
    assert result_df['site'].iloc[0] == 'A'


def test_report_run_all_basic():
    """Test basic GA4 multi-item run"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    from types import SimpleNamespace
    from megaton.start import ReportResult
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))
    
    # Mock the __call__ method - run.allから呼ばれる
    def mock_call(self, d, m, **kwargs):
        self.parent.data = pd.DataFrame({'date': ['2025-01-01'], 'users': [100]})
    
    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [
            {'site': 'siteA', 'ga4_property_id': '123456'},
            {'site': 'siteB', 'ga4_property_id': '789012'},
        ]
        
        result = app.report.run.all(
            sites,
            d=['date'],
            m=['users'],
            verbose=False,
        )
    
    # ReportResult インスタンスであることを確認
    assert isinstance(result, ReportResult)
    assert len(result) == 2
    assert 'site' in result.columns
    assert set(result['site']) == {'siteA', 'siteB'}


def test_report_run_all_site_metric_prefix():
    """site. プレフィックスでサイト別メトリクスを指定"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    metrics_used = []

    def mock_call(self, d, m, **kwargs):
        metrics_used.append(m)
        self.parent.data = pd.DataFrame({'users': [1]})

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [
            {'clinic': '札幌', 'ga4_property_id': '12345', 'cv': 'totalPurchasers'},
            {'clinic': '仙台', 'ga4_property_id': '67890', 'cv': 'keyEvents'},
        ]

        result = app.report.run.all(
            sites,
            d=[('yearMonth', 'month')],
            m=[('site.cv', 'cv')],
            item_key='clinic',
            verbose=False,
        )

    assert len(result) == 2
    assert metrics_used == [
        [('totalPurchasers', 'cv')],
        [('keyEvents', 'cv')],
    ]


def test_report_run_all_site_metric_missing_key():
    """site. プレフィックスで存在しないキーを指定するとエラー"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    sites = [{'clinic': '札幌', 'ga4_property_id': '12345'}]

    with pytest.raises(ValueError, match="Site key 'cv' not found"):
        app.report.run.all(
            sites,
            d=[('yearMonth', 'month')],
            m=[('site.cv', 'cv')],
            item_key='clinic',
            verbose=False,
        )


def test_report_run_all_mixed_metrics():
    """通常メトリクスと site. プレフィックスの混在"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    metrics_used = []

    def mock_call(self, d, m, **kwargs):
        metrics_used.append(m)
        self.parent.data = pd.DataFrame({'users': [1], 'cv': [2]})

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '札幌', 'ga4_property_id': '12345', 'cv': 'totalPurchasers'}]

        result = app.report.run.all(
            sites,
            d=[('yearMonth', 'month')],
            m=[('activeUsers', 'users'), ('site.cv', 'cv')],
            item_key='clinic',
            verbose=False,
        )

    assert len(result) == 1
    assert metrics_used == [[('activeUsers', 'users'), ('totalPurchasers', 'cv')]]


def test_report_run_all_absolute_url_from_item_url():
    """absolute=True converts relative paths using item['url'] domain only"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    mock_df = pd.DataFrame({'lp': ['/apl/netuser/?id=1'], 'users': [1]})

    with patch.object(app.report.run.__class__, '__call__'):
        app.report.data = mock_df
        sites = [
            {
                'site': 'dentamap',
                'ga4_property_id': '123456',
                'url': 'https://plus.dentamap.jp/apl/netuser/',
            }
        ]

        result = app.report.run.all(
            sites,
            d=[('landingPage', 'lp', {'absolute': True})],
            m=['users'],
            verbose=False,
        )

    assert result['lp'].iloc[0] == 'https://plus.dentamap.jp/apl/netuser/?id=1'


def test_report_run_all_absolute_url_skips_without_base():
    """absolute=True leaves relative paths when item['url'] is missing/empty"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    mock_df = pd.DataFrame({'lp': ['/landing/'], 'users': [1]})

    with patch.object(app.report.run.__class__, '__call__'):
        app.report.data = mock_df
        sites = [
            {'site': 'siteA', 'ga4_property_id': '123456', 'url': ''},
        ]

        result = app.report.run.all(
            sites,
            d=[('landingPage', 'lp', {'absolute': True})],
            m=['users'],
            verbose=False,
        )

    assert result['lp'].iloc[0] == '/landing/'


def test_report_run_all_absolute_preserves_already_absolute():
    """absolute=True preserves URLs that are already absolute"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    mock_df = pd.DataFrame({
        'lp': ['https://other.com/page', 'http://another.com/test'],
        'users': [1, 2]
    })

    with patch.object(app.report.run.__class__, '__call__'):
        app.report.data = mock_df
        sites = [
            {'site': 'siteA', 'ga4_property_id': '123456', 'url': 'https://example.com'},
        ]

        result = app.report.run.all(
            sites,
            d=[('landingPage', 'lp', {'absolute': True})],
            m=['users'],
            verbose=False,
        )

    # 既に絶対URLの場合は変更しない
    assert result['lp'].iloc[0] == 'https://other.com/page'
    assert result['lp'].iloc[1] == 'http://another.com/test'


def test_report_run_all_absolute_handles_none_and_empty():
    """absolute=True preserves None and empty strings"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    mock_df = pd.DataFrame({
        'lp': [None, '', '/page'],
        'users': [1, 2, 3]
    })

    with patch.object(app.report.run.__class__, '__call__'):
        app.report.data = mock_df
        sites = [
            {'site': 'siteA', 'ga4_property_id': '123456', 'url': 'https://example.com'},
        ]

        result = app.report.run.all(
            sites,
            d=[('landingPage', 'lp', {'absolute': True})],
            m=['users'],
            verbose=False,
        )

    # None と空文字はそのまま、相対パスのみ変換
    assert pd.isna(result['lp'].iloc[0])
    assert result['lp'].iloc[1] == ''
    assert result['lp'].iloc[2] == 'https://example.com/page'


def test_report_run_all_absolute_preserves_query_and_fragment():
    """absolute=True preserves query parameters and fragments"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    mock_df = pd.DataFrame({
        'lp': ['/page?a=1&b=2', '/section#anchor'],
        'users': [1, 2]
    })

    with patch.object(app.report.run.__class__, '__call__'):
        app.report.data = mock_df
        sites = [
            {'site': 'siteA', 'ga4_property_id': '123456', 'url': 'https://example.com'},
        ]

        result = app.report.run.all(
            sites,
            d=[('landingPage', 'lp', {'absolute': True})],
            m=['users'],
            verbose=False,
        )

    # クエリパラメータとフラグメントは保持される
    assert result['lp'].iloc[0] == 'https://example.com/page?a=1&b=2'
    assert result['lp'].iloc[1] == 'https://example.com/section#anchor'


def test_search_run_all_empty_gsc_site_url():
    """Test that empty gsc_site_url is skipped"""
    app = Megaton(None, headless=True)
    app.search.start_date = "2025-01-01"
    app.search.end_date = "2025-01-31"
    
    call_count = {'count': 0}
    
    def mock_call(self, dimensions, metrics, **kwargs):
        call_count['count'] += 1
        return pd.DataFrame({'query': ['test'], 'clicks': [5]})
    
    with patch.object(app.search.run.__class__, '__call__', mock_call):
        with patch.object(app.search, 'use'):
            sites = [
                {'clinic': 'A', 'gsc_site_url': 'https://a.com/'},
                {'clinic': 'B', 'gsc_site_url': ''},  # Empty gsc_site_url, should skip
                {'clinic': 'C', 'gsc_site_url': 'https://c.com/'},
            ]
            
            result = app.search.run.all(
                sites,
                dimensions=['query'],
                item_key='clinic',
                verbose=False,
            )
    
    # Should process A and C, skip B
    result_df = result.df
    assert len(result_df) == 2
    assert set(result_df['clinic']) == {'A', 'C'}
    assert call_count['count'] == 2


def test_report_run_all_missing_property_id():
    """Test GA4 handles missing property_id gracefully"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))
    
    mock_df = pd.DataFrame({'users': [100]})
    
    with patch.object(app.report.run.__class__, '__call__'):
        app.report.data = mock_df
        
        sites = [
            {'site': 'A', 'ga4_property_id': '123'},
            {'site': 'B'},  # Missing property_id
            {'site': 'C', 'ga4_property_id': '789'},
        ]
        
        result = app.report.run.all(
            sites,
            d=['date'],
            m=['users'],
            verbose=False,
        )
    
    # Should only process sites A and C
    assert len(result) == 2
    assert set(result['site']) == {'A', 'C'}


def test_search_run_all_handles_searchresult():
    """Test that run.all() correctly handles SearchResult from self()"""
    from megaton.start import SearchResult
    
    app = Megaton(None, headless=True)
    app.search.start_date = "2025-01-01"
    app.search.end_date = "2025-01-31"
    
    # Create a SearchResult instead of DataFrame
    mock_df = pd.DataFrame({
        'query': ['test query'],
        'clicks': [10],
        'impressions': [100],
    })
    mock_result = SearchResult(mock_df, None, ['query'])
    
    # Patch to return SearchResult (not DataFrame)
    with patch.object(app.search.run.__class__, '__call__', return_value=mock_result):
        with patch.object(app.search, 'use'):
            sites = [
                {'site': 'siteA', 'gsc_site_url': 'https://example.com/'},
                {'site': 'siteB', 'gsc_site_url': 'https://example2.com/'},
            ]
            
            result = app.search.run.all(
                sites,
                dimensions=['query'],
                metrics=['clicks', 'impressions'],
                verbose=False,
            )
    
    # Should successfully handle SearchResult.df
    assert result is not None
    result_df = result.df
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 2
    assert 'site' in result_df.columns
    
    # item_key が dimensions に含まれることを確認
    assert 'site' in result.dimensions
    
    # メソッドチェーンで site が保持されることを確認
    decoded = result.decode(group=True)
    assert 'site' in decoded.df.columns


def test_report_run_all_unsupported_metric_option():
    """未サポートのメトリクスオプションでエラー"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))
    
    def mock_call(self, d, m, filter_d=None, **kwargs):
        # このモックは呼ばれないはず（エラーが先に起きる）
        self.parent.data = pd.DataFrame({'page': ['/']})
    
    with patch.object(app.report.run.__class__, '__call__', mock_call):
        # filter_m はまだサポートされていない
        with pytest.raises(ValueError, match="Unsupported metric options"):
            app.report.run.all(
                [{'clinic': 'test', 'ga4_property_id': '12345'}],
                d=['page'],
                m=[('activeUsers', 'users', {'filter_m': 'some_filter'})],
                item_key='clinic',
                verbose=False
            )


def test_report_run_all_site_dimension_prefix():
    """site. プレフィックスで次元を動的に指定"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    actual_dimensions_by_clinic = {}

    def mock_call(self, d, m, **kwargs):
        # clinic 名を取得（property.id で判断）
        clinic_name = '札幌' if self.parent.parent.ga['4'].property.id == '12345' else 'dentamap'
        actual_dimensions_by_clinic[clinic_name] = d
        
        # 次元に応じたデータを返す
        if d and len(d) >= 2:
            dim_name = d[1][0] if isinstance(d[1], tuple) else d[1]
            if dim_name == 'landingPage':
                self.parent.data = pd.DataFrame({'month': ['202501'], 'lp': ['/page1'], 'users': [100], 'clinic': ['札幌']})
            elif dim_name == 'landingPagePlusQueryString':
                self.parent.data = pd.DataFrame({'month': ['202501'], 'lp': ['/page1?id=123'], 'users': [50], 'clinic': ['dentamap']})
        else:
            self.parent.data = pd.DataFrame()

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [
            {'clinic': '札幌', 'ga4_property_id': '12345', 'lp_dim': 'landingPage'},
            {'clinic': 'dentamap', 'ga4_property_id': '67890', 'lp_dim': 'landingPagePlusQueryString'},
        ]
        
        result = app.report.run.all(
            sites,
            d=[('yearMonth', 'month'), ('site.lp_dim', 'lp')],
            m=[('activeUsers', 'users')],
            item_key='clinic',
            verbose=False,
        )
    
    # 各サイトで異なる次元が使われたことを確認（順序非依存）
    assert len(actual_dimensions_by_clinic) == 2
    assert '札幌' in actual_dimensions_by_clinic
    assert 'dentamap' in actual_dimensions_by_clinic
    assert actual_dimensions_by_clinic['札幌'][1] == ('landingPage', 'lp')
    assert actual_dimensions_by_clinic['dentamap'][1] == ('landingPagePlusQueryString', 'lp')
    
    # 結果が統合されていることを確認
    assert len(result) == 2
    assert '札幌' in result['clinic'].values
    assert 'dentamap' in result['clinic'].values


def test_report_run_all_site_dimension_with_options():
    """site. プレフィックスで次元を指定し、オプションも適用"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    def mock_call(self, d, m, **kwargs):
        # 次元に応じたデータを返す（相対パス）
        if d and len(d) >= 2:
            dim_name = d[1][0] if isinstance(d[1], tuple) else d[1]
            if dim_name == 'landingPage':
                self.parent.data = pd.DataFrame({'month': ['202501'], 'lp': ['/page1'], 'users': [100]})

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [
            {'clinic': '札幌', 'ga4_property_id': '12345', 'lp_dim': 'landingPage', 'url': 'https://example.com'},
        ]
        
        result = app.report.run.all(
            sites,
            d=[('yearMonth', 'month'), ('site.lp_dim', 'lp', {'absolute': True})],
            m=[('activeUsers', 'users')],
            item_key='clinic',
            verbose=False,
        )
    
    # absolute オプションが適用され、絶対URLに変換されていることを確認
    assert len(result) == 1
    assert result['lp'].iloc[0] == 'https://example.com/page1'


def test_report_run_all_site_dimension_missing_key():
    """site. プレフィックスで存在しないキーを指定するとエラー"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    def mock_call(self, d, m, **kwargs):
        self.parent.data = pd.DataFrame()

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [
            {'clinic': '札幌', 'ga4_property_id': '12345'},  # lp_dim キーがない
        ]
        
        with pytest.raises(ValueError, match="Site key 'lp_dim' not found"):
            app.report.run.all(
                sites,
                d=[('yearMonth', 'month'), ('site.lp_dim', 'lp')],
                m=[('activeUsers', 'users')],
                item_key='clinic',
                verbose=False,
            )


def test_report_run_all_metric_specific_filter_d():
    """メトリクスごとに異なるfilter_dを指定"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    call_count = 0
    filters_used = []

    def mock_call(self, d, m, filter_d=None, **kwargs):
        nonlocal call_count
        call_count += 1
        filters_used.append(filter_d)
        
        # filter_dに応じて異なるデータを返す
        if filter_d == 'sessionDefaultChannelGroup==Organic Search':
            self.parent.data = pd.DataFrame({'page': ['/'], 'users': [100]})
        elif filter_d == 'defaultChannelGroup==Organic Search':
            self.parent.data = pd.DataFrame({'page': ['/'], 'cv': [10]})
        else:
            self.parent.data = pd.DataFrame({'page': ['/']})

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '札幌', 'ga4_property_id': '12345'}]
        
        result = app.report.run.all(
            sites,
            d=[('landingPage', 'page')],
            m=[
                ('activeUsers', 'users', {'filter_d': 'sessionDefaultChannelGroup==Organic Search'}),
                ('totalPurchasers', 'cv', {'filter_d': 'defaultChannelGroup==Organic Search'}),
            ],
            item_key='clinic',
            verbose=False,
        )
    
    # 2つの異なるfilter_dで2回呼ばれる
    assert call_count == 2
    assert 'sessionDefaultChannelGroup==Organic Search' in filters_used
    assert 'defaultChannelGroup==Organic Search' in filters_used
    
    # 結果が統合されている
    assert len(result) == 1
    assert 'users' in result.columns
    assert 'cv' in result.columns
    assert result['users'].iloc[0] == 100
    assert result['cv'].iloc[0] == 10
    assert result['clinic'].iloc[0] == '札幌'


def test_report_run_all_mixed_filter_d():
    """グローバルfilter_dとメトリクス個別filter_dの混在"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    filters_used = []

    def mock_call(self, d, m, filter_d=None, **kwargs):
        filters_used.append(filter_d)
        if filter_d == 'sessionDefaultChannelGroup==Organic Search':
            self.parent.data = pd.DataFrame({'page': ['/'], 'users': [50]})
        elif filter_d == 'defaultChannelGroup==Organic Search':
            self.parent.data = pd.DataFrame({'page': ['/'], 'cv': [5]})
        else:
            self.parent.data = pd.DataFrame({'page': ['/']})

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '仙台', 'ga4_property_id': '67890'}]
        
        result = app.report.run.all(
            sites,
            d=[('landingPage', 'page')],
            m=[
                ('activeUsers', 'users'),  # グローバルfilter_dを使用
                ('totalPurchasers', 'cv', {'filter_d': 'defaultChannelGroup==Organic Search'}),
            ],
            filter_d='sessionDefaultChannelGroup==Organic Search',  # グローバル
            item_key='clinic',
            verbose=False,
        )
    
    # users: sessionDefaultChannelGroup, cv: defaultChannelGroup
    assert 'sessionDefaultChannelGroup==Organic Search' in filters_used
    assert 'defaultChannelGroup==Organic Search' in filters_used
    assert result['users'].iloc[0] == 50
    assert result['cv'].iloc[0] == 5


def test_report_run_all_same_filter_d_optimization():
    """同じfilter_dのメトリクスは1回のAPIコールにまとめる"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    call_count = 0

    def mock_call(self, d, m, filter_d=None, **kwargs):
        nonlocal call_count
        call_count += 1
        # 複数メトリクスを同時に返す
        self.parent.data = pd.DataFrame({'page': ['/'], 'users': [100], 'sessions': [150]})

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '札幌', 'ga4_property_id': '12345'}]
        
        result = app.report.run.all(
            sites,
            d=[('landingPage', 'page')],
            m=[
                ('activeUsers', 'users', {'filter_d': 'sessionDefaultChannelGroup==Organic Search'}),
                ('sessions', 'sessions', {'filter_d': 'sessionDefaultChannelGroup==Organic Search'}),
            ],
            item_key='clinic',
            verbose=False,
        )
    
    # 同じfilter_dなので1回のAPIコールのみ
    assert call_count == 1
    assert 'users' in result.columns
    assert 'sessions' in result.columns

def test_report_run_all_global_explicit_same_filter_optimization():
    """グローバルfilter_dと明示的filter_dが同じ値の場合、1回のAPIコールにまとめる"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    call_count = 0
    filters_used = []

    def mock_call(self, d, m, filter_d=None, **kwargs):
        nonlocal call_count
        call_count += 1
        filters_used.append(filter_d)
        # 複数メトリクスを同時に返す
        self.parent.data = pd.DataFrame({'page': ['/'], 'users': [100], 'cv': [10]})

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '札幌', 'ga4_property_id': '12345'}]
        
        result = app.report.run.all(
            sites,
            d=[('landingPage', 'page')],
            m=[
                ('activeUsers', 'users'),  # グローバルfilter_dを使用
                ('totalPurchasers', 'cv', {'filter_d': 'sessionDefaultChannelGroup==Organic Search'}),  # 明示的に同じ値
            ],
            filter_d='sessionDefaultChannelGroup==Organic Search',  # グローバル
            item_key='clinic',
            verbose=False,
        )
    
    # グローバルと明示的が同じ値なので1回のAPIコールのみ（最適化）
    assert call_count == 1
    assert filters_used == ['sessionDefaultChannelGroup==Organic Search']
    assert result['users'].iloc[0] == 100
    assert result['cv'].iloc[0] == 10


def test_report_run_all_site_dimension_string_with_multiple_metrics():
    """文字列の site.<key> を使用してメトリクス統合時の列名解決をテスト
    
    バグ再現：文字列 'site.lp_dim' を d に渡すと、resolved_d では 'landingPage' に解決されるが、
    マージ時の dim_cols 取得で元の d を使うと 'site.lp_dim' を列名として探してしまいKeyError
    
    修正後：resolved_d から dim_cols を取得することで正しく 'landingPage' を使用してマージ
    """
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    call_count = 0

    def mock_call(self, d, m, filter_d=None, **kwargs):
        nonlocal call_count
        call_count += 1
        
        # filter_dに応じて異なるデータを返す（列名はAPIが返す実際の名前）
        if filter_d == 'filter1':
            self.parent.data = pd.DataFrame({'month': ['202501'], 'landingPage': ['/page1'], 'users': [100]})
        elif filter_d == 'filter2':
            self.parent.data = pd.DataFrame({'month': ['202501'], 'landingPage': ['/page1'], 'cv': [10]})
        else:
            self.parent.data = pd.DataFrame()

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '札幌', 'ga4_property_id': '12345', 'lp_dim': 'landingPage'}]
        
        result = app.report.run.all(
            sites,
            d=[('yearMonth', 'month'), 'site.lp_dim'],  # 文字列パターンで site.lp_dim -> 'landingPage' に解決
            m=[
                ('activeUsers', 'users', {'filter_d': 'filter1'}),
                ('totalPurchasers', 'cv', {'filter_d': 'filter2'}),
            ],
            item_key='clinic',
            verbose=False,
        )
    
    # 2つの異なるfilter_dで2回APIコール
    assert call_count == 2
    # マージが成功することを確認（KeyErrorが発生しない）
    assert len(result) == 1
    assert 'landingPage' in result.columns  # site.lp_dim が landingPage に解決されている
    assert 'month' in result.columns
    assert 'clinic' in result.columns
    assert 'users' in result.columns
    assert 'cv' in result.columns
    assert result['users'].iloc[0] == 100
    assert result['cv'].iloc[0] == 10


def test_report_run_all_custom_metrics_dimensions():
    """カスタムメトリクス（cv, ad_cost等）が dimensions に入らないことを確認"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    def mock_call(self, d, m, filter_d=None, **kwargs):
        # カスタムメトリクスを含むデータを返す
        self.parent.data = pd.DataFrame({
            'month': ['202501', '202501'],
            'sessionSource': ['google', 'yahoo'],
            'sessions': [100, 50],
            'cv': [10, 5],  # カスタムメトリクス
            'ad_cost': [1000.0, 500.0],  # カスタムメトリクス
        })

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '札幌', 'ga4_property_id': '12345'}]
        
        result = app.report.run.all(
            sites,
            d=[('yearMonth', 'month'), ('sessionDefaultChannelGroup', 'sessionSource')],
            m=[('sessions', 'sessions'), ('cv', 'cv'), ('ad_cost', 'ad_cost')],
            item_key='clinic',
            verbose=False,
        )
    
    # カスタムメトリクスが dimensions に含まれない
    assert 'cv' not in result.dimensions
    assert 'ad_cost' not in result.dimensions
    # ディメンションのみが含まれる（item_key も除外される）
    assert 'month' in result.dimensions
    assert 'sessionSource' in result.dimensions
    assert 'clinic' not in result.dimensions  # item_key は除外


def test_report_run_all_numeric_dimensions_protected():
    """数値型ディメンション（month, yearMonth等）が dimensions に正しく含まれることを確認"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    def mock_call(self, d, m, filter_d=None, **kwargs):
        # 数値型ディメンションを含むデータを返す
        self.parent.data = pd.DataFrame({
            'month': [202501, 202501],  # 数値型だが既知のディメンション
            'yearMonth': [202501, 202501],  # 数値型だが既知のディメンション
            'sessionSource': ['google', 'yahoo'],
            'sessions': [100, 50],
            'cv': [10, 5],  # カスタムメトリクス
        })

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '札幌', 'ga4_property_id': '12345'}]
        
        result = app.report.run.all(
            sites,
            d=[('month', 'month'), ('yearMonth', 'yearMonth'), ('sessionSource', 'sessionSource')],
            m=[('sessions', 'sessions'), ('cv', 'cv')],
            item_key='clinic',
            verbose=False,
        )
    
    # 数値型でも既知のディメンションは dimensions に含まれる
    assert 'month' in result.dimensions
    assert 'yearMonth' in result.dimensions
    assert 'sessionSource' in result.dimensions
    # メトリクスは除外される
    assert 'sessions' not in result.dimensions
    assert 'cv' not in result.dimensions
    # item_key は除外される
    assert 'clinic' not in result.dimensions


def test_report_run_all_explicit_dimension_alias_kept():
    """明示指定の次元エイリアス（数値型でも）を dimensions に保持"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    def mock_call(self, d, m, filter_d=None, **kwargs):
        self.parent.data = pd.DataFrame({
            'ym': [202501, 202502],
            'sessions': [100, 50],
        })

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '札幌', 'ga4_property_id': '12345'}]
        
        result = app.report.run.all(
            sites,
            d=[('yearMonth', 'ym')],
            m=[('sessions', 'sessions')],
            item_key='clinic',
            verbose=False,
        )
    
    assert 'ym' in result.dimensions
    assert 'sessions' not in result.dimensions
    assert 'clinic' not in result.dimensions


def test_report_run_all_empty_result_preserves_dimensions():
    """空結果でも明示指定したディメンションが保持されることを確認"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    def mock_call(self, d, m, filter_d=None, **kwargs):
        # 空DataFrameを返す（列はあるが0行）
        self.parent.data = pd.DataFrame(columns=['month', 'source', 'sessions'])

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '札幌', 'ga4_property_id': '12345'}]
        
        result = app.report.run.all(
            sites,
            d=[('yearMonth', 'month'), ('sessionSource', 'source')],
            m=[('sessions', 'sessions')],
            item_key='clinic',
            verbose=False,
        )
    
    # 空結果でもディメンション情報が保持される
    assert len(result) == 0
    assert 'month' in result.dimensions
    assert 'source' in result.dimensions
    assert 'sessions' not in result.dimensions
    # メソッドチェーンが動作することを確認
    filled = result.fill()
    assert filled.dimensions == result.dimensions

def test_report_run_all_empty_no_site_prefix_in_dimensions():
    """全サイトスキップ時、site.プレフィックスが dimensions に残らないことを確認"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    # 全サイトで ga4_property_id が欠落しているケース
    sites = [
        {'clinic': '札幌'},  # ga4_property_id なし
        {'clinic': '仙台'},  # ga4_property_id なし
    ]
    
    result = app.report.run.all(
        sites,
        d=['site.lp_dim', ('yearMonth', 'month')],  # 文字列で site.lp_dim を指定
        m=[('sessions', 'sessions')],
        item_key='clinic',
        verbose=False,
    )
    
    # 空結果でも site. プレフィックスが除去される
    assert len(result) == 0
    assert 'site.lp_dim' not in result.dimensions
    assert 'lp_dim' in result.dimensions  # site. が除去された列名
    assert 'month' in result.dimensions


def test_report_run_all_site_filter_d():
    """site.filter_d パターンでサイト別にfilter_dを動的解決"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))
    
    filter_d_values = []
    
    def mock_call(self, d, m, **kwargs):
        filter_d_values.append(kwargs.get('filter_d'))
        self.parent.data = pd.DataFrame({'yearMonth': ['202501'], 'sessions': [100]})
    
    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [
            {'clinic': 'dentamap', 'ga4_property_id': '111', 'filter_d': 'sessionDefaultChannelGroup==Organic Social'},
            {'clinic': '札幌', 'ga4_property_id': '222', 'filter_d': ''},  # filter_d は空文字列
        ]
        
        result = app.report.run.all(
            sites,
            d=[('yearMonth', 'month')],
            m=[('sessions', 'sessions')],
            filter_d='site.filter_d',  # site.filter_d パターン
            item_key='clinic',
            verbose=False,
        )
    
    assert len(result) == 2
    assert set(result.df['clinic']) == {'dentamap', '札幌'}
    # filter_dが正しく解決された
    assert 'sessionDefaultChannelGroup==Organic Social' in filter_d_values
    assert None in filter_d_values  # 空文字列は None に変換される


def test_report_run_all_site_filter_d_missing_key():
    """site.filter_d でキーが存在しない場合はエラー（次元・メトリクスと同じ動作）"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))
    
    def mock_call(self, d, m, **kwargs):
        self.parent.data = pd.DataFrame({'yearMonth': ['202501'], 'sessions': [100]})
    
    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [
            {'clinic': '札幌', 'ga4_property_id': '222'},  # filter_d キーが存在しない
        ]
        
        with pytest.raises(ValueError, match="Site key 'filter_d' not found in site '札幌'"):
            app.report.run.all(
                sites,
                d=[('yearMonth', 'month')],
                m=[('sessions', 'sessions')],
                filter_d='site.filter_d',
                item_key='clinic',
                verbose=False,
            )


def test_report_run_all_site_filter_d_empty_value():
    """site.filter_d で値が空文字列の場合はfilter_dなしとして扱う"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))
    
    call_count = [0]
    filter_d_values = []
    
    def mock_call(self, d, m, **kwargs):
        call_count[0] += 1
        filter_d_values.append(kwargs.get('filter_d'))
        self.parent.data = pd.DataFrame({'yearMonth': ['202501'], 'sessions': [100]})
    
    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [
            {'clinic': 'A', 'ga4_property_id': '111', 'filter_d': ''},  # 空文字列
            {'clinic': 'B', 'ga4_property_id': '222', 'filter_d': None},  # None
        ]
        
        result = app.report.run.all(
            sites,
            d=[('yearMonth', 'month')],
            m=[('sessions', 'sessions')],
            filter_d='site.filter_d',
            item_key='clinic',
            verbose=False,
        )
    
    assert len(result) == 2
    # 両方ともfilter_dがNoneで呼ばれることを確認
    assert all(fd is None for fd in filter_d_values)


def test_report_run_all_metric_level_site_filter_d():
    """メトリクス個別 filter_d での site.<key> 解決をテスト"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))
    
    call_count = [0]
    filter_d_values = []
    metric_names_per_call = []
    
    def mock_call(self, d, m, **kwargs):
        call_count[0] += 1
        filter_d_values.append(kwargs.get('filter_d'))
        
        # メトリクス名を記録（どのメトリクスがどのfilter_dで呼ばれたか）
        m_names = []
        m_aliases = []
        for metric in m:
            if isinstance(metric, tuple):
                m_names.append(metric[0])
                m_aliases.append(metric[1] if len(metric) >= 2 else metric[0])
            else:
                m_names.append(metric)
                m_aliases.append(metric)
        metric_names_per_call.append(m_names)
        
        # 次元のエイリアス名を使用
        d_aliases = []
        for dim in d:
            if isinstance(dim, tuple):
                d_aliases.append(dim[1] if len(dim) >= 2 else dim[0])
            else:
                d_aliases.append(dim)
        
        # モックデータを返す（次元はエイリアス名を使用）
        data = {}
        for alias in d_aliases:
            data[alias] = ['202501']
        for alias in m_aliases:
            data[alias] = [100]
        self.parent.data = pd.DataFrame(data)
    
    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [
            {'clinic': 'A', 'ga4_property_id': '111', 'sns_filter': 'channel==Organic Social'},
            {'clinic': 'B', 'ga4_property_id': '222', 'sns_filter': 'channel==Paid Social'},
        ]
        
        # メトリクス個別に site.<key> を使用
        result = app.report.run.all(
            sites,
            d=[('yearMonth', 'month')],
            m=[
                ('activeUsers', 'users'),
                ('sessions', 'sessions', {'filter_d': 'site.sns_filter'}),  # メトリクス個別
            ],
            item_key='clinic',
            verbose=False,
        )
    
    # APIコール回数の確認
    # A: users (no filter), sessions (channel==Organic Social)
    # B: users (no filter), sessions (channel==Paid Social)
    assert call_count[0] == 4  # 2サイト × 2グループ
    
    # filter_dの値を確認
    assert 'channel==Organic Social' in filter_d_values
    assert 'channel==Paid Social' in filter_d_values
    assert filter_d_values.count(None) == 2  # filter_dなしのメトリクス(users)が2回
    
    # 結果の確認
    assert len(result) > 0
    assert 'users' in result.df.columns
    assert 'sessions' in result.df.columns


