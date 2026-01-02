"""Tests for mg.search.run.sites() and mg.report.run.sites()"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from megaton import dates
from megaton.start import Megaton


def test_search_run_sites_basic():
    """Test basic Search Console multi-site run"""
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
            
            result = app.search.run.sites(
                sites,
                dimensions=['query'],
                metrics=['clicks', 'impressions'],
                verbose=False,
            )
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert 'site' in result.columns
    assert set(result['site']) == {'siteA', 'siteB'}


def test_search_run_sites_with_filter_list():
    """Test Search Console with site_filter as list"""
    app = Megaton(None, headless=True)
    app.search.start_date = "2025-01-01"
    app.search.end_date = "2025-01-31"
    
    call_count = {'count': 0}
    
    def mock_call(self, dimensions, metrics, **kwargs):
        call_count['count'] += 1
        return pd.DataFrame({
            'query': ['test'],
            'clicks': [5],
        })
    
    with patch.object(app.search.run.__class__, '__call__', mock_call):
        with patch.object(app.search, 'use'):
            sites = [
                {'clinic': 'A', 'gsc_site_url': 'https://a.com/'},
                {'clinic': 'B', 'gsc_site_url': 'https://b.com/'},
                {'clinic': 'C', 'gsc_site_url': 'https://c.com/'},
            ]
            
            result = app.search.run.sites(
                sites,
                dimensions=['query'],
                site_key='clinic',
                site_filter=['A', 'C'],
                verbose=False,
            )
    
    assert len(result) == 2
    assert set(result['clinic']) == {'A', 'C'}
    assert call_count['count'] == 2


def test_search_run_sites_with_add_month():
    """Test Search Console with add_month parameter"""
    app = Megaton(None, headless=True)
    app.search.start_date = "2025-01-01"
    app.search.end_date = "2025-01-31"
    
    mock_df = pd.DataFrame({'query': ['test'], 'clicks': [5]})
    
    with patch.object(app.search.run.__class__, '__call__', return_value=mock_df):
        with patch.object(app.search, 'use'):
            sites = [{'site': 'A', 'gsc_site_url': 'https://a.com/'}]
            
            # Test with string
            result = app.search.run.sites(
                sites,
                dimensions=['query'],
                add_month='202501',
                verbose=False,
            )
            assert 'month' in result.columns
            assert result['month'].iloc[0] == '202501'
            
            # Test with DateWindow
            p = dates.DateWindow(
                start_iso='2025-01-01',
                end_iso='2025-01-31',
                start_ym='202501',
                end_ym='202501',
                start_ymd='20250101',
                end_ymd='20250131',
            )
            result = app.search.run.sites(
                sites,
                dimensions=['query'],
                add_month=p,
                verbose=False,
            )
            assert result['month'].iloc[0] == '202501'


def test_search_run_sites_url_fallback():
    """Test Search Console site_url_key fallback to 'url'"""
    app = Megaton(None, headless=True)
    app.search.start_date = "2025-01-01"
    app.search.end_date = "2025-01-31"
    
    mock_df = pd.DataFrame({'query': ['test'], 'clicks': [5]})
    
    with patch.object(app.search.run.__class__, '__call__', return_value=mock_df):
        with patch.object(app.search, 'use'):
            # Site without gsc_site_url but with url
            sites = [{'site': 'A', 'url': 'https://a.com/'}]
            
            result = app.search.run.sites(
                sites,
                dimensions=['query'],
                verbose=False,
            )
    
    assert len(result) == 1
    assert result['site'].iloc[0] == 'A'


def test_report_run_sites_basic():
    """Test basic GA4 multi-site run"""
    app = Megaton(None, headless=True)
    # Access ga[4] to initialize GA4 version
    app.ga['4'].property.id = None
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    call_count = {'count': 0}
    
    def mock_call(d, m, **kwargs):
        call_count['count'] += 1
        app.report.data = pd.DataFrame({
            'date': ['2025-01-01'],
            'users': [100],
        })
    
    app.report.run.__call__ = mock_call
    
    sites = [
        {'site': 'siteA', 'ga4_property_id': '123456'},
        {'site': 'siteB', 'ga4_property_id': '789012'},
    ]
    
    result = app.report.run.sites(
        sites,
        d=['date'],
        m=['users'],
        verbose=False,
    )
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert 'site' in result.columns
    assert set(result['site']) == {'siteA', 'siteB'}
    assert call_count['count'] == 2


def test_report_run_sites_with_dimensions_metrics():
    """Test GA4 with explicit dimensions/metrics parameters"""
    app = Megaton(None, headless=True)
    app.ga['4'].property.id = None
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    def mock_call(d, m, **kwargs):
        app.report.data = pd.DataFrame({'date': ['2025-01-01'], 'users': [50]})
    
    app.report.run.__call__ = mock_call
    
    sites = [{'site': 'A', 'ga4_property_id': '123'}]
    
    result = app.report.run.sites(
        sites,
        dimensions=['date'],
        metrics=['users'],
        verbose=False,
    )
    
    assert len(result) == 1


def test_report_run_sites_with_filter_function():
    """Test GA4 with site_filter as callable"""
    app = Megaton(None, headless=True)
    app.ga['4'].property.id = None
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    call_count = {'count': 0}
    
    def mock_call(d, m, **kwargs):
        call_count['count'] += 1
        app.report.data = pd.DataFrame({'users': [100]})
    
    app.report.run.__call__ = mock_call
    
    sites = [
        {'clinic': 'Tokyo', 'ga4_property_id': '123'},
        {'clinic': 'Osaka', 'ga4_property_id': '456'},
        {'clinic': 'Kyoto', 'ga4_property_id': '789'},
    ]
    
    # Filter function: only sites with 'o' in name
    result = app.report.run.sites(
        sites,
        d=['date'],
        m=['users'],
        site_key='clinic',
        site_filter=lambda s: 'o' in s.get('clinic', '').lower(),
        verbose=False,
    )
    
    assert len(result) == 2
    assert set(result['clinic']) == {'Tokyo', 'Kyoto'}
    assert call_count['count'] == 2


def test_report_run_sites_missing_property_id():
    """Test GA4 handles missing property_id gracefully"""
    app = Megaton(None, headless=True)
    app.ga['4'].property.id = None
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    def mock_call(d, m, **kwargs):
        app.report.data = pd.DataFrame({'users': [100]})
    
    app.report.run.__call__ = mock_call
    
    sites = [
        {'site': 'A', 'ga4_property_id': '123'},
        {'site': 'B'},  # Missing property_id
        {'site': 'C', 'ga4_property_id': '789'},
    ]
    
    result = app.report.run.sites(
        sites,
        d=['date'],
        m=['users'],
        verbose=False,
    )
    
    # Should only process sites A and C
    assert len(result) == 2
    assert set(result['site']) == {'A', 'C'}
