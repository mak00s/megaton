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
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert 'site' in result.columns
    assert set(result['site']) == {'siteA', 'siteB'}


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
    
    assert len(result) == 2
    assert set(result['clinic']) == {'A', 'C'}


def test_search_run_all_with_add_month():
    """Test Search Console with add_month parameter"""
    app = Megaton(None, headless=True)
    app.search.start_date = "2025-01-01"
    app.search.end_date = "2025-01-31"
    
    mock_df = pd.DataFrame({'query': ['test'], 'clicks': [5]})
    
    with patch.object(app.search.run.__class__, '__call__', return_value=mock_df):
        with patch.object(app.search, 'use'):
            sites = [{'site': 'A', 'gsc_site_url': 'https://a.com/'}]
            
            # Test with string
            result = app.search.run.all(
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
            result = app.search.run.all(
                sites,
                dimensions=['query'],
                add_month=p,
                verbose=False,
            )
            assert result['month'].iloc[0] == '202501'


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
    
    assert len(result) == 1
    assert result['site'].iloc[0] == 'A'


def test_report_run_all_basic():
    """Test basic GA4 multi-item run"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    # Mock GA4 structure
    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))
    
    mock_df = pd.DataFrame({'date': ['2025-01-01'], 'users': [100]})
    
    # Mock the __call__ method and report.data
    with patch.object(app.report.run.__class__, '__call__'):
        app.report.data = mock_df
        
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
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert 'site' in result.columns
    assert set(result['site']) == {'siteA', 'siteB'}


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
    assert len(result) == 2
    assert set(result['clinic']) == {'A', 'C'}
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
