"""
Tests for Report.show() and Report.download() methods.
These tests ensure that show() and download() correctly access self.data.
"""
import pandas as pd
from unittest.mock import MagicMock, patch
from megaton.start import Megaton, ReportResult


def test_report_show_uses_self_data():
    """Test that Report.show() uses self.data, not self.parent.data"""
    # Setup
    mg = Megaton()
    mg.ga = {'4': MagicMock()}
    mg.ga['4'].account.id = '12345'
    mg.ga['4'].property.id = '67890'
    
    # Create test data
    test_df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    
    # Mock the GA4 report.run method to return test data
    with patch.object(mg.ga['4'].report, 'run', return_value=test_df):
        mg.ga['4'].report.run(d=[('date', 'date')], m=['users'])
    
    # Set report.data
    mg.report.data = test_df
    
    # Mock show.table to verify it's called with correct data
    with patch.object(mg.show, 'table') as mock_show_table:
        # Call show() through Report
        mg.report.show()
        
        # Verify show.table was called with self.data
        mock_show_table.assert_called_once()
        # Get the actual DataFrame passed to show.table
        actual_df = mock_show_table.call_args[0][0]
        pd.testing.assert_frame_equal(actual_df, test_df)


def test_report_download_uses_self_data():
    """Test that Report.download() uses self.data, not self.parent.data"""
    # Setup
    mg = Megaton()
    mg.ga = {'4': MagicMock()}
    mg.ga['4'].account.id = '12345'
    mg.ga['4'].property.id = '67890'
    
    # Create test data
    test_df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    
    # Set report.data
    mg.report.data = test_df
    
    # Mock download method to verify it's called with correct data
    with patch.object(mg, 'download') as mock_download:
        # Call download() through Report
        mg.report.download('test.csv')
        
        # Verify download was called with self.data
        mock_download.assert_called_once()
        actual_df = mock_download.call_args[0][0]
        filename = mock_download.call_args[0][1]
        
        pd.testing.assert_frame_equal(actual_df, test_df)
        assert filename == 'test.csv'


def test_report_run_returns_report_result():
    """Test that Report.run() returns ReportResult and still calls show()"""
    # Setup
    mg = Megaton()
    mg.ga = {'4': MagicMock()}
    mg.ga['4'].account.id = '12345'
    mg.ga['4'].property.id = '67890'
    
    # Create test data
    test_df = pd.DataFrame({'date': ['2024-01-01'], 'users': [100]})
    
    # Mock GA4 report.run to return test data
    mg.ga['4'].report.run = MagicMock(return_value=test_df)
    
    # Mock show.table to verify it's called
    with patch.object(mg.show, 'table') as mock_show:
        # Call report.run
        result = mg.report.run(d=[('date', 'date')], m=['users'])

        assert isinstance(result, ReportResult)
        pd.testing.assert_frame_equal(result.df, test_df)
        mock_show.assert_called_once()


def test_report_show_with_no_data():
    """Test that Report.show() handles case when self.data is None"""
    # Setup
    mg = Megaton()
    mg.ga = {'4': MagicMock()}
    mg.ga['4'].account.id = '12345'
    mg.ga['4'].property.id = '67890'
    
    # Set report.data to None
    mg.report.data = None
    
    # Mock show.table
    with patch.object(mg.show, 'table') as mock_show_table:
        # Call show() should not raise error
        mg.report.show()
        
        # Verify show.table was called with None
        mock_show_table.assert_called_once_with(None)


def test_report_download_with_no_data():
    """Test that Report.download() handles case when self.data is None"""
    # Setup
    mg = Megaton()
    mg.ga = {'4': MagicMock()}
    
    # Set report.data to None
    mg.report.data = None
    
    # Mock download method
    with patch.object(mg, 'download') as mock_download:
        # Call download() should not raise error
        mg.report.download('test.csv')
        
        # Verify download was called with None
        mock_download.assert_called_once_with(None, 'test.csv')
