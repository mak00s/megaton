"""Tests for ReportResult class"""
import pytest
import pandas as pd
from megaton.start import ReportResult


def test_report_result_init():
    """ReportResult の初期化テスト"""
    df = pd.DataFrame({
        'date': ['2024-01', '2024-02'],
        'sessionSource': ['google', 'yahoo'],
        'sessions': [100, 50]
    })
    
    result = ReportResult(df)
    
    assert isinstance(result, ReportResult)
    assert len(result) == 2
    assert result.columns == ['date', 'sessionSource', 'sessions']
    assert result.dimensions == ['date', 'sessionSource']
    assert not result.empty


def test_report_result_init_with_dimensions():
    """明示的な dimensions 指定"""
    df = pd.DataFrame({
        'date': ['2024-01'],
        'sessions': [100]
    })
    
    result = ReportResult(df, dimensions=['date'])
    
    assert result.dimensions == ['date']


def test_report_result_auto_detect_dimensions():
    """指標列の自動検出"""
    df = pd.DataFrame({
        'date': ['2024-01'],
        'sessionSource': ['google'],
        'sessions': [100],
        'users': [80],
        'activeUsers': [60]
    })
    
    result = ReportResult(df)
    
    # 指標列は dimensions に含まれない
    assert 'sessions' not in result.dimensions
    assert 'users' not in result.dimensions
    assert 'activeUsers' not in result.dimensions
    # ディメンション列のみが含まれる
    assert 'date' in result.dimensions
    assert 'sessionSource' in result.dimensions


def test_report_result_numeric_dimensions_protected():
    """数値型ディメンション（month, yearMonth等）が dimensions に正しく含まれることを確認"""
    df = pd.DataFrame({
        'month': [202401, 202402],  # 数値型だが既知のディメンション
        'yearMonth': [202401, 202402],  # 数値型だが既知のディメンション
        'sessionSource': ['google', 'yahoo'],
        'sessions': [100, 50],
        'cv': [10, 5],  # カスタムメトリクス（数値型）
    })
    
    result = ReportResult(df)
    
    # 数値型でも既知のディメンションは dimensions に含まれる
    assert 'month' in result.dimensions
    assert 'yearMonth' in result.dimensions
    assert 'sessionSource' in result.dimensions
    # メトリクスは除外される
    assert 'sessions' not in result.dimensions
    assert 'cv' not in result.dimensions
    
    # fill() で正しく動作することを確認
    result_filled = result.fill()
    # month と yearMonth がディメンションとして扱われることを確認（直積が作られる）
    assert len(result_filled) >= len(result)


def test_report_result_backward_compatibility():
    """後方互換性のテスト（DataFrame的な操作）"""
    df = pd.DataFrame({
        'date': ['2024-01'],
        'sessions': [100]
    })
    
    result = ReportResult(df)
    
    # __getitem__
    assert (result['date'] == df['date']).all()
    assert (result['sessions'] == df['sessions']).all()
    
    # len
    assert len(result) == len(df)
    
    # .df property
    pd.testing.assert_frame_equal(result.df, df)


def test_report_result_classify():
    """classify() メソッドのテスト"""
    df = pd.DataFrame({
        'sessionSource': ['google search', 'yahoo', 'direct'],
        'sessions': [100, 50, 30]
    })
    
    result = ReportResult(df)
    classified = result.classify(
        dimension='sessionSource',
        by={'.*google.*': 'Search', '.*yahoo.*': 'Search'},
        output='source_type'
    )
    
    assert 'source_type' in classified.columns
    assert 'source_type' in classified.dimensions
    # group=True（デフォルト）なので集計されている
    assert len(classified) == 2  # Search と (other)
    search_row = classified.df[classified.df['source_type'] == 'Search']
    assert search_row['sessions'].values[0] == 150  # 100 + 50


def test_report_result_classify_no_group():
    """classify() group=False のテスト"""
    df = pd.DataFrame({
        'sessionSource': ['google search', 'yahoo', 'direct'],
        'sessions': [100, 50, 30]
    })
    
    result = ReportResult(df)
    classified = result.classify(
        dimension='sessionSource',
        by={'.*google.*': 'Search', '.*yahoo.*': 'Search'},
        output='source_type',
        group=False
    )
    
    assert 'source_type' in classified.columns
    assert 'source_type' in classified.dimensions
    # group=False なので元の行数を保持
    assert len(classified) == 3
    assert classified['source_type'][0] == 'Search'
    assert classified['source_type'][1] == 'Search'
    assert classified['source_type'][2] == '(other)'


def test_report_result_classify_default_output():
    """classify() デフォルト出力列名のテスト"""
    df = pd.DataFrame({
        'sessionSource': ['google', 'yahoo'],
        'sessions': [100, 50]
    })
    
    result = ReportResult(df)
    classified = result.classify(
        dimension='sessionSource',
        by={'google': 'Google'},
        group=False  # 集計なしで出力列名のみ確認
    )
    
    assert 'sessionSource_category' in classified.columns


def test_report_result_classify_custom_default():
    """classify() カスタムデフォルト値のテスト"""
    df = pd.DataFrame({
        'sessionSource': ['google', 'unknown'],
        'sessions': [100, 50]
    })
    
    result = ReportResult(df)
    classified = result.classify(
        dimension='sessionSource',
        by={'google': 'Google'},
        default='Others',
        group=False  # 集計なしでデフォルト値のみ確認
    )
    
    assert classified['sessionSource_category'][1] == 'Others'


def test_report_result_group():
    """group() メソッドのテスト"""
    df = pd.DataFrame({
        'date': ['2024-01', '2024-01', '2024-02'],
        'sessionSource': ['google', 'yahoo', 'google'],
        'sessions': [100, 50, 150]
    })
    
    result = ReportResult(df)
    grouped = result.group(by='sessionSource', metrics=['sessions'])
    
    assert len(grouped) == 2
    assert grouped.dimensions == ['sessionSource']
    assert grouped.df[grouped.df['sessionSource'] == 'google']['sessions'].values[0] == 250


def test_report_result_group_multiple_dimensions():
    """group() 複数ディメンションのテスト"""
    df = pd.DataFrame({
        'date': ['2024-01', '2024-01', '2024-02'],
        'sessionSource': ['google', 'google', 'yahoo'],
        'sessions': [100, 50, 150]
    })
    
    result = ReportResult(df)
    grouped = result.group(by=['date', 'sessionSource'])
    
    # google + google で集計されるため 2 行
    assert len(grouped) == 2
    assert grouped.dimensions == ['date', 'sessionSource']
    # 2024-01 google の sessions は 100 + 50 = 150
    assert grouped.df[(grouped.df['date'] == '2024-01') & 
                      (grouped.df['sessionSource'] == 'google')]['sessions'].values[0] == 150


def test_report_result_group_auto_detect_metrics():
    """group() 指標自動検出のテスト"""
    df = pd.DataFrame({
        'date': ['2024-01', '2024-01'],
        'sessions': [100, 50],
        'users': [80, 40]
    })
    
    result = ReportResult(df)
    grouped = result.group(by='date')
    
    assert grouped.df['sessions'].values[0] == 150
    assert grouped.df['users'].values[0] == 120


def test_report_result_group_method():
    """group() 集計方法のテスト"""
    df = pd.DataFrame({
        'date': ['2024-01', '2024-01'],
        'sessions': [100, 50]
    })
    
    result = ReportResult(df)
    
    # sum
    grouped_sum = result.group(by='date', metrics=['sessions'], method='sum')
    assert grouped_sum.df['sessions'].values[0] == 150
    
    # mean
    grouped_mean = result.group(by='date', metrics=['sessions'], method='mean')
    assert grouped_mean.df['sessions'].values[0] == 75


def test_report_result_group_empty_dataframe():
    """group() 空DataFrameのテスト"""
    # 空DataFrameでmetrics明示指定
    df = pd.DataFrame(columns=['date', 'sessionSource', 'sessions', 'users'])
    result = ReportResult(df, dimensions=['date', 'sessionSource'])
    
    grouped = result.group(by=['date', 'sessionSource'], metrics=['sessions', 'users'])
    
    assert grouped.df.empty
    assert list(grouped.df.columns) == ['date', 'sessionSource', 'sessions', 'users']
    assert grouped.dimensions == ['date', 'sessionSource']


def test_report_result_group_empty_auto_metrics():
    """group() 空DataFrameで自動metrics検出のテスト"""
    df = pd.DataFrame(columns=['date', 'sessions'])
    result = ReportResult(df, dimensions=['date'])
    
    grouped = result.group(by='date')
    
    assert grouped.df.empty
    assert 'date' in grouped.df.columns
    assert grouped.dimensions == ['date']


def test_report_result_group_missing_metrics():
    """group() 指定したmetricsが存在しない場合のテスト"""
    df = pd.DataFrame({
        'date': ['2024-01', '2024-01'],
        'sessions': [100, 50]
    })
    
    result = ReportResult(df)
    
    # 存在しないmetric列を指定
    grouped = result.group(by='date', metrics=['sessions', 'nonexistent_metric'])
    
    # 存在する列のみ集計される
    assert 'sessions' in grouped.df.columns
    assert 'nonexistent_metric' not in grouped.df.columns
    assert grouped.df['sessions'].values[0] == 150


def test_report_result_group_empty_with_ghost_metrics():
    """group() 空DataFrameで存在しない列を指定した場合のテスト"""
    df = pd.DataFrame(columns=['date', 'sessions'])
    result = ReportResult(df, dimensions=['date'])
    
    # 存在しないmetric列を指定
    grouped = result.group(by='date', metrics=['sessions', 'ghost'])
    
    # 存在する列のみ含まれる
    assert grouped.df.empty
    assert 'sessions' in grouped.df.columns
    assert 'ghost' not in grouped.df.columns
    assert list(grouped.df.columns) == ['date', 'sessions']


def test_report_result_group_metrics_as_string():
    """group() metricsを文字列で指定した場合のテスト"""
    df = pd.DataFrame({
        'date': ['2024-01', '2024-01'],
        'sessions': [100, 50]
    })
    
    result = ReportResult(df)
    
    # metricsを文字列で指定（リストではなく）
    grouped = result.group(by='date', metrics='sessions')
    
    assert 'sessions' in grouped.df.columns
    assert grouped.df['sessions'].values[0] == 150
    assert grouped.dimensions == ['date']


def test_report_result_sort():
    """sort() メソッドのテスト"""
    df = pd.DataFrame({
        'sessionSource': ['yahoo', 'google', 'direct'],
        'sessions': [50, 100, 30]
    })
    
    result = ReportResult(df)
    sorted_result = result.sort(by='sessions', ascending=False)
    
    assert sorted_result.df['sessions'].values[0] == 100
    assert sorted_result.df['sessions'].values[1] == 50
    assert sorted_result.df['sessions'].values[2] == 30


def test_report_result_sort_multiple_columns():
    """sort() 複数列ソートのテスト"""
    df = pd.DataFrame({
        'date': ['2024-02', '2024-01', '2024-02'],
        'sessions': [100, 50, 150]
    })
    
    result = ReportResult(df)
    sorted_result = result.sort(by=['date', 'sessions'], ascending=[True, False])
    
    assert sorted_result.df['date'].values[0] == '2024-01'
    assert sorted_result.df['date'].values[1] == '2024-02'
    assert sorted_result.df['sessions'].values[1] == 150


def test_report_result_fill():
    """fill() メソッドのテスト"""
    df = pd.DataFrame({
        'date': ['2024-01', None, '2024-02'],
        'sessionSource': ['google', 'yahoo', None],
        'sessions': [100, 50, 30]
    })
    
    result = ReportResult(df)
    filled = result.fill()
    
    assert filled.df['date'].isna().sum() == 0
    assert filled.df['sessionSource'].isna().sum() == 0
    assert filled.df['date'].values[1] == '(not set)'
    assert filled.df['sessionSource'].values[2] == '(not set)'


def test_report_result_fill_custom_value():
    """fill() カスタム値のテスト"""
    df = pd.DataFrame({
        'date': [None],
        'sessions': [100]
    })
    
    result = ReportResult(df)
    filled = result.fill(to='Unknown')
    
    assert filled.df['date'].values[0] == 'Unknown'


def test_report_result_fill_specific_dimensions():
    """fill() 特定ディメンションのテスト"""
    df = pd.DataFrame({
        'date': [None],
        'sessionSource': [None],
        'sessions': [100]
    })
    
    result = ReportResult(df, dimensions=['date', 'sessionSource'])
    filled = result.fill(dimensions=['date'])
    
    assert filled.df['date'].values[0] == '(not set)'
    assert pd.isna(filled.df['sessionSource'].values[0])


def test_report_result_to_int():
    """to_int() メソッドのテスト"""
    df = pd.DataFrame({
        'date': ['2024-01'],
        'sessions': [100.5],
        'users': [80.9]
    })
    
    result = ReportResult(df)
    int_result = result.to_int(metrics='sessions')
    
    assert int_result.df['sessions'].dtype == 'int64'
    assert int_result.df['sessions'].values[0] == 100
    assert int_result.df['users'].dtype == 'float64'


def test_report_result_to_int_multiple_metrics():
    """to_int() 複数指標のテスト"""
    df = pd.DataFrame({
        'sessions': [100.5],
        'users': [80.9]
    })
    
    result = ReportResult(df)
    int_result = result.to_int(metrics=['sessions', 'users'])
    
    assert int_result.df['sessions'].dtype == 'int64'
    assert int_result.df['users'].dtype == 'int64'


def test_report_result_to_int_with_nan():
    """to_int() 欠損値のテスト"""
    df = pd.DataFrame({
        'sessions': [100.5, None, 50.2]
    })
    
    result = ReportResult(df)
    int_result = result.to_int(metrics='sessions', fill_value=0)
    
    assert int_result.df['sessions'].values[1] == 0


def test_report_result_replace():
    """replace() メソッドのテスト（固定文字列）"""
    df = pd.DataFrame({
        'sessionSource': ['google', 'yahoo', 'direct'],
        'sessions': [100, 50, 30]
    })
    
    result = ReportResult(df)
    replaced = result.replace(
        dimension='sessionSource',
        by={'google': 'Google', 'yahoo': 'Yahoo!'},
        regex=False  # 固定文字列として扱う
    )
    
    assert replaced.df['sessionSource'].values[0] == 'Google'
    assert replaced.df['sessionSource'].values[1] == 'Yahoo!'
    assert replaced.df['sessionSource'].values[2] == 'direct'


def test_report_result_replace_regex():
    """replace() メソッドのテスト（正規表現、default）"""
    df = pd.DataFrame({
        'campaign': ['test(123)', 'hello(world)', 'plain'],
        'sessions': [100, 50, 30]
    })
    
    result = ReportResult(df)
    
    # 正規表現で括弧内を削除（regex=True がデフォルト）
    replaced = result.replace(
        dimension='campaign',
        by={r'\([^)]*\)': ''}
    )
    
    assert replaced.df['campaign'].values[0] == 'test'
    assert replaced.df['campaign'].values[1] == 'hello'
    assert replaced.df['campaign'].values[2] == 'plain'


def test_report_result_replace_regex_multiple():
    """replace() メソッドで複数の正規表現パターンを適用"""
    df = pd.DataFrame({
        'campaign': ['Sale(2024)', 'New(Launch)', 'plain'],
        'sessions': [100, 50, 30]
    })
    
    result = ReportResult(df)
    
    # 括弧内を削除する正規表現
    replaced = result.replace(
        dimension='campaign',
        by={r'\(.*?\)': ''}
    )
    
    assert replaced.df['campaign'].values[0] == 'Sale'
    assert replaced.df['campaign'].values[1] == 'New'
    assert replaced.df['campaign'].values[2] == 'plain'


def test_report_result_method_chaining():
    """メソッドチェーンのテスト"""
    df = pd.DataFrame({
        'date': ['2024-01', '2024-01', None],
        'sessionSource': ['google', 'yahoo', 'direct'],
        'sessions': [100.5, 50.2, 30.8]
    })
    
    result = (ReportResult(df)
        .fill(to='(no date)', dimensions=['date'])
        .classify(
            dimension='sessionSource',
            by={'.*google.*': 'Search', '.*yahoo.*': 'Search'},
            output='source_type',
            group=False  # まだ集計しない
        )
        .group(by=['date', 'source_type'], metrics=['sessions'])
        .to_int(metrics='sessions')
        .sort(by='sessions', ascending=False)
    )
    
    assert 'source_type' in result.columns
    assert result.df['sessions'].dtype == 'int64'
    assert result.df['sessions'].values[0] == result.df['sessions'].max()


def test_report_result_empty_dataframe():
    """空のDataFrameのテスト"""
    df = pd.DataFrame()
    result = ReportResult(df)
    
    assert result.empty
    assert len(result) == 0


def test_report_result_repr():
    """__repr__() のテスト"""
    df = pd.DataFrame({
        'date': ['2024-01', '2024-02'],
        'sessions': [100, 50]
    })
    
    result = ReportResult(df)
    repr_str = repr(result)
    
    assert 'ReportResult' in repr_str
    assert '2 rows' in repr_str
    assert '2 columns' in repr_str


def test_report_result_classify_missing_column():
    """classify() 存在しない列のエラーテスト"""
    df = pd.DataFrame({
        'date': ['2024-01'],
        'sessions': [100]
    })
    
    result = ReportResult(df)
    
    with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
        result.classify(dimension='nonexistent', by={'x': 'y'})


def test_report_result_replace_missing_column():
    """replace() 存在しない列のエラーテスト"""
    df = pd.DataFrame({
        'date': ['2024-01'],
        'sessions': [100]
    })
    
    result = ReportResult(df)
    
    with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
        result.replace(dimension='nonexistent', by={'x': 'y'})


def test_report_result_custom_metrics_not_in_dimensions():
    """カスタムメトリクス（cv, ad_cost, totalPurchasers等）が dimensions に入らないことを確認"""
    df = pd.DataFrame({
        'date': ['2024-01', '2024-01'],
        'sessionSource': ['google', 'yahoo'],
        'sessions': [100, 50],
        'cv': [10, 5],  # カスタムメトリクス（数値型）
        'ad_cost': [1000.5, 500.25],  # カスタムメトリクス（数値型）
        'totalPurchasers': [8, 4],  # 標準メトリクスだがKNOWN_GA4_METRICSに追加済み
    })
    
    result = ReportResult(df)
    
    # カスタムメトリクスは dimensions に含まれない（数値型として自動検出）
    assert 'cv' not in result.dimensions
    assert 'ad_cost' not in result.dimensions
    assert 'totalPurchasers' not in result.dimensions
    # ディメンションのみが含まれる
    assert result.dimensions == ['date', 'sessionSource']
    
    # classify() で正しく集計されることを確認
    classified = result.classify(dimension='sessionSource', by={
        'google': 'Search',
        'yahoo': 'Search'
    }, group=True)
    
    # group=True なので1行に集計され、カテゴリ列が作成される
    assert len(classified) == 1
    assert classified['sessionSource_category'].iloc[0] == 'Search'
    assert classified['sessions'].iloc[0] == 150  # 100 + 50
    assert classified['cv'].iloc[0] == 15  # 10 + 5
    assert classified['ad_cost'].iloc[0] == 1500.75  # 1000.5 + 500.25
    assert classified['totalPurchasers'].iloc[0] == 12  # 8 + 4


