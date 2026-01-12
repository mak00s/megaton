# Megaton Cheat Sheet

- 詳細: `api-reference.md`

## Start

- `mg = start.Megaton(creds)`
- `mg.open.sheet(url)`
- `mg.launch_bigquery(project)`

## GA4

- `mg.report.set.dates(date_from, date_to)`
- `mg.report.set.months(ago, window_months, tz?, now?, min_ymd?)`
- `mg.report.run(d, m, filter_d?, filter_m?, sort?, show?)`
- `mg.report.run.all(items, d, m, item_key?, property_key?, item_filter?)`
- `mg.report.prep(conf, df?)`
- `mg.report.data`

## Sheets (by name)

- `mg.save.to.sheet(name, df?, sort_by?, sort_desc?, auto_width?, freeze_header?)`
- `mg.append.to.sheet(name, df?)`
- `mg.upsert.to.sheet(name, df?, keys, columns?, sort_by?)`

## Sheets (current)

- `mg.sheets.select(name)`
- `mg.sheets.create(name)`
- `mg.sheets.delete(name)`
- `mg.sheet.save(df?, sort_by?, sort_desc?, auto_width?, freeze_header?)`
- `mg.sheet.append(df?)`
- `mg.sheet.upsert(df?, keys, columns?, sort_by?)`
- `mg.sheet.cell.set(cell, value)`
- `mg.sheet.range.set(a1_range, values)`

## Search Console

- `mg.search.use(site_url)`
- `mg.search.set.dates(date_from, date_to)`
- `mg.search.set.months(ago, window_months, tz?, now?, min_ymd?)`
- `mg.search.run(dimensions, metrics?, limit?, clean?, dimension_filter?)`
- `mg.search.run.all(items, dimensions, metrics?, item_key?, site_url_key?, item_filter?, dimension_filter?)`
- `mg.search.filter_by_thresholds(df, site, clicks_zero_only?)`
- `SearchResult: .decode() -> .clean_url() -> .remove_params() -> .remove_fragment() -> .lower()`
- `SearchResult: .normalize() -> .categorize(into=...) -> .classify() -> .aggregate()`

## Result

- `result.df`
- `result.fill(to?, dimensions?)`
- `result.group(by, metrics?, method?)`
- `result.to_int(metrics?, *, fill_value=0)`
- `result.clean_url(dimension, unquote?, drop_query?, drop_hash?, lower?)`

## Transform

- `ga4.classify_source_channel(df, channel_col?, medium_col?, source_col?, custom_channels?)`
- `ga4.classify_channel(df, channel_col?, medium_col?, source_col?, custom_channels?)`
- `ga4.convert_filter_to_event_scope(filter_d)`
- `text.map_by_regex(series, mapping, default?, flags?, lower?, strip?)`
- `text.clean_url(series, unquote?, drop_query?, drop_hash?, lower?)`
- `text.infer_site_from_url(url_val, sites, site_key?, id_key?)`
- `text.normalize_whitespace(series, mode?)`
- `text.force_text_if_numeric(series, prefix?)`
- `classify.classify_by_regex(df, src_col, mapping, out_col, default?)`
- `table.ensure_columns(df, columns, fill?, drop_extra?)`
- `table.normalize_key_cols(df, cols, to_str?, strip?, lower?, remove_trailing_dot0?)`
- `table.group_sum(df, group_cols, sum_cols)`
- `table.weighted_avg(df, group_cols, value_col, weight_col, out_col?)`
- `table.dedup_by_key(df, key_cols, prefer_by?, prefer_ascending?, keep?)`

## Files

- `mg.load.csv(path)`
- `mg.save_df(df, filename, mode?, include_dates?)`
- `mg.download(df, filename?)`

## BigQuery

- `bq = mg.launch_bigquery(project)`
- `bq.run(sql, to_dataframe=True)`
