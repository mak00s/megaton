"""
Functions for Google Analytics 3 (Universal Analytics) API
"""

from typing import Optional
import json
import logging
import pandas as pd
import re
import sys

from googleapiclient import errors as err
from google.oauth2.credentials import Credentials

from . import constants, errors, ga4, google_api, utils

LOGGER = logging.getLogger(__name__)


class MegatonUA(ga4.MegatonGA4):
    this = "Megaton UA"

    def __init__(self, credentials: Credentials, **kwargs):
        super().__init__(credentials, **kwargs)
        self.view = self.View(self)

    def _build_client(self):
        self.data_client = google_api.GoogleApi(
            "analyticsreporting",
            "v4",
            constants.DEFAULT_SCOPES,
            credentials=self.credentials,
            credential_cache_file=self.credential_cache_file)
        self.admin_client = google_api.GoogleApi(
            "analytics",
            "v3",
            constants.DEFAULT_SCOPES,
            credentials=self.credentials,
            credential_cache_file=self.credential_cache_file)

    def _update(self):
        """Returns account summaries accessible by the caller."""
        try:
            response = self.admin_client.management().accountSummaries().list().execute()
        except err.HttpError as e:
            status = e.resp.status
            data = json.loads(e.content.decode('utf-8'))
            message = data['error']['message']
            reason = data['error']['errors'][0]['reason']
            if status == 403:
                if 'not have any Google' in message:
                    raise errors.NoDataReturned
                elif 'accessNotConfigured' in reason or 'disabled' in message:
                    raise errors.ApiDisabled(message, "Google Analytics API")
            raise e
        # except Exception as e:
        #     type, value, _ = sys.exc_info()
        #     LOGGER.debug(f"type = {type}")
        #     LOGGER.debug(f"value = {value}")
        #     raise e
        if response:
            results = []
            for i in response.get('items', []):
                account = {
                    'id': i['id'],
                    'name': i['name'],
                    'properties': [],
                }
                for p in i['webProperties']:
                    prop = {
                        'id': p['id'],
                        'name': p['name']
                    }
                    account['properties'].append(prop)
                results.append(account)
            self.accounts = results
            return results

    class Account(ga4.MegatonGA4.Account):
        def __init__(self, parent):
            super().__init__(parent)

        def _update(self):
            response = self.parent.admin_client.management().webproperties().list(
                accountId=self.id).execute()
            results = []
            for i in response.get('items', []):
                dict = {
                    'id': i['id'],
                    'name': i['name'],
                    'industry': i.get('industryVertical', ''),
                    'service_level': i['level'],
                    'created_time': i['created'],
                    'updated_time': i['updated'],
                    'data_retention': i['dataRetentionTtl'],
                    'data_retention_reset_on_activity': i['dataRetentionResetOnNewActivity'],
                    'properties': '',
                }
                results.append(dict)
            self.properties = results
            return results

        def _clear(self):
            super()._clear()
            self.parent.view.clear()

        @property
        def segments(self):
            """Returns built-in and custom segments for the account."""
            response = self.parent.admin_client.management().segments().list().execute()
            results = []
            for i in response.get('items', []):
                dict = {
                    'id': i['id'],
                    'name': i['name'],
                    'type': i['type'],
                    'definition': i['definition'],
                }
                results.append(dict)
            return results

    class Property(ga4.MegatonGA4.Property):
        def __init__(self, parent):
            super().__init__(parent)

        def clear(self):
            super().clear()
            self.parent.view.clear()

        def _get_metadata(self):
            return {'dimensions': [], 'metrics': []}

        @property
        def custom_dimensions(self):
            """Returns custom dimensions for the property."""
            response = self.parent.admin_client.management().customDimensions().list(
                accountId=self.parent.account.id,
                webPropertyId=self.id
            ).execute()
            results = []
            for i in response.get('items', []):
                dict = {
                    'index': i['index'],
                    'display_name': i['name'],
                    'scope': i['scope'],
                    'active': i['active'],
                }
                results.append(dict)
            return results

        @property
        def custom_metrics(self):
            """Returns custom metrics for the property."""
            response = self.parent.admin_client.management().customMetrics().list(
                accountId=self.parent.account.id,
                webPropertyId=self.parent.property.id
            ).execute()
            results = []
            for i in response.get('items', []):
                dict = {
                    'index': i['index'],
                    'display_name': i['name'],
                    'scope': i['scope'],
                    'type': i['type'],
                    'active': i['active'],
                }
                results.append(dict)
            return results

        def _update(self):
            #self.clear()
            self.get_info()
            # self.get_available()  # Metadata API is not implemented

            # get views
            response = self.parent.admin_client.management().profiles().list(
                accountId=self.parent.account.id,
                webPropertyId=self.parent.property.id
            ).execute()
            results = []
            for i in response.get('items', []):
                dict = {
                    'property_id': i['webPropertyId'],
                    'id': i['id'],
                    'name': i['name'],
                    'currency': i['currency'],
                    'time_zone': i['timezone'],
                    'url': i['websiteUrl'],
                    'type': i['type'],
                    'ecommerce': i['eCommerceTracking'],
                    'bot_filtering': i.get('botFilteringEnabled', False),
                    'site_search_parameters': i.get('siteSearchQueryParameters', ''),
                    'default_page': i.get('defaultPage', ''),
                    'created_time': i['created'],
                    'updated_time': i['updated'],
                }
                results.append(dict)
            self.views = results
            # return results

        def get_info(self):
            """Get property data from parent account"""
            dict = [p for p in self.parent.account.properties if p['id'] == self.id][0]
            self.name = dict['name']
            self.created_time = dict['created_time']
            self.updated_time = dict['updated_time']
            self.industry = dict['industry']
            self.service_level = dict['service_level']
            self.data_retention = dict.get('data_retention', '')
            self.data_retention_reset_on_activity = dict.get('data_retention_reset_on_activity', '')
            return dict

        def get_dimensions(self):
            """Get custom dimension settings"""
            if not self.api_custom_dimensions:
                self.api_custom_dimensions = self.custom_dimensions()
            return self.api_custom_dimensions

        def get_metrics(self):
            """Get custom metrics settings"""
            if not self.api_custom_metrics:
                self.api_custom_metrics = self.custom_metrics()
            return self.api_custom_metrics

        def show(self, me: str = 'info', index_col: Optional[str] = None):
            res = None
            sort_values = []
            if me == 'custom_dimensions':
                index_col = 'index'
                res = self.get_dimensions()
            elif me == 'custom_metrics':
                index_col = 'index'
                res = self.get_metrics()

            if res:
                if index_col:
                    return pd.DataFrame(res).set_index(index_col).sort_values(by=sort_values)
                else:
                    return pd.DataFrame(res).sort_values(by=sort_values)
            return pd.DataFrame()

    class View:
        def __init__(self, parent):
            self.parent = parent
            self.id = None
            self.name = None
            self.currency = None
            self.time_zone = None
            self.url = None
            self.type = None
            self.ecommerce = None
            self.bot_filtering = None
            self.site_search_parameters = None
            self.default_page = None
            self.created_time = None
            self.updated_time = None

        def clear(self):
            self.id = None
            self.name = None
            self.currency = None
            self.time_zone = None
            self.url = None
            self.type = None
            self.ecommerce = None
            self.bot_filtering = None
            self.site_search_parameters = None
            self.default_page = None
            self.created_time = None
            self.updated_time = None

        def select(self, id: str):
            if id:
                if id != self.id:
                    self.id = id
                    self._update()
            else:
                self.clear()

        def _update(self):
            self.get_info()

        def get_info(self):
            """Get view data from parent property"""
            i = [p for p in self.parent.property.views if p['id'] == self.id][0]
            self.name = i['name']
            self.currency = i['currency']
            self.time_zone = i['time_zone']
            self.url = i['url']
            self.type = i['type']
            self.ecommerce = i['ecommerce']
            self.bot_filtering = i['bot_filtering']
            self.site_search_parameters = i['site_search_parameters']
            self.default_page = i['default_page']
            self.created_time = i['created_time']
            self.updated_time = i['updated_time']
            return i

        @property
        def goals(self):
            """Get Goals"""
            response = self.parent.admin_client.management().goals().list(
                accountId=self.parent.account.id,
                webPropertyId=self.parent.property.id,
                profileId=self.id
            ).execute()
            results = []
            for i in response.get('items', []):
                dict = {
                    'id': i.get('id'),
                    'name': i.get('name'),
                    'value': i.get('value'),
                    'type': i.get('type'),
                    'url_destination': i.get('urlDestinationDetails'),
                    'time_on_site': i.get('visitTimeOnSiteDetails'),
                    'pages_per_session': i.get('visitNumPagesDetails'),
                    'event': i.get('eventDetails'),
                    'created_time': i.get('created'),
                    'updated_time': i.get('updated'),
                    'active': i.get('active'),
                }
                results.append(dict)
            return results

        def show(self, me: str = 'info', index_col: Optional[str] = None):
            res = None
            sort_values = []
            if me == 'info':
                res = [self.get_info()]
                index_col = 'id'
            elif me == 'goals':
                res = self.goals
                index_col = 'id'

            if res:
                if index_col:
                    return pd.DataFrame(res).set_index(index_col).sort_values(by=sort_values)
                else:
                    return pd.DataFrame(res).sort_values(by=sort_values)
            return pd.DataFrame()

    class Report(ga4.MegatonGA4.Report):

        def _format_name(self, name: str, type: str = None):
            """Convert api_name to an object for request and OrderBy"""
            after = ("ga:" + name) if not name.startswith("ga:") else name
            if type == 'dimension':
                # Dimension object in ReportRequest
                return {
                    'name': after,
                }
            elif type == 'metric':
                # Metric object in ReportRequest
                return {
                    'expression': after,
                    'alias': name,
                }
            else:
                # simple api_name for OrderBy in ReportRequest, etc
                return after

        def _parse_operator(self, operator: str, type: str):
            """Convert a string to legacy filter operator from Core Reporting API v3"""
            is_dimension = True if type.startswith('d') else False

            if is_dimension:
                if operator.endswith('='):
                    return 'EXACT'
                elif operator.endswith('~'):
                    return 'REGEXP'
                return 'PARTIAL'
            else:  # is metric
                if operator.endswith('='):
                    return 'EQUAL'
                elif operator == '>':
                    return 'GREATER_THAN'
                elif operator == '<':
                    return 'LESS_THAN'

        def _parse_filter_condition(self, condition: str, **kwargs):
            """Split a string into name, operator and value
            Operator:
                == : Equals
                != : Does not equal
                =@ : Contains substring
                !@ : Does not contain substring
                =~ : Contains a match for the regular expression
                !~ : Does not match regular expression
                > : Greater than
                < : Less than
            """
            m = re.search(r'^(.+)(==|!=|=@|!@|=~|!~|>|<)(.+)$', condition)
            if m:
                field = self._format_name(m.groups()[0])
                type = kwargs.get('type')
                value = m.groups()[2]
                op = m.groups()[1]
                is_not = True if op.startswith('!') else False
                operator = self._parse_operator(op, type)

                if type == 'dimensions':
                    return {
                        'dimensionName': field,
                        'not': is_not,
                        'operator': operator,
                        'expressions': [value],
                        'caseSensitive': False
                    }
                elif type == 'metrics':
                    return {
                        'metricName': field,
                        'not': is_not,
                        'operator': operator,
                        'comparisonValue': value
                    }

        def _format_filter(self, conditions: str, type='dimensions'):
            """Convert legacy filters format from Core Reporting API v3 to DimensionFilterClause or MetricFilterClause object"""
            if not conditions:
                return []

            filters = [self._parse_filter_condition(i, type=type) for i in conditions.split(';')]
            return [{
                'operator': 'AND',
                'filters': filters
            }]

        def _format_order_bys(self, before: str):
            """Convert legacy sort format from Core Reporting API v3 to a list of OrderBy object"""
            if not before:
                return

            result = []
            for i in before.split(','):
                obj = {}
                try:
                    _, field = i.split('-')
                except ValueError:
                    # Ascending
                    obj['fieldName'] = self._format_name(i)
                    obj['sortOrder'] = 'ASCENDING'
                else:
                    # Descending
                    obj['fieldName'] = self._format_name(field)
                    obj['sortOrder'] = 'DESCENDING'
                result.append(obj)
            return result

        def _format_request(self, **kwargs):
            """Construct a request for API"""
            dimension_api_names = [self._format_name(r, type='dimension') for r in kwargs.get('dimensions')]
            metrics_api_names = [self._format_name(r, type='metric') for r in kwargs.get('metrics')]

            return {
                'viewId': self.parent.view.id,
                'dateRanges': [{
                    'startDate': kwargs.get('start_date'),
                    'endDate': kwargs.get('end_date')
                }],
                'samplingLevel': 'LARGE',
                'dimensions': dimension_api_names,
                'dimensionFilterClauses': self._format_filter(kwargs.get('dimension_filter'), type='dimensions'),
                'metrics': metrics_api_names,
                'metricFilterClauses': self._format_filter(kwargs.get('metric_filter'), type='metrics'),
                'orderBys': self._format_order_bys(kwargs.get('order_bys')),
                'segments': kwargs.get('segments'),
                'includeEmptyRows': False,
                'hideTotals': not kwargs.get('show_total', False),
                'hideValueRanges': True,
                'pageSize': kwargs.get('limit'),
            }

        def _parse_response(self, report: dict):
            all_data = []
            names = []
            dimension_types = []
            metric_types = []

            for i in report['columnHeader']['dimensions']:
                names.append(i.replace('ga:', ''))
                dimension_types.append('category')

            for i in report['columnHeader']['metricHeader']['metricHeaderEntries']:
                names.append(i['name'])
                metric_types.append(i['type'])

            for row in report['data'].get('rows', []):
                row_data = []
                for d in row['dimensions']:
                    row_data.append(d)
                for d in row['metrics']:
                    for i, v in enumerate(d['values']):
                        row_data.append(self._convert_metric(v, metric_types[i]))
                all_data.append(row_data)

            return all_data, names, dimension_types + metric_types

        def _request_report_api(self, offset: str, request: dict):
            if offset:
                request["pageToken"] = offset

            try:
                response = self.parent.data_client.reports().batchGet(
                    body={
                        'reportRequests': [request],
                        'useResourceQuotas': False  # only for 360
                    }
                ).execute()
            except err.HttpError as e:
                data = json.loads(e.content.decode('utf-8'))
                code = data['error']["code"]
                # message = data['error']['message']
                status = data['error']['status']
                if code == 400 and status == 'INVALID_ARGUMENT':
                    raise errors.BadRequest(status)
            except Exceptions as e:
                raise e

            report_data = response.get('reports', [])[0]
            total_rows = report_data['data'].get('rowCount', 0)

            samples_count = report_data['data'].get('samplesReadCounts')
            samples_size = report_data['data'].get('samplingSpaceSizes')
            next_token = report_data.get('nextPageToken', None)
            if samples_count:
                LOGGER.warn(f"samplesReadCounts = {samples_count}")
            if samples_size:
                LOGGER.warn(f"samplingSpaceSizes = {samples_size}")

            data, headers, types = self._parse_response(report_data)

            return data, total_rows, headers, types, next_token

        def _report_generator(self, request: dict):
            """Send request to get report data"""
            if not self.parent.view.id:
                # LOGGER.error("Viewを先に選択してから実行してください。")
                return

            token = "0"
            while True:
                try:
                    (data, total_rows, headers, types, next_token) = self._request_report_api(token, request)
                except err.HttpError as e:
                    value = str(sys.exc_info()[1])
                    if 'disabled' in value:
                        LOGGER.error("\nGCPのプロジェクトでAnalytics Reporting APIを有効化してください。")
                        raise errors.ApiDisabled
                    elif 'Invalid value' in value:
                        LOGGER.error("レポート抽出条件の書式や内容を見直してください。")
                        raise errors.BadRequest
                    else:
                        raise e

                if token == "0" and total_rows:
                    LOGGER.info(f"Total {total_rows} rows found.")

                    if total_rows == 10001:
                        if [d for d in request['dimensions'] if d['name'] == 'ga:clientId']:
                            LOGGER.info("clientId is not officially supported by Google. Using this dimension in an "
                                        "Analytics report may thus result in unexpected & unexplainable behavior ("
                                        "such as restricting the report to exactly 10,000 or 10,001 rows).")
                    self.headers = headers
                    self.types = types

                if len(data):
                    LOGGER.info(f"(received row #{token}-{int(token) + len(data) - 1})")

                for row in data:
                    yield row

                if not next_token:
                    # no more data
                    break
                if len(data) == 0:
                    # API bug
                    raise errors.PartialDataReturned()

                token = next_token

        def show(self, dimensions: list, metrics: list, return_generator: Optional[bool] = None, **kwargs):
            """Get Analytics report data"""
            if not self.parent.view.id:
                LOGGER.error("Viewを先に選択してから実行してください。")
                return

            if len(dimensions) > 7:
                LOGGER.warn("Up to 7 dimensions are allowed.")
                dimensions = dimensions[:7]
            if len(metrics) > 10:
                LOGGER.warn("Up to 10 dimensions are allowed.")
                metrics = metrics[:10]

            start_date = kwargs.get('start_date', self.start_date)
            end_date = kwargs.get('end_date', self.end_date)
            LOGGER.info(f"Requesting a report ({start_date} - {end_date})")

            request = self._format_request(
                dimensions=dimensions,
                metrics=metrics,
                start_date=start_date,
                end_date=end_date,
                dimension_filter=kwargs.get('dimension_filter'),
                metric_filter=kwargs.get('metric_filter'),
                order_bys=kwargs.get('order_bys'),
                segments=kwargs.get('segments'),
                show_total=False,
                limit=kwargs.get('limit', 10000),
            )
            # print(request)

            iterator = self._report_generator(request)

            if return_generator:
                return iterator

            try:
                return pd.DataFrame(list(iterator), columns=self.headers)
            except AttributeError:
                LOGGER.info("No data found.")
                return pd.DataFrame()
            except errors.PartialDataReturned:
                LOGGER.warn("APIのバグにより全データを取得できませんでした。")
                if start_date != end_date:
                    LOGGER.warn("1日毎にデータを分割して取得します。")
                    all_data = []
                    for date in utils.get_date_range(start_date, end_date):
                        print(f"...{date}", end='')
                        request['dateRanges'][0]['startDate'] = date
                        request['dateRanges'][0]['endDate'] = date
                        request['pageToken'] = "0"
                        iterator = self._report_generator(request)
                        data = list(iterator)
                        LOGGER.debug(f"{len(data)} rows")
                        all_data.extend(data)
                    return pd.DataFrame(all_data, columns=self.headers)


def get_cid_date_page(ga3, conf: dict):
    """データ抽出
        Dimensions:
            対象ページを閲覧したclient_id,セッションカウント, 日付
        Metrics:
            入口, PV, 出口
    """
    filter = []
    if conf['include_domains']:
        filter.append(f"hostname=~{conf['include_domains']}")
    if conf['include_pages']:
        filter.append(f"pagePath=~{conf['include_pages']}")
    if conf['exclude_pages']:
        filter.append(f"pagePath!~{conf['exclude_pages']}")
    dimension_filter = ";".join(filter)

    df = ga3.report.show(
        dimensions=[
            'clientId',
            'sessionCount',
            'date',
            'pagePath',
        ],
        dimension_filter=dimension_filter,
        metrics=[
            'entrances',
            'uniquePageviews',
            'exits',
        ],
        order_bys='clientId,sessionCount,date,pagePath',
    )

    if len(df):
        # 値を変換
        if conf['page_regex']:
            utils.format_df(df, [('pagePath', conf['page_regex'], '')])
        # カラム名を変更
        return df.rename(columns={
            'pagePath': 'page',
            'uniquePageviews': 'sessions',
        })
    else:
        raise errors.NoDataReturned


def get_last_returned_date(ga3):
    """データ抽出：再訪問した人の最終訪問日
        Dimensions:
            再訪問時のclient_id,日付
        Metrics:
            入口
    """
    df = ga3.report.show(
        dimensions=[
            'clientId',
            'date',
        ],
        dimension_filter='ga:sessionCount!=1',
        metrics=[
            'entrances',  # 不要
        ],
        order_bys='ga:clientId,ga:date',
    )

    # cid単位でまとめて最後に訪問した日を算出
    _df = df.groupby(['clientId']).max().rename(columns={'date': 'last_visit_date'})

    return _df.drop(['entrances'], inplace=False, axis=1)


def get_no_entrance_cv_cid(ga3, include_pages=None, metric_filter: str = 'ga:entrances<1'):
    """元データ抽出：入口以外でCVページに到達したcid
    """
    filter = []
    if include_pages:
        filter.append(f'pagePath=~{include_pages}')
    dimension_filter = ";".join(filter)

    _df = ga3.report.show(
        dimensions=[
            'pagePath',  # 不要（確認のため）
            'clientId',
            'date',
            'sessionCount',
        ],
        dimension_filter=dimension_filter,
        metrics=[
            'users',  # 不要
        ],
        metric_filter=metric_filter,
        order_bys='pagePath,clientId,date',
    )
    if len(_df):
        return _df.drop(['users'], axis=1)
    else:
        return pd.DataFrame()


def get_page_title(ga3, conf: dict):
    """ 対象・除外・変換条件を指定してpageとtitleを抽出 """
    filter = []
    if conf['include_domains']:
        filter.append(f"hostname=~{conf['include_domains']}")
    if conf['include_pages']:
        filter.append(f"pagePath=~{conf['include_pages']}")
    if conf['exclude_pages']:
        filter.append(f"pagePath!~{conf['exclude_pages']}")
    dimension_filter = ";".join(filter)

    df = ga3.report.show(
        dimensions=[
            'pagePath',
            'pageTitle',
        ],
        dimension_filter=dimension_filter,
        metrics=[
            'uniquePageviews',
        ],
        order='-uniquePageviews',
    )

    df.rename(columns={
        'pagePath': 'page',
        'pageTitle': 'title',
    }, inplace=True)

    # 値を変換
    if conf['page_regex']:
        utils.format_df(df, [('pagePath', conf['page_regex'], '')])
    if conf['title_regex']:
        utils.format_df(df, [('title', conf['title_regex'], '')])

    # group byでまとめる
    try:
        return df.sort_values(
            ['page', 'uniquePageviews'],
            ascending=False
        ).groupby('page').first().reset_index()[['page', 'title']]
    except KeyError:
        raise errors.NoDataReturned
