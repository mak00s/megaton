"""
Functions for Google Analytics 4 API
"""

import logging
import re
import sys
from collections import OrderedDict
from datetime import datetime
from typing import Optional

import pandas as pd
import pytz
from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.admin_v1alpha.types import CustomDimension
from google.analytics.admin_v1alpha.types import CustomMetric
from google.analytics.admin_v1alpha.types import DataRetentionSettings
from google.analytics.admin_v1alpha.types import IndustryCategory
from google.analytics.admin_v1alpha.types import ServiceLevel
from google.analytics.data import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Filter
from google.analytics.data_v1beta.types import FilterExpression
from google.analytics.data_v1beta.types import FilterExpressionList
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import MetricAggregation
from google.analytics.data_v1beta.types import MetricType
from google.analytics.data_v1beta.types import NumericValue
from google.analytics.data_v1beta.types import OrderBy
from google.analytics.data_v1beta.types import RunReportRequest
from google.api_core.exceptions import PermissionDenied
from google.api_core.exceptions import ServiceUnavailable
from google.api_core.exceptions import Unauthenticated
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials

from . import errors, utils

LOGGER = logging.getLogger(__name__)


class MegatonGA4(object):
    this = "Megaton GA4"
    required_scopes = [
        'https://www.googleapis.com/auth/analytics.edit',
        'https://www.googleapis.com/auth/analytics.readonly',
    ]

    def __init__(self, credentials: Credentials, **kwargs):
        """constructor"""
        self.credentials = credentials
        self.credential_cache_file = kwargs.get('credential_cache_file')
        self.data_client = None
        self.admin_client = None
        self.accounts = None
        self.account = self.Account(self)
        self.property = self.Property(self)
        self.report = self.Report(self)
        if credentials:
            self.authorize()

    def _get_account_id_from_account_path(self, path: str):
        dict = self.admin_client.parse_account_path(path)
        return dict.get('account')

    def _get_property_id_from_property_path(self, path: str):
        dict = self.admin_client.parse_property_path(path)
        return dict.get('property')

    def _build_client(self):
        self.data_client = BetaAnalyticsDataClient(credentials=self.credentials)
        self.admin_client = AnalyticsAdminServiceClient(credentials=self.credentials)

    def _update(self):
        """Returns account summaries accessible by the caller."""
        try:
            results_iterator = self.admin_client.list_account_summaries()
        except PermissionDenied as e:
            message = getattr(e, 'message', repr(e))
            m = re.search(r'reason: "([^"]+)', str(sys.exc_info()[1]))
            if m:
                reason = m.group(1)
                if reason == 'SERVICE_DISABLED':
                    raise errors.ApiDisabled(message, 'Google Analytics Admin API')
            LOGGER.error(f"APIを使う権限がありません。{message}")
        except ServiceUnavailable as e:
            value = str(sys.exc_info()[1])
            m = re.search(r"error: \('([^:']+): ([^']+)", value)
            if m and m.group(1) == 'invalid_grant':
                LOGGER.error(f"認証の期限が切れています。{m.group(2)}")
                self.credentials = None
            raise e
        except Unauthenticated:
            LOGGER.error("認証に失敗しました。")
            self.credentials = None
            LOGGER.warning(sys.exc_info()[1])
            raise
        except AttributeError as e:
            try:
                reason = e.__context__.code().value[1]
                message = e.__context__.details()
                if reason == 'permission denied':
                    raise errors.ApiDisabled(message, "Google Analytics Data API")
                else:
                    LOGGER.error(message)
                    raise
            except:  # noqa
                raise
        # except Exception as e:
        #     type_, value, _ = sys.exc_info()
        #     LOGGER.error(type_)
        #     LOGGER.error(value)
        #     # print(f"type={type}, value={value}")
        #     raise
        else:
            results = []
            for i in results_iterator:
                dict1 = {
                    'id': self._get_account_id_from_account_path(i.account),
                    'name': i.display_name,
                    'properties': [],
                }
                for p in i.property_summaries:
                    dict2 = {
                        'id': self._get_property_id_from_property_path(p.property),
                        'name': p.display_name
                    }
                    dict1['properties'].append(dict2)
                results.append(dict1)
            self.accounts = results
            return results

    # retry(stop=stop_after_attempt(1), retry=retry_if_exception_type(ServiceUnavailable))
    def authorize(self):
        if not isinstance(self.credentials, (Credentials, service_account.Credentials)):
            self.credentials = None
            raise errors.BadCredentialFormat

        self._build_client()

        if bool(set(self.credentials.scopes) & set(self.required_scopes)):
            if not self._update():
                return
            LOGGER.info(f"{self.this} launched!")
            return True
        else:
            raise errors.BadCredentialScope(self.required_scopes)

    class Account(object):
        def __init__(self, parent):
            self.parent = parent
            self.id = None
            self.properties = None

        def _clear(self):
            self.id = None
            self.properties = None
            self.parent.property.clear()

        def _update(self):
            """Update summaries of all properties for the account"""
            try:
                results_iterator = self.parent.admin_client.list_properties({
                    'filter': f"parent:accounts/{self.id}",
                    'show_deleted': False,
                })
            except ServiceUnavailable as e:
                # str(sys.exc_info()[1])
                type_, value, _ = sys.exc_info()
                LOGGER.debug(type_)
                LOGGER.debug(value)
                raise e
            # except Exception as e:
            #     print(e)
            #     raise e
            else:
                results = []
                for i in results_iterator:
                    dict = {
                        'id': self.parent._get_property_id_from_property_path(i.name),
                        'name': i.display_name,
                        'time_zone': i.time_zone,
                        'currency': i.currency_code,
                        'industry': IndustryCategory(i.industry_category).name,
                        'service_level': ServiceLevel(i.service_level).name,
                        'created_time': convert_proto_datetime(i.create_time),
                        'updated_time': convert_proto_datetime(i.update_time),
                    }
                    results.append(dict)
                self.properties = results
                return results

        def select(self, id: str):
            if id:
                if id != self.id:
                    self.id = id
                    self._update()
            else:
                self._clear()

        def show(self, index_col: str = 'id'):
            res = self.properties
            if res:
                df = pd.DataFrame(res)
                if index_col:
                    return df.set_index(index_col)

    class Property(object):
        def __init__(self, parent):
            self.parent = parent
            self.id = None
            self.name = None
            self.created_time = None
            self.updated_time = None
            self.time_zone = None
            self.currency = None
            self.industry = None
            self.service_level = None
            self.data_retention = None
            self.data_retention_reset_on_activity = None
            self.api_custom_dimensions = None
            self.api_custom_metrics = None
            self.api_metadata = None
            self.dimensions = None
            self.metrics = None

        def clear(self):
            self.id = None
            self.name = None
            self.created_time = None
            self.updated_time = None
            self.time_zone = None
            self.currency = None
            self.industry = None
            self.service_level = None
            self.data_retention = None
            self.data_retention_reset_on_activity = None
            self.api_custom_dimensions = None
            self.api_custom_metrics = None
            self.api_metadata = None
            self.dimensions = None
            self.metrics = None

        def _get_metadata(self):
            """Returns available dimensions and metrics for the property."""
            path = self.parent.data_client.metadata_path(self.id)
            try:
                response = self.parent.data_client.get_metadata(name=path)
            except PermissionDenied as e:
                message = getattr(e, 'message', repr(e))
                m = re.search(r'reason: "([^"]+)', str(sys.exc_info()[1]))
                if m:
                    reason = m.group(1)
                    if reason == 'SERVICE_DISABLED':
                        raise errors.ApiDisabled(message, "Google Analytics Data API")
                LOGGER.error(f"APIを使う権限がありません。{message}")
            except AttributeError as e:
                try:
                    reason = e.__context__.code().value[1]
                    message = e.__context__.details()
                    if reason == 'permission denied':
                        raise errors.ApiDisabled(message, "Google Analytics Data API")
                    else:
                        # LOGGER.error(message)
                        raise
                except:  # noqa
                    raise
            else:
                dimensions = []
                for i in response.dimensions:
                    dimensions.append({
                        'customized': i.custom_definition,
                        'category': i.category,
                        'api_name': i.api_name,
                        'display_name': i.ui_name,
                        'description': i.description,
                        # 'deprecated_api_names': i.deprecated_api_names,
                    })
                metrics = []
                for i in response.metrics:
                    metrics.append({
                        'customized': i.custom_definition,
                        'category': i.category,
                        'api_name': i.api_name,
                        'display_name': i.ui_name,
                        'description': i.description,
                        # 'deprecated_api_names': i.deprecated_api_names,
                        'type': i.type_,
                        'expression': i.expression,
                    })
                return {'dimensions': dimensions, 'metrics': metrics}

        @property
        def custom_dimensions(self):
            """Returns custom dimensions for the property."""
            try:
                results_iterator = self.parent.admin_client.list_custom_dimensions(
                    parent=f"properties/{self.id}")
            except Exception as e:
                LOGGER.error(e)
            else:
                results = []
                for i in results_iterator:
                    dict = {
                        'parameter_name': i.parameter_name,
                        'display_name': i.display_name,
                        'description': i.description,
                        'scope': CustomDimension.DimensionScope(i.scope).name,
                        # 'disallow_ads_personalization': item.disallow_ads_personalization,
                    }
                    results.append(dict)
                return results

        @property
        def custom_metrics(self):
            """Returns custom metrics for the property."""
            try:
                results_iterator = self.parent.admin_client.list_custom_metrics(
                    parent=f"properties/{self.id}")
            except Exception as e:
                LOGGER.error(e)
            else:
                results = []
                for i in results_iterator:
                    dict = {
                        'parameter_name': i.parameter_name,
                        'display_name': i.display_name,
                        'description': i.description,
                        'scope': CustomDimension.DimensionScope(i.scope).name,
                        'measurement_unit': CustomMetric.MeasurementUnit(i.measurement_unit).name,
                        'restricted_metric_type': [CustomMetric.RestrictedMetricType(d).name for d in
                                                   i.restricted_metric_type],
                    }
                    results.append(dict)
                return results

        def _get_data_retention(self):
            """Returns data retention settings for the property."""
            try:
                item = self.parent.admin_client.get_data_retention_settings(
                    name=f"properties/{self.id}/dataRetentionSettings")
            except Exception as e:
                LOGGER.error(e)
            else:
                dict = {
                    'data_retention': DataRetentionSettings.RetentionDuration(item.event_data_retention).name,
                    'reset_user_data_on_new_activity': item.reset_user_data_on_new_activity,
                }
                return dict

        def _update(self):
            # self.clear()
            self.get_info()
            self.get_available()

        def select(self, id: str):
            if id:
                if id != self.id:
                    self.id = id
                    self._update()
            else:
                self.clear()

        def get_info(self):
            """Get property data from parent account"""
            dict = [p for p in self.parent.account.properties if p['id'] == self.id][0]
            self.name = dict['name']
            self.created_time = dict['created_time']
            self.updated_time = dict['updated_time']
            self.industry = dict['industry']
            self.service_level = dict['service_level']
            dict2 = self._get_data_retention()
            dict['data_retention'] = dict2['data_retention']
            dict['data_retention_reset_on_activity'] = dict2['reset_user_data_on_new_activity']
            self.time_zone = dict.get('time_zone', None)  # GA4 only
            self.currency = dict.get('currency', None)  # GA4 only
            return dict

        def get_available(self):
            if not self.api_metadata:
                self.api_metadata = self._get_metadata()
            return self.api_metadata

        def get_dimensions(self):
            self.get_available()
            if not self.api_custom_dimensions:
                self.api_custom_dimensions = self.custom_dimensions
            # integrate data
            new = []
            for m in self.api_metadata['dimensions']:
                dict = m.copy()
                if m['customized']:
                    for c in self.api_custom_dimensions:
                        if m['display_name'] == c['display_name'] or m['display_name'] == c['parameter_name']:
                            dict['description'] = c['description']
                            dict['parameter_name'] = c['parameter_name']
                            dict['scope'] = c['scope']
                new.append(dict)
            self.dimensions = new
            return self.dimensions

        def get_metrics(self):
            self.get_available()
            if not self.api_custom_metrics:
                self.api_custom_metrics = self.custom_metrics
            # integrate data
            new = []
            for m in self.api_metadata['metrics']:
                dict = m.copy()
                if m['customized']:
                    for c in self.api_custom_metrics or {}:
                        if m['display_name'] == c['display_name']:
                            dict['description'] = c['description']
                            dict['parameter_name'] = c['parameter_name']
                            dict['scope'] = c['scope']
                            dict['unit'] = c['measurement_unit']
                if 'type' in m.keys():
                    dict['type'] = MetricType(m['type']).name
                new.append(dict)
            self.metrics = new
            return self.metrics

        def show(self, me: str = 'info', index_col: Optional[str] = None):
            res = None
            sort_values = []
            if me == 'metrics':
                list_of_dict = self.get_metrics()
                my_order = ["category", "display_name", "description", "api_name", "parameter_name", "scope", "unit",
                            "expression"]
                res = []
                for d in list_of_dict:
                    res.append(OrderedDict((k, d[k]) for k in my_order if k in d.keys()))
                sort_values = ['category', 'display_name']
            elif me == 'dimensions':
                list_of_dict = self.get_dimensions()
                my_order = ["category", "display_name", "description", "api_name", "parameter_name", "scope"]
                res = []
                for d in list_of_dict:
                    res.append(OrderedDict((k, d[k]) for k in my_order if k in d.keys()))
                sort_values = ['category', 'display_name']
            elif me == 'custom_dimensions':
                index_col = 'api_name'
                dict = self.get_dimensions()
                res = []
                for r in dict:
                    if r['customized']:
                        res.append({
                            'display_name': r['display_name'],
                            'api_name': r['api_name'],
                            'parameter_name': r['parameter_name'],
                            'description': r['description'],
                            'scope': r['scope'],
                        })
            elif me == 'custom_metrics':
                index_col = 'api_name'
                dict = self.get_metrics()
                res = []
                for r in dict:
                    if r['customized']:
                        res.append({
                            'display_name': r['display_name'],
                            'api_name': r['api_name'],
                            'description': r['description'],
                            'type': r['type'] if 'type' in r else '',
                            'scope': r['scope'] if 'scope' in r else '',
                            'parameter_name': r['parameter_name'] if 'parameter_name' in r else '',
                            'unit': r['unit'] if 'unit' in r else '',
                            'expression': r['expression'],
                        })
            elif me == 'info':
                res = [self.get_info()]
                index_col = 'id'

            if res:
                if index_col:
                    return pd.DataFrame(res).set_index(index_col).sort_values(by=sort_values)
                else:
                    return pd.DataFrame(res).sort_values(by=sort_values)
            return pd.DataFrame()

        def create_custom_dimension(self, parameter_name: str, display_name: str, description: str,
                                    scope: str = 'EVENT'):
            """Create custom dimension for the property."""
            try:
                created_cd = self.parent.admin_client.create_custom_dimension(
                    parent=f"properties/{self.id}",
                    custom_dimension={
                        'parameter_name': parameter_name,
                        'display_name': display_name,
                        'description': description,
                        'scope': CustomDimension.DimensionScope[scope].value,
                    }
                )
                return created_cd
            except Exception as e:
                LOGGER.error(e)

    class Report(object):
        def __init__(self, parent):
            self.parent = parent
            self.start_date = '7daysAgo'
            self.end_date = 'yesterday'
            self.segment = None

        def set_dates(self, start_date: str, end_date: str):
            self.start_date = start_date.strip()
            self.end_date = end_date.strip()

        def _format_name(self, name: str):
            """Convert api_name or display_name of valid dimensions or metrics to an api_name
            Args:
                before [str] api_name or display_name
            Returns:
                api_name [str]
                field_type: 'dimension' or 'metric'
            """
            what = 'api_name'  # field name to return
            for type in ['dimensions', 'metrics']:
                for i in self.parent.property.api_metadata[type]:
                    if i['display_name'] == name.strip():
                        return i[what], type
                    elif i['api_name'] == name.strip():
                        return i[what], type
            raise errors.BadRequest(f"{name} is not a dimension or a metric.")

        def _parse_operator(self, operator: str, type: str):
            """Convert legacy filters format from Core Reporting API v3 to Filter object"""
            is_dimension = True if type.startswith('d') else False

            if is_dimension:
                if operator.endswith('='):
                    return Filter.StringFilter.MatchType.EXACT
                elif operator.endswith('~'):
                    return Filter.StringFilter.MatchType.PARTIAL_REGEXP
                else:
                    return Filter.StringFilter.MatchType.CONTAINS
            else:  # is metric
                if operator.endswith('='):
                    return Filter.NumericFilter.Operation.EQUAL
                elif operator == '>':
                    return Filter.NumericFilter.Operation.GREATER_THAN
                elif operator == '>=':
                    return Filter.NumericFilter.Operation.GREATER_THAN_OR_EQUAL
                elif operator == '<':
                    return Filter.NumericFilter.Operation.LESS_THAN
                elif operator == '<=':
                    return Filter.NumericFilter.Operation.LESS_THAN_OR_EQUAL
                return Filter.NumericFilter.Operation.OPERATION_UNSPECIFIED

        def _parse_filter_condition(self, condition: str):
            """Convert a single legacy filter format from Core Reporting API v3 to FilterExpression object"""
            m = re.search(r'^([\w_\+\/\(\) ]+)(==|!=|=@|!@|=~|!~|>|>=|<|<=)(.+)$', condition)
            if not m:
                raise errors.BadRequest(f"Invalid Filter: '{condition}'")
            field, type = self._format_name(m.groups()[0])
            value = m.groups()[2]
            op = m.groups()[1]
            is_not = True if op.startswith('!') else False
            operator = self._parse_operator(op, type)

            if type == 'dimensions':
                filter = Filter(
                    field_name=field,
                    string_filter=Filter.StringFilter(
                        match_type=operator,
                        value=value,
                    )
                )
            elif type == 'metrics':
                if utils.is_integer(value):
                    value_class = NumericValue(int64_value=int(float(value)))
                else:
                    value_class = NumericValue(double_value=float(value))
                filter = Filter(
                    field_name=field,
                    numeric_filter=Filter.NumericFilter(
                        operation=operator,
                        value=value_class,
                    )
                )
            if is_not:
                return FilterExpression(
                    not_expression=FilterExpression(
                        filter=filter
                    )
                )
            else:
                return FilterExpression(filter=filter)

        def _format_filter(self, conditions):
            """Convert legacy filters format from Core Reporting API v3 to Filter object"""
            if not conditions:
                return

            expressions = [self._parse_filter_condition(i) for i in conditions.split(';')]
            if len(expressions) == 1:
                return expressions[0]
            else:
                return FilterExpression(
                    and_group=FilterExpressionList(
                        expressions=expressions
                    )
                )

        def _convert_metric(self, value, type: str):
            """Metric's Value types for GA4 are
                    METRIC_TYPE_UNSPECIFIED = 0
                    TYPE_CURRENCY = 9
                    TYPE_FEET = 10
                    TYPE_FLOAT = 2
                    TYPE_HOURS = 7
                    TYPE_INTEGER = 1
                    TYPE_KILOMETERS = 13
                    TYPE_METERS = 12
                    TYPE_MILES = 11
                    TYPE_MILLISECONDS = 5
                    TYPE_MINUTES = 6
                    TYPE_SECONDS = 4
                    TYPE_STANDARD = 8
                Metric's Value types for UA are
                    METRIC_TYPE_UNSPECIFIED
                    INTEGER
                    FLOAT
                    CURRENCY
                    PERCENT
                    TIME (in HH:MM:SS format)
            """
            type = type.replace('TYPE_', '')
            if type in ['INTEGER', 'HOURS', 'MINUTES', 'SECONDS', 'MILLISECONDS']:
                try:
                    return int(value)
                except:
                    return value
            elif type in ['FLOAT']:
                return float(value)
            else:
                return value

        def _format_order_bys(self, before: str):
            """Convert legacy sort format from Core Reporting API v3 to a list of OrderBy object"""
            if not before:
                return

            result = []
            for i in before.split(','):
                try:
                    _, field = i.split('-')
                except ValueError:
                    # Ascending
                    field = i
                    desc = False
                else:
                    # Descending
                    desc = True
                if self._format_name(field):
                    # DIMENSION
                    result.append(
                        OrderBy(
                            desc=desc,
                            dimension=OrderBy.DimensionOrderBy(
                                dimension_name=field
                            )
                        )
                    )
                elif self._format_name(field):
                    # METRIC
                    result.append(
                        OrderBy(
                            desc=desc,
                            metric=OrderBy.MetricOrderBy(
                                metric_name=field
                            )
                        )
                    )
                else:
                    LOGGER.warn(f"ignoring unknown field '{field}'.")
            return result

        def _format_request(self, **kwargs):
            """Construct a request for API"""
            dimension_api_names = [self._format_name(r)[0] for r in kwargs.get('dimensions')]
            metrics_api_names = [self._format_name(r)[0] for r in kwargs.get('metrics')]

            metric_aggregations = []
            if kwargs.get('show_total', False):
                metric_aggregations = [
                    MetricAggregation.TOTAL,
                    MetricAggregation.MAXIMUM,
                    MetricAggregation.MINIMUM,
                ]

            return RunReportRequest(
                property=f"properties/{self.parent.property.id}",
                date_ranges=[DateRange(
                    start_date=kwargs.get('start_date'),
                    end_date=kwargs.get('end_date')
                )],
                dimensions=[Dimension(name=d) for d in dimension_api_names],
                dimension_filter=self._format_filter(kwargs.get('dimension_filter')),
                metrics=[Metric(name=m) for m in metrics_api_names],
                metric_filter=self._format_filter(kwargs.get('metric_filter')),
                order_bys=self._format_order_bys(kwargs.get('order_bys')),
                metric_aggregations=metric_aggregations,
                keep_empty_rows=False,
                return_property_quota=False,
                limit=kwargs.get('limit'),
            )

        def _parse_response(self, response: dict):
            if not response:
                return [], [], []

            all_data = []
            names = []
            dimension_types = []
            metric_types = []

            for i in response.dimension_headers:
                names.append(i.name)
                dimension_types.append('category')

            for i in response.metric_headers:
                names.append(i.name)
                metric_types.append(MetricType(i.type_).name)

            for row in response.rows:
                row_data = []
                for d in row.dimension_values:
                    row_data.append(d.value)
                for i, v in enumerate(row.metric_values):
                    row_data.append(
                        self._convert_metric(
                            v.value,
                            metric_types[i]
                        )
                    )
                all_data.append(row_data)

            return all_data, names, dimension_types + metric_types

        def _request_report_api(self, offset: int, request: dict):
            if offset:
                request.offset = offset

            total_rows, response = 0, None
            try:
                response = self.parent.data_client.run_report(request)
                total_rows = response.row_count
            except PermissionDenied as e:
                LOGGER.error("権限がありません。")
                message = getattr(e, 'message', repr(e))
                ex_value = sys.exc_info()[1]
                m = re.search(r'reason: "([^"]+)', str(ex_value))
                if m:
                    reason = m.group(1)
                    if reason == 'SERVICE_DISABLED':
                        LOGGER.error("GCPのプロジェクトでData APIを有効化してください。")
                LOGGER.warn(message)
            except Exception as e:
                type_, value, traceback_ = sys.exc_info()
                LOGGER.debug(type_)
                LOGGER.debug(value)

            data, headers, types = self._parse_response(response)

            return data, total_rows, headers, types

        def run(self, dimensions: list, metrics: list, to_pd: bool = True, **kwargs):
            """Get Analytics report data"""
            if not self.parent.property.id:
                LOGGER.error("Propertyを先に選択してから実行してください。")
                return

            if len(dimensions) > 9:
                LOGGER.warn("Up to 9 dimensions are allowed.")
                dimensions = dimensions[:9]
            if len(metrics) > 10:
                LOGGER.warn("Up to 10 dimensions are allowed.")
                metrics = metrics[:10]

            limit = kwargs.get('limit', 10000)
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
                show_total=False,
                limit=limit,
            )
            # print(request)

            all_rows, offset, page = [], 0, 1
            while True:
                (data, total_rows, headers, types) = self._request_report_api(offset, request)
                if len(data) > 0:
                    all_rows.extend(data)
                    if offset == 0:
                        LOGGER.info(f"Total {total_rows} rows found.")
                    LOGGER.info(f" p{page}: retrieved #{offset + 1} - {offset + len(data)}")
                    if offset + len(data) == total_rows:
                        break
                    else:
                        page += 1
                        offset += limit
                else:
                    break

            if len(all_rows) > 0:
                LOGGER.info(f"All {len(all_rows)} rows were retrieved.")
                if to_pd:
                    df = pd.DataFrame(all_rows, columns=headers)
                    df = utils.change_column_type(df)
                    df.columns = dimensions + metrics
                    return df
                else:
                    return all_rows, headers, types
            else:
                LOGGER.warn("no data found.")
                if to_pd:
                    return pd.DataFrame()
                else:
                    return all_rows, headers, types

        """
        pre-defined reports
        """

        def audit(self, dimension: str = 'eventName', metric: str = 'eventCount'):
            """Audit collected data for a dimension or a metric specified
            Args:
                dimension (str): api_name or display_name of a dimension
                metric (str): metric to use
            """
            df_e = self.run(
                [dimension, 'date'],
                [metric],
                start_date=self.parent.property.created_time.strftime("%Y-%m-%d"),
                end_date='yesterday'
            )
            if len(df_e) > 0:
                df = df_e.groupby(dimension).sum().merge(
                    df_e.groupby(dimension).agg({'date': 'min'}), on=dimension, how='left').merge(
                    df_e.groupby(dimension).agg({'date': 'max'}), on=dimension, how='left',
                    suffixes=['_first', '_last']).sort_values(by=[metric], ascending=False)
                return df
            else:
                return pd.DataFrame()

        def audit_dimensions(self, only: list = None, ignore: list = []):
            """ディメンションの計測アイテム毎の回数・記録された最初と最後の日
            """
            if not only:
                only = self.parent.property.show('custom_dimensions').index.to_list()

            dict = {}
            for item in only:
                if item not in ignore:
                    LOGGER.info(f"Auditing dimension {item}...")
                    dict[item] = self.audit(item)
            LOGGER.info("...done")
            return dict

        def audit_metrics(self, only: list = None, ignore: list = []):
            if not only:
                only = [d['api_name'] for d in self.parent.property.metrics if 'scope' in d]

            dict = {}
            for item in only:
                if item not in ignore:
                    LOGGER.info(f"Auditing metric {item}...")
                    dict[item] = self.audit(metric=item)
            LOGGER.info("...done")
            return dict

        def pv_by_day(self):
            dimensions = [
                'date',
                'eventName',
            ]
            metrics = [
                'eventCount',
            ]
            dimension_filter = FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    string_filter=Filter.StringFilter(value="page_view"),
                )
            )
            order_bys = [
                OrderBy(
                    desc=False,
                    dimension=OrderBy.DimensionOrderBy(
                        dimension_name="date"
                    )
                ),
            ]
            return self.run(
                dimensions,
                metrics,
                dimension_filter=dimension_filter,
                order_bys=order_bys
            )

        def events_by_day(self):
            dimensions = [
                'date',
                'eventName',
            ]
            metrics = [
                'eventCount',
            ]
            order_bys = [
                OrderBy(
                    desc=False,
                    dimension=OrderBy.DimensionOrderBy(
                        dimension_name="date"
                    )
                ),
                OrderBy(
                    desc=True,
                    metric=OrderBy.MetricOrderBy(
                        metric_name="eventCount"
                    )
                ),
            ]
            return self.run(
                dimensions,
                metrics,
                # dimension_filter=dimension_filter,
                order_bys=order_bys
            )

        def custom_dimensions(self):
            dimensions = [
                'date',
                'eventName',
            ]
            metrics = [
                'eventCount',
            ]
            # dimension_filter = FilterExpression(
            #     filter=Filter(
            #         field_name="eventName",
            #         string_filter=Filter.StringFilter(value="page_view"),
            #     )
            # )
            order_bys = [
                OrderBy(
                    desc=False,
                    dimension=OrderBy.DimensionOrderBy(
                        dimension_name="date"
                    )
                ),
            ]
            return self.run(
                dimensions,
                metrics,
                # dimension_filter=dimension_filter,
                order_bys=order_bys
            )

        def pv(self):
            dimensions = [
                # 'customUser:gtm_client_id',
                # 'customUser:ga_client_id',
                # 'customEvent:ga_session_number',
                # 'city',
                # 'customEvent:local_datetime',
                'eventName',
                'pagePath',
            ]
            metrics = [
                'eventCount',
                # 'customEvent:entrances',
                # 'customEvent:engagement_time_msec',
            ]
            (data, headers, types) = self.run(dimensions, metrics)

            return headers, data


def convert_ga4_type_to_bq_type(type):
    if type == 'string':
        return 'STRING'
    elif type == 'int':
        return 'INT64'
    elif type == 'integer':
        return 'INT64'
    elif type == 'float':
        return 'FLOAT'
    elif type == 'double':
        return 'FLOAT'


def convert_proto_datetime(dt):
    try:
        return datetime.fromtimestamp(
            dt.seconds,
            pytz.timezone('Asia/Tokyo')
        )
    except:
        return datetime.fromtimestamp(
            dt.timestamp(),
            pytz.timezone('Asia/Tokyo')
        )
