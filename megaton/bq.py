"""
Functions for Google Cloud BigQuery
"""

from typing import List
import re
import sys

from google.api_core.exceptions import PermissionDenied
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer
from google.cloud.exceptions import NotFound

from . import constants, ga4


class MegatonBQ:
    """Class for Google Cloud BigQuery client
    """

    def __init__(self, parent, credentials, project_id: str):
        self.parent = parent
        self.id = project_id
        self.datasets = None
        self.credentials = credentials
        self.client = bigquery.Client(
            project=self.id,
            credentials=self.credentials
        )
        self.dts_client = bigquery_datatransfer.DataTransferServiceClient(
            credentials=self.credentials
        )
        self.dataset = self.Dataset(self)
        self.table = self.Table(self)
        self.ga4 = self.GA4(self)
        self.update()

    def update(self) -> None:
        """Get a list of dataset ids for the project"""
        # Make an API request.
        datasets = list(self.client.list_datasets())

        if datasets:
            # extract dataset id
            self.datasets = [d.dataset_id for d in datasets]
        else:
            print(f"project {self.id} has no datasets.")

    def run(self, query: str, to_dataframe: bool = False):
        """Run a query and return data
        Args:
            query (str):
                SQL query to be executed.
            to_dataframe (bool):
                if true, data is retured as pandas DataFrame
        """
        job = self.client.query(query=query)
        results = job.result()  # Waits for job to complete.

        if to_dataframe:
            return results.to_dataframe()
        else:
            return results

    class Dataset:
        def __init__(self, parent):
            self.parent = parent
            self.ref = None
            self.instance = None
            self.id = None
            self.tables = None

        def _clear(self) -> None:
            self.ref = None
            self.instance = None
            self.id = None
            self.tables = None
            self.parent.table.select()

        def select(self, dataset_id: str) -> None:
            """select dataset"""
            if dataset_id:
                if dataset_id in self.parent.datasets:
                    if dataset_id != self.id:
                        self.update(dataset_id)
                else:
                    print(f"dataset {dataset_id} is not found in the project {self.parent.id}")
            else:
                self._clear()

        def update(self, dataset_id: str = ''):
            """Get a list of table ids for the dataset specified"""
            id = dataset_id if dataset_id else self.id

            try:
                dataset = self.parent.client.get_dataset(id)
                self.instance = dataset
                self.ref = dataset.reference
                self.id = id
            except NotFound as e:
                if 'Not found: Dataset' in str(e):
                    print(f"Dataset {dataset_id} is not found in the project {self.parent.id}")
                return False

            # Make an API request
            tables = list(self.parent.client.list_tables(dataset))

            if tables:
                # extract table id
                self.tables = [d.table_id for d in tables]
            else:
                print(f"dataset {self.id} has no tables.")

    class Table:
        def __init__(self, parent):
            self.parent = parent
            self.ref = None
            self.instance = None
            self.id = None

        # def _get_info(self):
        #     """Get metadata for the table"""

        def select(self, table_id: str) -> None:
            """select a table"""
            if table_id:
                if table_id in self.parent.dataset.tables:
                    if table_id != self.id:
                        self.update(table_id)
                else:
                    print(f"table {table_id} is not found in the dataset {self.parent.dataset.id}")
            else:
                self.ref = None
                self.instance = None
                self.id = None

        def update(self, table_id: str = ''):
            """Get an api reference for the table"""
            id = table_id if table_id else self.id
            if self.parent.dataset.ref:
                try:
                    table_ref = self.parent.dataset.ref.table(id)
                    self.ref = table_ref
                    self.instance = self.parent.client.get_table(self.ref)
                    self.id = id
                    # self._get_info()
                except Exception as e:
                    raise e
            else:
                print("Please select a dataset first.")

        def create(self, table_id: str, schema: bigquery.SchemaField, description: str = '',
                   partitioning_field: str = '', clustering_fields=None) -> bigquery.table.Table:
            if clustering_fields is None:
                clustering_fields = []
            dataset_ref = self.parent.dataset.ref
            table_ref = dataset_ref.table(table_id)
            table = bigquery.Table(table_ref, schema=schema)

            if partitioning_field:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partitioning_field,  # name of column to use for partitioning
                )
            if clustering_fields:
                table.clustering_fields = clustering_fields
            if description:
                table.description = description

            # Make an API request.
            table = self.parent.client.create_table(table)

            print(f"Created a table '{table.table_id}'", end='')
            if table.time_partitioning.field:
                print(f", partitioned on a column '{table.time_partitioning.field}'")
            self.parent.dataset.update()

            return table

    class GA4:
        """utilities to manage GA4"""

        def __init__(self, parent):
            self.parent = parent
            self.clustering_fields = ['client_id', 'event_name']
            self.flat_table_id = 'flat'
            self.start_date = ''
            self.end_date = ''

        @property
        def first_date_recorded(self) -> str:
            """the first date GA4 data was transferred to BigQuery in YYYYMMDD format"""
            partitions = [t for t in self.parent.dataset.tables if t.startswith('events_')]
            if partitions:
                return sorted(partitions)[0].replace("events_", "")

        def set_dates(self, start_date: str, end_date: str) -> None:
            self.start_date = start_date.strip()
            self.end_date = end_date.strip()

        def audit_ep(self):
            sql = f'''--Event Parameterの発生状況
                SELECT
                    param.key AS parameter_name,
                    event_name,
                    DATE(timestamp_micros(event_timestamp), 'Asia/Tokyo') AS date,
                    COUNT(1) AS count,
                    COUNT(param.value.string_value) AS string,
                    COUNT(param.value.int_value) AS int,
                    COUNT(param.value.float_value) AS float,
                    COUNT(param.value.double_value) AS double,
                FROM
                    `analytics_{self.parent.parent.ga['4'].property.id}.events_*`
                        CROSS JOIN UNNEST(event_params) AS param
                WHERE
                    _TABLE_SUFFIX >= '{self.start_date}' AND _TABLE_SUFFIX <= '{self.end_date}'
                GROUP BY 1,2,3
                ORDER BY 1,2,3
            '''
            df = self.parent.run(sql, to_dataframe=True)
            return df

        def template_schema(self) -> list:
            self.parent.parent.launch_gs(constants.GOOGLE_SHEET_GA4_TEMPLATE_URL)
            if self.parent.parent.gs.sheet.select('推奨BQ'):
                return [d for d in self.parent.parent.gs.sheet.data if d['Valid']]

        def get_flat_schema(self, ep: list, up: list) -> list:
            gs_data = self.template_schema()
            basic_schema = [
                {
                    'name': d['Field Name'],
                    'type': d['Type'],
                    'description': d['Description']
                 } for d in gs_data
            ]

            custom_schema = []
            for d in up + ep:
                custom_schema.append({
                    'name': d['field_name'],
                    'type': ga4.convert_ga4_type_to_bq_type(d['type']),
                    'description': d['description']
                })
            return basic_schema + custom_schema

        def get_bq_schema(self, schema: list) -> List[bigquery.SchemaField]:
            """Converts a dictionary to list of bigquery.SchemaField
            for use with bigquery client library.
            Dict must contain name and type keys.
            """
            return [
                bigquery.SchemaField(
                    name=x['name'],
                    field_type=x['type'],
                    description=x.get('description') if x.get('description') else '')
                for x in schema
            ]

        def create_flat_table(self, schema: list) -> None:
            """Create a table to store flattened GA4 data."""
            print(f"Creating a table to store flattened GA4 data.")
            # Make an API request.
            self.parent.table.create(
                table_id=self.flat_table_id,
                description='This is a table to store flattened and optimized GA4 data exported.',
                schema=self.get_bq_schema(schema),
                partitioning_field='date',
                clustering_fields=self.clustering_fields
            )

        def flatten_events(
                self,
                date1: str,
                date2: str,
                event_parameters: list = [],
                user_properties: list = [],
                to: str = 'dataframe'
        ) -> None:
            """Flattened event tables exported from GA4"""

            sql = self.get_query_to_flatten_events(
                date1,
                date2,
                event_parameters=event_parameters,
                user_properties=user_properties,
            )

            if to == 'dataframe':
                # return the data as pandas dataframe
                df = self.parent.run(sql).to_dataframe()
                print(f"{len(df)} rows were retrieved.")
                return df
            elif to == 'table':
                # append the data to a table
                self.parent.table.select(self.flat_table_id)
                table_ref = self.parent.table.ref
                rows_before = self.parent.table.instance.num_rows
                print(f"The table '{self.flat_table_id}' had {rows_before} rows.")

                job_config = bigquery.QueryJobConfig(
                    # clustering_fields=self.clustering_fields,
                    destination=table_ref,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )
                # Start the query, passing in the extra configuration.
                query_job = self.parent.client.query(sql, job_config=job_config)  # Make an API request.
                result_iterator = query_job.result()  # Wait for the job to complete.

                rows_after = result_iterator.total_rows
                print(f"{rows_after - rows_before} rows were added for the period {date1} - {date2}")
                # return result_iterator
            else:
                print(f"Unknown destination: {to}")

        def get_query_to_flatten_events(
                self,
                date1: str,
                date2: str,
                event_parameters: list = [],
                user_properties: list = [],
                to: str = 'select'
        ):
            """Return a query to flatten GA4 event tables exported"""

            dataset = self.parent.dataset.id
            table_id = self.flat_table_id
            schema = self.template_schema()

            if to == 'scheduled_query':
                query = f'''--未処理のGA4生データを変換しflatテーブルへ追記
DECLARE last_exported_date DATE;
DECLARE next_date_to_be_processed DATE;
DECLARE processing_date STRING;

EXECUTE IMMEDIATE FORMAT("""
  --最後にエクスポートされたGA4データの日付
  SELECT CAST(RIGHT(table_name, 8) AS DATE FORMAT 'YYYYMMDD')
  FROM `{dataset}.INFORMATION_SCHEMA.TABLES`
  WHERE REGEXP_CONTAINS(table_name, r'^events_2')
  ORDER BY 1 DESC
  LIMIT 1
""") INTO last_exported_date;

EXECUTE IMMEDIATE FORMAT("""
  --未処理の最初の日付
  SELECT DATE_ADD(date, INTERVAL 1 DAY)
  FROM `{dataset}.{table_id}`
  WHERE date > DATE_SUB('%t', INTERVAL 10 DAY)--節約のため直近に絞る
  GROUP BY 1
  ORDER BY 1 DESC
  LIMIT 1
""", last_exported_date) INTO next_date_to_be_processed;

--処理すべき日付を判定
IF next_date_to_be_processed = last_exported_date THEN
  SET processing_date = FORMAT_DATE("%Y%m%d", last_exported_date);
  SELECT FORMAT('New data for %t found. Processing events_%s...', next_date_to_be_processed, processing_date);
ELSEIF next_date_to_be_processed < last_exported_date THEN
  SET processing_date = FORMAT_DATE("%Y%m%d", next_date_to_be_processed);
  SELECT FORMAT('New data for %t found, but processing events_%s first...', last_exported_date, processing_date);
ELSE
  --skip
  RAISE USING MESSAGE = FORMAT("%t has already been processed. Skipping...", last_exported_date);
END IF;

--go ahead
INSERT INTO `{dataset}.{table_id}` (
  '''

            else:
                query = ''
            query += f'''--Flatten GA4 events
  SELECT'''

            for s in schema:
                if s['Category']:
                    query += f'''
    --{s['Category']}'''
                query += f'''
    {s['Select']} AS {s['Field Name']},'''

            if user_properties:
                query += f'''
    --Custom User Properties'''
                for d in user_properties:
                    query += f'''
    (SELECT value.{d['type']}_value FROM UNNEST(user_properties) WHERE key = '{d['key']}') AS {d['field_name']},'''

            if event_parameters:
                query += f'''
    --Custom Event Parameters'''
                for d in event_parameters:
                    query += f'''
    (SELECT value.{d['type']}_value FROM UNNEST(event_params) WHERE key = '{d['key']}') AS {d['field_name']},'''

            query += f'''
  FROM
    `{dataset}.events_*`
  WHERE
    '''

            if to == 'scheduled_query':
                query += f"""_TABLE_SUFFIX = processing_date
);

SELECT FORMAT("%d rows (%d bytes) of data for %s were successfully inserted into %s.", @@row_count, @@script.bytes_processed, processing_date, output_dataset);
"""

            else:
                query += f"""_TABLE_SUFFIX >= '{date1}' AND _TABLE_SUFFIX <= '{date2}'"""

            return query

        def schedule_query_to_flatten_events(
                self,
                event_parameters: list = [],
                user_properties: list = []
        ):
            """Save a scheduled query to flatten event tables exported from GA4"""
            sql = self.get_query_to_flatten_events('', '',
                                                   event_parameters,
                                                   user_properties,
                                                   to='scheduled_query')

            dataset_id = self.parent.dataset.id
            table_id = self.parent.table.id

            transfer_config = bigquery_datatransfer.TransferConfig(
                # destination_dataset_id=dataset_id,
                display_name=f"Flatten GA4 events for {dataset_id}",
                data_source_id="scheduled_query",
                params={
                    "query": sql,
                },
                schedule="every 4 hours",
            )

            request = bigquery_datatransfer.CreateTransferConfigRequest(
                parent=self.parent.dts_client.common_location_path(
                    self.parent.id,
                    self.parent.dataset.instance.location),
                transfer_config=transfer_config,
            )

            try:
                response = self.parent.dts_client.create_transfer_config(request=request)
                print(f"Scheduled query was created: {response.name}")
                return response
            except PermissionDenied as e:
                print("APIを実行する権限がありません。")
                m = re.search(r'reason: "([^"]+)', str(sys.exc_info()[1]))
                if m:
                    reason = m.group(1)
                    if reason == 'SERVICE_DISABLED':
                        print("GCPのプロジェクトでBigQuery Data Transfer APIを有効化してください。")
                message = getattr(e, 'message', repr(e))
                print(message)
