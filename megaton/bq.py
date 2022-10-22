"""
Functions for Google Cloud BigQuery
"""

from typing import Dict, List
import re
import sys

from google.api_core.exceptions import PermissionDenied
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer
from google.cloud.exceptions import NotFound


class MegatonBQ:
    """Class for Google Cloud BigQuery client
    """

    def __init__(self, credentials, project_id: str):
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
        self.for_ga4 = self.ForGA4(self)
        self.update()

    def update(self):
        """Get a list of dataset ids for the project"""
        # Make an API request.
        datasets = list(self.client.list_datasets())

        if datasets:
            # extract dataset id
            self.datasets = [d.dataset_id for d in datasets]
        else:
            print(f"project {self.id} does not have any datasets.")

    def run(self, query: str):
        """Run a SQL query and return data
        Args:
            query (str):
                SQL query to be executed.
        """
        job = self.client.query(query=query)
        results = job.result()  # Waits for job to complete.
        return results

    class Dataset:
        def __init__(self, parent):
            self.parent = parent
            self.ref = None
            self.instance = None
            self.id = None
            self.tables = None

        def select(self, id: str):
            if id:
                if id in self.parent.datasets:
                    if id != self.id:
                        self.update(id)
                else:
                    print(f"dataset {id} is not found in the project {self.parent.id}")
            else:
                self.ref = None
                self.instance = None
                self.id = None
                self.tables = None
                self.parent.table.select()

        def update(self, dataset_id: str = ''):
            """Get a list of table ids for the dataset"""
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

            # Make an API request.
            tables = list(self.parent.client.list_tables(dataset))

            if tables:
                # extract table id
                self.tables = [d.table_id for d in tables]
            else:
                print(f"dataset {self.id} does not have any tables.")

    class Table:
        def __init__(self, parent):
            self.parent = parent
            self.ref = None
            self.instance = None
            self.id = None

        def _get_info(self):
            """Get metadata of the table"""

        def select(self, id: str):
            if id:
                if id in self.parent.dataset.tables:
                    if id != self.id:
                        self.update(id)
                else:
                    print(f"table {id} is not found in the dataset {self.parent.dataset.id}")
            else:
                self.ref = None
                self.instance = None
                self.id = None

        def update(self, table_id: str = ''):
            """Get an api reference for a table"""
            id = table_id if table_id else self.id
            if self.parent.dataset.ref:
                try:
                    table_ref = self.parent.dataset.ref.table(id)
                    self.ref = table_ref
                    self.instance = self.parent.client.get_table(self.ref)
                    self.id = id
                    self._get_info()
                except Exception as e:
                    raise e
            else:
                print("Please select a dataset first.")

        def create(self, table_id: str, schema: bigquery.SchemaField, description: str = '', partitioning_field: str = '',
                   clustering_fields: list = []):
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

            print(f"Created table {table.table_id}", end='')
            if table.time_partitioning.field:
                print(f", partitioned on column {table.time_partitioning.field}")
            self.parent.dataset.update()

            return table

    class ForGA4:
        """utilities to manage GA4"""

        def __init__(self, parent):
            self.parent = parent
            self.clustering_fields = ['client_id', 'event_name']
            self.clean_table_id = 'clean'

        def get_first_date_recorded(self):
            partitions = [t for t in self.parent.dataset.tables if t.startswith('events_')]
            if partitions:
                return sorted(partitions)[0].replace("events_", "")

        def create_clean_table(self, schema: Dict):
            """Create a table to store flatten GA data."""
            print(f"Creating a table to store flattened GA data.")
            # Make an API request.
            self.parent.table.create(
                table_id=self.clean_table_id,
                description='This is a table to store flattened and optimized GA4 data based on the exported raw data.',
                schema=self.dict_to_bq_schema(schema),
                partitioning_field='date',
                clustering_fields=self.clustering_fields
            )

        def dict_to_bq_schema(self, schema: Dict) -> List[bigquery.SchemaField]:
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
            return schema

        def flatten_events(
                self,
                date1: str,
                date2: str,
                schema: Dict,
                event_parameters: list = [],
                user_properties: list = [],
                to: str = 'dataframe'
        ):
            """Flatten event tables exported from GA4"""

            sql = self.get_query_to_flatten_events(
                date1,
                date2,
                schema,
                event_parameters=event_parameters,
                user_properties=user_properties,
            )

            if to == 'dataframe':
                # return the data as pandas dataframe
                df = self.parent.run(sql).to_dataframe()
                print(f"{len(df)} rows were retrieved.")
                return df
            elif to == 'table':
                # append data to the clean table
                self.parent.table.select(self.clean_table_id)
                table_ref = self.parent.table.ref
                rows_before = self.parent.table.instance.num_rows
                print(f"The table '{self.clean_table_id}' had {rows_before} rows.")

                job_config = bigquery.QueryJobConfig(
                    # clustering_fields=self.clustering_fields,
                    destination=table_ref,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )
                # Start the query, passing in the extra configuration.
                query_job = self.parent.client.query(sql, job_config=job_config)  # Make an API request.
                result_iterator = query_job.result()  # Wait for the job to complete.

                rows_after = result_iterator.total_rows
                print(f"{rows_after - rows_before} rows were added for the period ({date1} - {date2})")
                return result_iterator
            else:
                print(f"Unknown destination: {to}")

        def get_query_to_flatten_events(
                self,
                date1: str,
                date2: str,
                schema: Dict,
                event_parameters: list = [],
                user_properties: list = [],
                to: str = 'select'
        ):
            """Return a query to flatten event tables exported from GA4"""

            project_id = self.parent.id
            dataset = self.parent.dataset.id
            table_id = self.clean_table_id

            if to == 'scheduled_query':
                query = f'''DECLARE Yesterday DATE DEFAULT DATE_SUB(DATE(@run_time, "Asia/Tokyo"), INTERVAL 1 DAY);
DECLARE YesterdayRecords DEFAULT (
    --check if data already exists
    SELECT COUNT(1)
    FROM `{dataset}.{table_id}`
    WHERE date = Yesterday
);

IF YesterdayRecords > 0 THEN
    --skip
    RAISE USING MESSAGE = FORMAT("%t already has %d records. skipping...", Yesterday, YesterdayRecords);
ELSE
    --go ahead
    INSERT INTO `{dataset}.{table_id}` (
        '''
            else:
                query = '        '
            query += f'''--GA4 flatten events
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
                query += f"""_TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", Yesterday)
    );
END IF;"""

            else:
                query += f"""_TABLE_SUFFIX >= '{date1}' AND _TABLE_SUFFIX <= '{date2}'"""

            return query

        def schedule_query_to_flatten_events(
                self,
                schema: Dict,
                event_parameters: list = [],
                user_properties: list = []
        ):
            """Save a scheduled query to flatten event tables exported from GA4"""
            sql = self.get_query_to_flatten_events('', '',
                                                   schema,
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
                print(f"Schedule query was created: {response.name}")
                return response
            except PermissionDenied as e:
                print("権限がありません。")
                m = re.search(r'reason: "([^"]+)', str(sys.exc_info()[1]))
                if m:
                    reason = m.group(1)
                    if reason == 'SERVICE_DISABLED':
                        print("GCPのプロジェクトでBigQuery Data Transfer APIを有効化してください。")
                message = getattr(e, 'message', repr(e))
                print(message)
