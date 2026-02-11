"""
Functions for Google Cloud BigQuery
"""

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

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
        self.dataset = self.Dataset(self)
        self.table = self.Table(self)
        self.ga4 = self.GA4(self)
        self.update()

    def update(self) -> bool:
        """Refresh dataset ids for the project."""
        # Make an API request.
        datasets = list(self.client.list_datasets())

        if datasets:
            # extract dataset id
            self.datasets = [d.dataset_id for d in datasets]
        else:
            self.datasets = []
            print(f"project {self.id} has no datasets.")
        return True

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

        def _clear(self) -> bool:
            self.ref = None
            self.instance = None
            self.id = None
            self.tables = None
            self.parent.table.select()
            if hasattr(self.parent.parent, "state"):
                self.parent.parent.state.bq_dataset_id = None
                self.parent.parent.state.bq_table_id = None
            return True

        def select(self, dataset_id: str | None = None) -> bool:
            """Select a dataset by id, or clear selection when empty."""
            if not dataset_id:
                return self._clear()

            if self.parent.datasets is None:
                self.parent.update()
            if dataset_id not in (self.parent.datasets or []):
                raise ValueError(
                    f"Dataset '{dataset_id}' is not found in project '{self.parent.id}'."
                )
            if dataset_id == self.id and self.ref is not None:
                return True
            return self.update(dataset_id)

        def update(self, dataset_id: str = '') -> bool:
            """Refresh selected dataset metadata and table ids."""
            dataset_id = dataset_id if dataset_id else self.id
            if not dataset_id:
                raise ValueError("No dataset selected. Call bq.dataset.select(dataset_id) first.")

            try:
                dataset = self.parent.client.get_dataset(dataset_id)
                self.instance = dataset
                self.ref = dataset.reference
                self.id = dataset_id
                if hasattr(self.parent.parent, "state"):
                    self.parent.parent.state.bq_dataset_id = dataset_id
            except NotFound as exc:
                raise ValueError(
                    f"Dataset '{dataset_id}' is not found in project '{self.parent.id}'."
                ) from exc

            # Make an API request
            tables = list(self.parent.client.list_tables(dataset))

            if tables:
                # extract table id
                self.tables = [d.table_id for d in tables]
            else:
                self.tables = []
                print(f"dataset {self.id} has no tables.")
            return True

    class Table:
        def __init__(self, parent):
            self.parent = parent
            self.ref = None
            self.instance = None
            self.id = None

        # def _get_info(self):
        #     """Get metadata for the table"""

        def select(self, table_id: str | None = None) -> bool:
            """Select a table by id, or clear selection when empty."""
            if not table_id:
                self.ref = None
                self.instance = None
                self.id = None
                if hasattr(self.parent.parent, "state"):
                    self.parent.parent.state.bq_table_id = None
                return True

            if not self.parent.dataset.ref:
                raise ValueError("No dataset selected. Call bq.dataset.select(dataset_id) first.")
            if self.parent.dataset.tables is None:
                self.parent.dataset.update(self.parent.dataset.id)
            if table_id not in (self.parent.dataset.tables or []):
                raise ValueError(
                    f"Table '{table_id}' is not found in dataset '{self.parent.dataset.id}'."
                )
            if table_id == self.id and self.ref is not None:
                return True
            return self.update(table_id)

        def update(self, table_id: str = '') -> bool:
            """Refresh selected table metadata."""
            if not self.parent.dataset.ref:
                raise ValueError("No dataset selected. Call bq.dataset.select(dataset_id) first.")
            table_id = table_id if table_id else self.id
            if not table_id:
                raise ValueError("No table selected. Call bq.table.select(table_id) first.")
            try:
                table_ref = self.parent.dataset.ref.table(table_id)
                self.ref = table_ref
                self.instance = self.parent.client.get_table(self.ref)
                self.id = table_id
                if hasattr(self.parent.parent, "state"):
                    self.parent.parent.state.bq_table_id = table_id
                return True
            except NotFound as exc:
                raise ValueError(
                    f"Table '{table_id}' is not found in dataset '{self.parent.dataset.id}'."
                ) from exc

        def create(self, table_id: str, schema: list[bigquery.SchemaField], description: str = '',
                   partitioning_field: str = '', clustering_fields=None) -> bigquery.table.Table:
            if not self.parent.dataset.ref:
                raise ValueError("No dataset selected. Call bq.dataset.select(dataset_id) first.")
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
