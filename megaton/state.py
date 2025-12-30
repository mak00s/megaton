from dataclasses import dataclass
from typing import Optional


@dataclass
class MegatonState:
    """Lightweight mirror of current selections/settings."""
    ga_version: Optional[str] = None
    ga_account_id: Optional[str] = None
    ga_property_id: Optional[str] = None
    ga_view_id: Optional[str] = None
    gs_url: Optional[str] = None
    gs_title: Optional[str] = None
    gs_sheet_name: Optional[str] = None
    bq_project_id: Optional[str] = None
    bq_dataset_id: Optional[str] = None
    bq_table_id: Optional[str] = None
