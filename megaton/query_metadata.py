"""Raw API pagination evidence, independent of later DataFrame transforms."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal

COMPLETENESS_ATTR = "megaton_query_completeness"


@dataclass(frozen=True)
class QueryCompleteness:
    fetched_rows: int | None
    row_limit: int | None
    total_rows: int | None = None
    status: Literal["complete", "truncated", "unknown"] = "unknown"
    discarded_rows: int = 0

    def __post_init__(self) -> None:
        for count in (self.fetched_rows, self.row_limit, self.total_rows, self.discarded_rows):
            if count is not None and (type(count) is not int or count < 0):
                raise ValueError("Completeness counts must be nonnegative integers or null.")
        if self.status not in {"complete", "truncated", "unknown"}:
            raise ValueError("Invalid completeness status.")
        if self.status != "unknown" and self.fetched_rows is None:
            raise ValueError("Known completeness requires a raw fetched-row count.")
        if type(self.discarded_rows) is not int:
            raise ValueError("discarded_rows must be an integer.")
        if self.discarded_rows and self.status != "unknown":
            raise ValueError("Discarded rows prevent complete pagination evidence.")
        if self.status == "complete" and self.total_rows is not None and self.fetched_rows != self.total_rows:
            raise ValueError("Complete results must match a known total.")
        if (self.status == "truncated" and self.total_rows is not None
                and self.fetched_rows is not None and self.fetched_rows >= self.total_rows):
            raise ValueError("Truncation requires more rows than were fetched.")

    @property
    def limit_reached(self) -> bool | None:
        if self.row_limit is None or self.fetched_rows is None:
            return None
        return self.fetched_rows >= self.row_limit

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "limit_reached": self.limit_reached}

    @classmethod
    def from_frame(cls, frame, *, row_limit: int | None = None) -> QueryCompleteness:
        """Read original API pagination evidence, not current frame completeness.

        Filtering or aggregation may retain attrs unchanged. Counts and status
        still describe the source fetch; they are not recomputed from the frame.
        """
        value = frame.attrs.get(COMPLETENESS_ATTR)
        try:
            if not isinstance(value, dict):
                raise TypeError
            return cls(**{key: value[key] for key in (
                "fetched_rows", "row_limit", "total_rows", "status", "discarded_rows",
            )})
        except (KeyError, TypeError, ValueError):
            # A post-aggregation row count cannot reconstruct raw pagination.
            return cls(fetched_rows=None, row_limit=row_limit)
