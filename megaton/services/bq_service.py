"""BigQuery service wrapper."""

import logging

from .. import bq

logger = logging.getLogger(__name__)


class BQService:
    def __init__(self, app):
        self.app = app

    def launch_bigquery(self, project_id: str):
        if not self.app.creds:
            logger.warning('認証が完了していないため、BigQuery を初期化できません。')
            return None
        self.app.bq = bq.MegatonBQ(self.app, self.app.creds, project_id)
        self.app.state.bq_project_id = project_id
        return self.app.bq
