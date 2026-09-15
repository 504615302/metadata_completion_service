import unittest
from unittest.mock import patch

from app.services import mysql_meta_source


class _Cursor:
    def __init__(self, queries: list[str]):
        self.queries = queries

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params):
        self.queries.append(query)

    def fetchall(self):
        return []


class _Connection:
    def __init__(self, queries: list[str]):
        self.queries = queries

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _Cursor(self.queries)


class PendingMetadataQueryTests(unittest.TestCase):
    def _capture_query(self, fetch):
        queries: list[str] = []
        with patch.object(
            mysql_meta_source,
            "get_mysql_conn",
            return_value=_Connection(queries),
        ):
            self.assertEqual(fetch(limit=10), [])
        self.assertEqual(len(queries), 1)
        return " ".join(queries[0].lower().split())

    def test_field_pending_query_does_not_reference_removed_need_complete_column(self):
        query = self._capture_query(mysql_meta_source.fetch_missing_fields)

        self.assertNotIn("need_complete", query)
        self.assertNotIn("fields_daily_id", query)
        self.assertNotIn("field_id", query)
        self.assertNotIn("ch_field_name", query)
        self.assertIn("completion_type = '0'", query)
        self.assertIn("exists ( select 1 from `insert_task_tables`", query)
        self.assertIn("task_table.schema_name = daily.schema_name", query)
        self.assertIn("task_table.table_name = daily.table_name", query)

    def test_table_pending_query_does_not_reference_removed_need_complete_column(self):
        query = self._capture_query(mysql_meta_source.fetch_missing_tables)

        self.assertNotIn("need_complete", query)
        self.assertNotIn("tables_daily_id", query)
        self.assertNotIn("ch_table_name", query)
        self.assertIn("completion_type = '0'", query)
        self.assertIn("exists ( select 1 from `insert_task_tables`", query)
        self.assertIn("task_table.schema_name = daily.schema_name", query)
        self.assertIn("task_table.table_name = daily.table_name", query)


if __name__ == "__main__":
    unittest.main()
