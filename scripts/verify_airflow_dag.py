"""Run with Airflow's Python inside the container: real imports, no test stubs."""
import os
import runpy
import unittest
from unittest.mock import patch


class DagTests(unittest.TestCase):
    def get_dag(self, schedule="0 * * * *"):
        with patch.dict(os.environ, {"PIPELINE_SCHEDULE": schedule}):
            return runpy.run_path("/opt/airflow/dags/earthquake_ingestion_dag.py")["dag"]

    def test_import_and_dependencies(self):
        dag = self.get_dag()
        self.assertEqual(dag.dag_id, "quaketrack_earthquake_ingestion")
        self.assertEqual(set(dag.task_ids), {"determine_window", "ingest_and_check", "record_results"})
        self.assertEqual(dag.get_task("determine_window").downstream_task_ids, {"ingest_and_check"})
        self.assertEqual(dag.get_task("ingest_and_check").downstream_task_ids, {"record_results"})

    def test_scheduling_and_limits(self):
        dag = self.get_dag()
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(dag.get_task("ingest_and_check").retries, 2)
        self.assertIsNotNone(dag.get_task("ingest_and_check").execution_timeout)
        self.assertIsNotNone(dag.schedule)

    def test_manual_only_configuration(self):
        self.assertIsNone(self.get_dag("None").schedule)


if __name__ == "__main__":
    unittest.main()
