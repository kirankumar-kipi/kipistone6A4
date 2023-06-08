import unittest
from airflow.models import DagBag

class TestPaymentDataIngestion(unittest.TestCase):
    """Check AR Dag expectation"""

    def setUp(self):
        self.dagbag = DagBag()

    def test_task_count(self):
        """Check task count of hello_world dag"""
        dag_id='PAYMENT_DATA_TO_SNOWFLAKE'
        dag = self.dagbag.get_dag(dag_id)
        self.assertEqual(len(dag.tasks), 3)

    def test_contain_tasks(self):
        """Check task contains in hello_world dag"""
        dag_id='PAYMENT_DATA_TO_SNOWFLAKE'
        dag = self.dagbag.get_dag(dag_id)
        tasks = dag.tasks
        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertListEqual(task_ids, ['metadata_entry', 's3_copy_into_table','audit'])

    def test_dependencies_of_dummy_task(self):
        """Check the task dependencies of dummy_task in hello_world dag"""
        dag_id='PAYMENT_DATA_TO_SNOWFLAKE'
        dag = self.dagbag.get_dag(dag_id)
        dummy_task = dag.get_task('metadata_entry')

        upstream_task_ids = list(map(lambda task: task.task_id, dummy_task.upstream_list))
        self.assertListEqual(upstream_task_ids, [])
        downstream_task_ids = list(map(lambda task: task.task_id, dummy_task.downstream_list))
        self.assertListEqual(downstream_task_ids, ['s3_copy_into_table'])

    def test_dependencies_of_hello_task(self):
        """Check the task dependencies of hello_task in hello_world dag"""
        dag_id='PAYMENT_DATA_TO_SNOWFLAKE'
        dag = self.dagbag.get_dag(dag_id)
        hello_task = dag.get_task('audit')

        upstream_task_ids = list(map(lambda task: task.task_id, hello_task.upstream_list))
        self.assertListEqual(upstream_task_ids, ['s3_copy_into_table'])
        downstream_task_ids = list(map(lambda task: task.task_id, hello_task.downstream_list))
        self.assertListEqual(downstream_task_ids, [])

suite = unittest.TestLoader().loadTestsFromTestCase(TestPaymentDataIngestion)
unittest.TextTestRunner(verbosity=1).run(suite)