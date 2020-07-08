from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="redshift",
                 dq_checks=[{'check_sql': 'SELECT 1',
                             'expected_result': 1}],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    #Adapted from https://knowledge.udacity.com/questions/54406
    def execute(self, context):
        passed_count = 0
        failed_count = 0
        passed_tests = []
        failed_tests = []
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for check in self.dq_checks:
            chk_sql = check.get('check_sql')
            exp_result = check.get('expected_result')
        
            records = redshift_hook.get_records(chk_sql)[0]
        
            if exp_result != records[0]:
                failed_count += 1
                failed_tests.append(chk_sql)
            else:
                passed_count += 1
                passed_tests.append(chk_sql)
        
        self.log.info("PASSED TEST(S):")
        for passed_test in passed_tests:
            self.log.info(passed_test)
        
        if failed_count > 0:
            self.log.info("FAILED TEST(S):")
            for failed_test in failed_tests:
                self.log.info(failed_test)
            raise ValueError("DATA QUALITY CHECK FAILED!")
        else:
            self.log.info("DATA QUALITY CHECK PASSED!")
