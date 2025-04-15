# Define a custom operator for advanced use cases
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CustomOperator(BaseOperator):

    @apply_defaults
    def __init__(self, custom_param, *args, **kwargs):
        super(CustomOperator, self).__init__(*args, **kwargs)
        self.custom_param = custom_param

    def execute(self, context):
        # Custom logic to be executed by the operator
        self.log.info(f'Custom parameter value: {self.custom_param}')
        # Add custom logic here
