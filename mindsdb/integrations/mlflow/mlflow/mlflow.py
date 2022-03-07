from mindsdb.integrations.libs.base_integration import BaseIntegration


class MLflowIntegration:

    def __init__(self):
        pass

    def connect(self):
        # client = MlflowClient('http://127.0.0.1:5000', 'sqlite:///mlflow.db')

        pass

    def check_status(self):
        """  """
        pass

    def get_tables(self):
        # client.list_registered_models()  # show list of models
        pass

    def run_native_query(self, query):
        """ Inside this one, anything is valid because you assume no inter-operability """
        # create predictor
        # other custom syntax
        pass

    def select_query(self, from_stmt, where_stmt, group_by=None, order_by=None, limit=None):
        """ Here you can inter-operate betweens integrations. """

        # if not served(model):
        #   serve()
        # then...

        if 'PREDICT' in query:  # <- idea
            for column in select:
                predict(column, df)  # multiple outputs for multiple predictors
        pass

    def join(self, left_integration_instance, left_where, on=None):
        if not on:
            on = '*'
        pass

    def describe_table(self, table_name):
        """ For getting standard info about a table. e.g. data types """
        # @TODO: standard formatting
        pass

# after this one, do a datasource.































