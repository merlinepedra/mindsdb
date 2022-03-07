class BaseIntegration:

    def __init__(self):
        pass

    def connect(self):
        pass

    def check_status(self):
        pass

    def get_tables(self):
        # show list of models
        pass

    def get_views(self):
        # not supported for predictor integrations (makes no sense)
        pass

    def run_native_query(self, query):
        """ Inside this one, anything is valid because you assume no inter-operability """
        # create predictor
        # other custom syntax
        pass

    def select_query(self, from_stmt, where_stmt, group_by=None, order_by=None, limit=None):
        """ Here you can inter-operate betweens integrations. """
        if 'PREDICT' in query:  # <- idea
            for column in select:
                predict(column, df)  # multiple outputs for multiple predictors
        pass

    def join(self, left_integration_instance, left_where, on=None):
        if not on:
            on = '*'
        pass

    def select_into(self, integration_instance, stmt):
        # not supported in predictor integrations @TODO: figure it out (interface)
        """ snf -> CTA.table_train """
        pass

    def describe_table(self, table_name):
        """ For getting standard info about a table. e.g. data types """
        # @TODO: standard formatting
        pass































