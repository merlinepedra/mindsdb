class BaseHandler:

    def __init__(self, config, name):
        self.name = name
        self.mindsdb_database = config['api']['mysql']['database']
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)

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

    def run_native_query(self, query_str):
        """ Inside this one, anything is valid because you assume no inter-operability """
        # create predictor
        # other custom syntax
        pass

    def select_query(self, statement):
        # from_stmt: str,
        # where_stmt: List[str],  # <- implicit and between elements
        # order_by=Union[None, str],  # DESC / ASC
        # order_by_direction='DESC',
        # group_by=Union[None, List[str]],
        # limit=Union[None, int]
        """ Here you can inter-operate betweens integrations. """

        """ OLD
        This assumes all statements have been parsed and so we get:
            select_stmt: list of column names to fetch predictions from models
            from_stmt: names of models to call. Each has "hard-coded" target name in their wrapper, so the select statement is used to filter and selectively call predictors.
        where_stmt: data to use as input. This implies all predictors shall use the exact same input, for now. In the future we can somehow communicate what each one expects and that means passing ALL data here would be easier from a UX perspective.

        Example query:
            SELECT target1, target2 FROM mlflow.model1, mlflow.model2 WHERE input1 = "A" and input2 = 1000;        
        """  # noqa

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































