from typing import List, Union
from datetime import datetime

from mindsdb.integrations.libs.base_integration import BaseIntegration
from mindsdb.interfaces.model.model_controller import ModelController

import mlflow
from mlflow.tracking import MlflowClient
import pandas as pd


class MLflowIntegration(BaseIntegration):
    def __init__(self, controller):
        """
        An MLflow integration needs to have a working connection to work. For this:
            - All models to use should be previously served
            - An mlflow server should be running, to access the model registry
            
        Example:
            1. Run `mlflow server --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./artifacts --host 0.0.0.0`
            2. Run `mlflow models serve --model-uri ./model_path`
            3. Instance this integration and call the `connect method` passing the relevant urls to mlflow and to the DB
            
        Note: above, `artifacts` is a folder to store artifacts for new experiments that do not specify an artifact store.
        """  # noqa
        super().__init__()
        self.mlflow_url = None
        self.registry_path = None
        self.connection = None
        self.published_model_names = set()
        self.published_models = {}
        self.controller = controller  # TODO: remove this, just for testing purposes

    def connect(self, mlflow_url, model_registry_path):
        """ Connect to the mlflow process using MlflowClient class. """  # noqa
        self.mlflow_url =  mlflow_url
        self.registry_path = model_registry_path
        self.connection = MlflowClient(self.mlflow_url, self.registry_path)
        return self.check_status()

    def check_status(self):
        """ Checks that the connection is, as expected, an MlflowClient instance. """  # noqa
        # TODO: use a heartbeat method (pending answer in slack, potentially not possible)
        try:
            assert isinstance(self.connection, mlflow.tracking.MlflowClient)
        except AssertionError as e:
            return {'status': '503', 'error': e}  # service unavailable
        return {'status': '200'}  # ok

    def get_tables(self):
        """ Returns list of model names """
        tables = [model.name for model in self._get_models()]
        return tables

    def _get_models(self):
        """ Returns RegisteredModel instances stored at self.registry_path """  # noqa
        return self.connection.list_registered_models()

    def run_native_query(self,
                         query: str  # <- raw
                         ):
        """ 
        Inside this method, anything is valid because you assume no inter-operability with other integrations.
        
        Currently supported:
            1. Publish a predictor: this will link a pre-existing (i.e. trained) mlflow model to a mindsdb table.
                ref.: PUBLISH PREDICTOR name PREDICT column INVOKE AT URL DTYPES [col1 type_col1, ...];
         
        """  # noqa
        # TODO
        # publish predictor
        # create predictor # later. all I/O should be handled by the integration
            # e.g. lightwood: save/load models, store metadata about models, all that is delegated to mdb.
            # mdb should not concern itself with how it is stored, just providing the context to company/users
        # other custom syntax # later

        # NOTE: this is a barebones parser, we should streamline it based on the work done at mindsdb_sql repo
        if "PUBLISH PREDICTOR" in query:
            model_stmt, rest = query.split("PREDICT")
            model_name = model.split(" ")[-1].strip()
            if model_name in self.published_model_names:
                return {"error": "A model with that name has already been published!"}

            target, rest = [elt.strip() for elt in rest.split("INVOKE AT")] # TODO: multiple target support?
            url, dtype_info = [elt.strip() for elt in rest.split("DTYPES")]
            input_cols = dtype_info[::2]
            dtypes = dtype_info[1::2]

            # with all the gathered information, we now use mindsdb pre-existing logic to register this model as a predictor
            self.controller.learn(
                name=model_name,
                from_data=None,
                to_predict=[target],
                dataset_id=None,
                kwargs={
                    'format': 'mlflow',
                    'dtype_dict': {col: dtype for col, dtype in zip(input_cols, dtypes)},
                    'target': target,
                    'url': {'predict': url}
                },
                company_id=None,
                delete_ds_on_fail=True
            )

        else:
            return {"error": "QUERY NOT SUPPORTED"}

    def select_query(self,
                     from_stmt: str,
                     where_stmt: List[str],  # <- implicit and between elements
                     order_by=Union[None, str],  # DESC / ASC
                     order_by_direction='DESC',
                     group_by=Union[None, List[str]],
                     limit=Union[None, int]
                     ):
        """
        This assumes all statements have been parsed and so we get:
            select_stmt: list of column names to fetch predictions from models
            from_stmt: names of models to call. Each has "hard-coded" target name in their wrapper, so the select statement is used to filter and selectively call predictors.
        where_stmt: data to use as input. This implies all predictors shall use the exact same input, for now. In the future we can somehow communicate what each one expects and that means passing ALL data here would be easier from a UX perspective.

        Example supported query:
            SELECT target1, target2 FROM mlflow.model1, mlflow.model2 WHERE input1 = "A" and input2 = 1000;

        NB: In general, for this method in all subclasses you can inter-operate betweens integrations here.
        """  # noqa
        # TODO

        # if not served(model):  # not served => # pass dummy data (empty DF) to model so that we know it's listening?
        #   serve()
        # then call...

        outputs = []
        for model_name in from_stmt:
            # get model
            model = self.connection.get_registered_model(model_name)
            if model.target in select_stmt:
                outputs.append(model.predict(where_stmt))

        # TODO: add order_by, group_by
        out = pd.DataFrame.from_records(outputs, columns=select_stmt)
        return out

    def join(self, left_integration_instance, left_where, on=None):
        # TODO
        if not on:
            on = '*'
        pass

    def describe_table(self, table_name: str):
        """ For getting standard info about a table. e.g. data types """  # noqa
        model = None
        for entry in self._get_models():
            if entry.name == table_name:
                model = entry

        latest_version = model.latest_versions[-1]
        description = {
            'NAME': model.name,
            'USER_DESCRIPTION': model.description,
            'LAST_STATUS': latest_version.status,
            'CREATED_AT': datetime.fromtimestamp(model.creation_timestamp//1000).strftime("%m/%d/%Y, %H:%M:%S"),
            'LAST_UPDATED': datetime.fromtimestamp(model.last_updated_timestamp//1000).strftime("%m/%d/%Y, %H:%M:%S"),
            'TAGS': model.tags,
            'LAST_RUN_ID': latest_version.run_id,
            'LAST_SOURCE_PATH': latest_version.source,
            'LAST_USER_ID': latest_version.user_id,
            'LAST_VERSION': latest_version.version,
        }
        return description


# Some pending stuff:
# TODO: how are we communicating connection success?
# TODO: all non-standard methods should be _private
# TODO: standard formatting in describe?
# TODO: after this one is done, try a datasource and LightwoodIntegration


if __name__ == '__main__':
    controller = ModelController(ray_based=False)
    cls = MLflowIntegration(controller)
    print(cls.connect(
        mlflow_url='http://127.0.0.1:5000',
        model_registry_path='sqlite:////Users/Pato/Work/MindsDB/temp/experiments/BYOM/mlflow.db'))
    print(cls.get_tables())
    print(cls.describe_table('nlp_kaggle4'))
    cls.run_native_query("PUBLISH PREDICTOR nlp_kaggle_mlflow PREDICT target INVOKE AT 'http://localhost:5000/invocations' DTYPES text rich_text target binary")


























