from ast import literal_eval
from typing import List, Union, Optional
from datetime import datetime

from mindsdb.integrations.libs.base_integration import BaseIntegration
from mindsdb import __version__ as mindsdb_version
from mindsdb.utilities.functions import mark_process
from lightwood.api.types import ProblemDefinition
import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mindsdb import (
    CreateDatasource,
    RetrainPredictor,
    CreatePredictor,
    DropDatasource,
    DropPredictor,
    CreateView
)

import mlflow
from mlflow.tracking import MlflowClient
import pandas as pd


class MLflowIntegration(BaseIntegration):
    def __init__(self):
        """
        An MLflow integration needs to have a working connection to work. For this:
            - All models to use should be previously served
            - An mlflow server should be running, to access the model registry
            
        Example:
            1. Run `mlflow server -p 5001 --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./artifacts --host 0.0.0.0`
            2. Run `mlflow models serve --model-uri ./model_path`
            3. Instance this integration and call the `connect method` passing the relevant urls to mlflow and to the DB
            
        Note: above, `artifacts` is a folder to store artifacts for new experiments that do not specify an artifact store.
        """  # noqa
        super().__init__()
        self.mlflow_url = None
        self.registry_path = None
        self.connection = None
        self.published_models = set()
        # self.controller = controller  # TODO: remove this, just for testing purposes

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
                         query: str,  # <- raw
                         session
                         ):
        """ 
        Inside this method, anything is valid because you assume no inter-operability with other integrations.
        
        Currently supported:
            1. Publish a predictor: this will link a pre-existing (i.e. trained) mlflow model to a mindsdb table.
                To query the predictor, make sure you serve it first.
                ref.: PUBLISH PREDICTOR name PREDICT column INVOKE AT URL DTYPES [col1 type_col1, ...];
         
        """  # noqa
        # TODO
        # publish predictor
        # create predictor # later. all I/O should be handled by the integration
            # e.g. lightwood: save/load models, store metadata about models, all that is delegated to mdb.
            # mdb should not concern itself with how it is stored, just providing the context to company/users
        # other custom syntax # later

        model_interface = session.model_interface
        data_store = session.data_store

        # TODO: probably better to use parse_sql(query, dialect='mindsdb')

        if "CREATE PREDICTOR" in query:  # TODO support "PUBLISH" instead (possible with parser?)
            query = query.replace('\n', '')  # .replace('\\', '')
            model_stmt, rest = query.split(" PREDICT ")
            model_name = model_stmt.split(" ")[-1].strip()
            if model_name in self.published_models:
                return {"error": "A model with that name has already been published!"}

            target, rest = [elt.strip().replace('`', '') for elt in rest.split("USING")] # TODO: multiple target support?
            url, dtype_info = [elt.strip() for elt in rest.split("format='mlflow',")]
            url = url.split('=')[-1].replace(',', '').strip().replace('\'', '')
            dtype_dict = literal_eval(dtype_info.split('=')[-1])

            pdef = {
                        'format': 'mlflow',
                        'dtype_dict': dtype_dict,
                        'target': target,
                        'url': {'predict': url}
            }

            # with all the gathered information, we now use mindsdb pre-existing logic to register this model as a predictor
            # model_interface.learn(model_name, None, target, None, problem_definition=pdef, delete_ds_on_fail=True)

            # name: str, from_data: dict, to_predict: str, dataset_id: int, problem_definition: dict,
            #               company_id: int, delete_ds_on_fail: Optional[bool] = False

            self._learn(model_name, None, target, None, problem_definition=pdef, company_id=None, delete_ds_on_fail=True)
            self.published_models.add(model_name)

        elif "DROP PREDICTOR" in query:
            pass  # TODO
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

    @mark_process(name='learn')
    def _learn(self, name: str, from_data: dict, to_predict: str, dataset_id: int, problem_definition: dict,
              company_id: int, delete_ds_on_fail: Optional[bool] = False) -> None:
        predictor_record = db.session.query(db.Predictor).filter_by(company_id=company_id, name=name).first()
        if predictor_record is not None:
            raise Exception('Predictor name must be unique.')

        predict_url = problem_definition['url'].get('predict', None)
        com_format = problem_definition['format']

        predictor_record = db.Predictor(
            company_id=company_id,
            name=name,
            dataset_id=dataset_id,
            mindsdb_version=mindsdb_version,
            lightwood_version=None,
            to_predict=problem_definition['target'],
            learn_args=ProblemDefinition.from_dict(problem_definition).to_dict(),
            data={'name': name, 'predict_url': predict_url, 'format': com_format, 'status': 'complete'},
            is_custom=True,
            dtype_dict=problem_definition['dtype_dict'],
        )

        db.session.add(predictor_record)
        db.session.commit()
        return

# TODO: standard formatting in describe?
# TODO: after this one is done, try a datasource and LightwoodIntegration
# TODO: may want to have an _edit_invocation_url method

if __name__ == '__main__':
    cls = MLflowIntegration()
    print(cls.connect(
        mlflow_url='http://127.0.0.1:5001',  # for this test, serve at 5001 and served model at 5000
        model_registry_path='sqlite:////Users/Pato/Work/MindsDB/temp/experiments/BYOM/mlflow.db'))
    print(cls.get_tables())
    print(cls.describe_table('nlp_kaggle4'))
    cls.run_native_query("CREATE PREDICTOR nlp_kaggle_mlflow_test_integration3 PREDICT target USING url.predict='http://localhost:5001/invocations', format='mlflow', data_dtype={'text': 'rich_text', 'target': 'binary'}")
