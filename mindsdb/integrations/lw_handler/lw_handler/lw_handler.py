import os
import dill
import requests
import torch.multiprocessing as mp

from ast import literal_eval
from typing import Dict, List, Union, Optional
from datetime import datetime

from lightwood.api.high_level import json_ai_from_problem, predictor_from_code, code_from_json_ai, ProblemDefinition

from mindsdb.integrations.libs.base_handler import BaseHandler, PredictiveHandler
from mindsdb.integrations.libs.storage_handler import SqliteStorageHandler
from mindsdb.integrations.mysql_handler.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.utilities.config import Config
from mindsdb import __version__ as mindsdb_version
from mindsdb.utilities.functions import mark_process
import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import Join
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


class LightwoodHandler(PredictiveHandler):
    def __init__(self, name):
        """ Lightwood AutoML integration """  # noqa
        super().__init__(name)
        self.storage = None
        self.parser = parse_sql
        self.dialect = 'mindsdb'
        self.handler_dialect = 'mysql'

    def connect(self, **kwargs) -> Dict[str, int]:
        """ Setup storage handler and check lightwood version """  # noqa
        self.storage = SqliteStorageHandler(context=self.name, config=kwargs['config'])
        return self.check_status()

    def check_status(self) -> Dict[str, int]:
        """ Checks that the connection is, as expected, an MlflowClient instance. """  # noqa
        # todo: potentially nothing to do here, as we can assume user to install requirements first
        try:
            import lightwood
            year, major, minor, hotfix = lightwood.__version__.split('.')
            assert int(year) >= 22
            assert int(major) >= 2
            assert int(minor) >= 3
            print("Lightwood OK!")
            return {'status': '200'}
        except AssertionError as e:
            print("Cannot import lightwood!")
        return {'status': '503', 'error': e}

    def get_tables(self) -> List:
        """ Returns list of model names (that have been succesfully linked with CREATE PREDICTOR) """  # noqa
        models = self.storage.get('models')
        return list(models.keys()) if models else []

    def describe_table(self, table_name: str) -> Dict:
        """ For getting standard info about a table. e.g. data types """  # noqa
        if table_name not in self.get_tables():
            print("Table not found.")
            return {}

        model = self.storage.get('models')[model_name]
        return model['jsonai']

    def run_native_query(self, query_str: str) -> Optional[object]:
        statement = self.parser(query_str, dialect=self.dialect)

        if type(statement) == CreatePredictor:
            model_name = statement.name.parts[-1]

            # check that it exists within mlflow and is not already registered
            if model_name in self.get_tables():
                print("Error: this model is already registered!")
            else:
                target = statement.targets[0].parts[-1]  # TODO: multiple target support?
                params = {
                    'target': target,
                }
                # TODO: insert all USING specs here
                using_params = {}

                # get training data from other integration
                # todo: MDB needs to expose all available handlers through some sort of global state
                handler = MDB_CURRENT_HANDLERS[str(statement.integration_name)]
                handler_query = self.parser(statement.query_str, dialect=self.handler_dialect)
                records = handler.select_query(targets=handler_query.targets, from_stmt=handler_query.from_table, where_stmt=handler_query.where)
                df = pd.DataFrame.from_records(records)[:10]
                jsonai = json_ai_from_problem(df, ProblemDefinition.from_dict(params))

                # todo inject with all params in using clause here
                code = code_from_json_ai(jsonai)
                predictor = predictor_from_code(code)
                predictor.learn(df)
                serialized_predictor = dill.dumps(predictor)

                all_models = self.storage.get('models')
                payload = {'jsonai': jsonai, 'predictor': serialized_predictor, 'code': code}
                if all_models is not None:
                    all_models[model_name] = payload
                else:
                    all_models = {model_name: payload}
                self.storage.set('models', all_models)

        elif type(statement) == DropPredictor:
            to_drop = statement.name.parts[-1]
            all_models = self.storage.get('models')
            del all_models[to_drop]
            self.storage.set('models', all_models)

        else:
            raise Exception(f"Query type {type(statement)} not supported")

    def select_query(self, stmt) -> pd.DataFrame:
        _, _, target, model_url = self._get_model(stmt)

        if target not in [str(t) for t in stmt.targets]:
            raise Exception("Predictor will not be called, target column is not specified.")

        df = pd.DataFrame.from_dict({stmt.where.args[0].parts[0]: [stmt.where.args[1].value]})
        return self._call_model(df, model_url)

    def join(self, stmt, data_handler: BaseHandler, into: Optional[str] = None) -> pd.DataFrame:
        """
        Batch prediction using the output of a query passed to a data handler as input for the model.
        """  # noqa

        # tag data and predictive handlers
        if len(stmt.from_table.left.parts) == 1:
            model_clause = 'left'
            data_clause = 'right'
        else:
            model_clause = 'right'
            data_clause = 'left'
        model_alias = str(getattr(stmt.from_table, model_clause).alias)

        # get model input
        data_handler_table = getattr(stmt.from_table, data_clause).parts[
            -1]  # todo should be ".".join(...) if data handlers support more than one table
        data_handler_cols = list(set([t.parts[-1] for t in stmt.targets]))

        data_query = f"""SELECT {','.join(data_handler_cols)} FROM {data_handler_table}"""
        if stmt.where:
            data_query += f" WHERE {str(stmt.where)}"
        if stmt.limit:
            # todo integration should handle this depending on type of query... e.g. if it is TS, then we have to fetch correct groups first and limit later
            data_query += f" LIMIT {stmt.limit.value}"

        parsed_query = self.parser(data_query, dialect=self.dialect)
        model_input = pd.DataFrame.from_records(
            data_handler.select_query(
                parsed_query.targets,
                parsed_query.from_table,
                parsed_query.where
            ))

        # rename columns
        aliased_columns = list(model_input.columns)
        for col in stmt.targets:
            if str(col.parts[0]) != model_alias and col.alias is not None:
                # assumes mdb_sql will alert if there are two columns with the same alias
                aliased_columns[aliased_columns.index(col.parts[-1])] = str(col.alias)
        model_input.columns = aliased_columns

        # get model output
        _, _, _, model_url = self._get_model(stmt)
        predictions = self._call_model(model_input, model_url)

        # rename columns
        aliased_columns = list(predictions.columns)
        for col in stmt.targets:
            if col.parts[0] == model_alias and col.alias is not None:
                aliased_columns[aliased_columns.index('prediction')] = str(col.alias)
        predictions.columns = aliased_columns

        if into:
            try:
                data_handler.select_into(into, predictions)
            except Exception as e:
                print("Error when trying to store the JOIN output in data handler.")

        return predictions

    def _get_model(self, stmt):
        if type(stmt.from_table) == Join:
            model_name = stmt.from_table.right.parts[-1]
        else:
            model_name = stmt.from_table.parts[-1]

        mlflow_models = [model.name for model in self.connection.list_registered_models()]
        if not model_name in self.get_tables():
            raise Exception("Error, not found. Please create this predictor first.")
        elif not model_name in mlflow_models:
            raise Exception(
                "Cannot connect with the model, it might not served. Please serve it with MLflow and try again.")

        model = self.connection.get_registered_model(model_name)
        model_info = self.storage.get('models')[model_name]
        return model_name, model, model_info['target'], model_info['url']

    def _call_model(self, df, model_url):
        resp = requests.post(model_url,
                             data=df.to_json(orient='records'),
                             headers={'content-type': 'application/json; format=pandas-records'})
        answer: List[object] = resp.json()

        predictions = pd.DataFrame({'prediction': answer})
        out = df.join(predictions)
        return out


if __name__ == '__main__':
    # TODO: turn this into tests

    MDB_CURRENT_HANDLERS = {
        'mysql_handler': MySQLHandler('test_handler', **{
            "host": "localhost",
            "port": "3306",
            "user": "root",
            "password": "root",
            "database": "test",
            "ssl": False
        })
    }
    print(MDB_CURRENT_HANDLERS['mysql_handler'].check_status())

    cls = LightwoodHandler('LWtest')
    config = Config()
    print(cls.connect(config={'path': config['paths']['root'], 'name': 'lightwood_handler.db'}))

    try:
        print('dropping predictor...')
        cls.run_native_query(f"DROP PREDICTOR {registered_model_name}")
    except:
        print('failed to drop')
        pass

    print(cls.get_tables())
    model_name = 'lw_test_predictor'
    if model_name not in cls.get_tables():
        query = f"CREATE PREDICTOR {model_name} FROM mysql_handler (SELECT * FROM test.home_rentals_subset ) PREDICT rental_price"
        cls.run_native_query(query)

    print(cls.describe_table(f'{model_name}'))
    #
    # # Tests with MySQL handler: JOIN
    # from mindsdb.integrations.mysql_handler.mysql_handler.mysql_handler import \
    #     MySQLHandler  # expose through parent init
    #
    # kwargs = {
    #     "host": "localhost",
    #     "port": "3306",
    #     "user": "root",
    #     "password": "root",
    #     "database": "test",
    #     "ssl": False
    # }
    # sql_handler_name = 'test_handler'
    # data_table_name = 'train_escaped_csv'  # 'tweet_sentiment_train'
    # handler = MySQLHandler(sql_handler_name, **kwargs)
    # assert handler.check_status()
    #
    # query = f"SELECT target from {registered_model_name} WHERE text='This is nice.'"
    # parsed = cls.parser(query, dialect=cls.dialect)
    # predicted = cls.select_query(parsed)
    #
    # into_table = 'test_join_into_mlflow'
    # query = f"SELECT tb.target as predicted, ta.target as real, tb.text from {sql_handler_name}.{data_table_name} AS ta JOIN {registered_model_name} AS tb LIMIT 10"
    # parsed = cls.parser(query, dialect=cls.dialect)
    # predicted = cls.join(parsed, handler, into=into_table)
    #
    # # checks whether `into` kwarg does insert into the table or not
    # q = f"SELECT * FROM {into_table}"
    # qp = cls.parser(q, dialect='mysql')
    # assert len(handler.select_query(qp.targets, qp.from_table, qp.where)) > 0
    #
    # try:
    #     handler.run_native_query(f"DROP TABLE test.{into_table}")
    # except:
    #     pass
