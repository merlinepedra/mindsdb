import mysql.connector
from contextlib import closing

from mindsdb.integrations.libs.base_integration import BaseIntegration

class MySQLHandler(BaseIntegration):

    def __init__(self, config, name, **kwargs):
        super().__init__(config, name)
        self.connection = None
        self.mysql_url = None

        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.password = kwargs.get('password')
        self.ssl = kwargs.get('ssl')
        self.ssl_ca = kwargs.get('ssl_ca')
        self.ssl_cert = kwargs.get('ssl_cert')
        self.ssl_key = kwargs.get('ssl_key')

    def connect(self):
        config = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password
        }
        if self.ssl is True:
            config['client_flags'] = [mysql.connector.constants.ClientFlag.SSL]
            if self.ssl_ca is not None:
                config["ssl_ca"] = self.ssl_ca
            if self.ssl_cert is not None:
                config["ssl_cert"] = self.ssl_cert
            if self.ssl_key is not None:
                config["ssl_key"] = self.ssl_key

        self.connection = mysql.connector.connect(**config)
        self._setup()
        return self.connection

    def check_status(self):
        try:
            con = self.connection
            with closing(con) as con:
                connected = con.is_connected()
        except Exception:
            connected = False
        return connected

    def run_native_query(self, query_str):
        if not self.check_status():
            con = self.connect()
        try:
            with closing(con) as con:
                cur = con.cursor(dictionary=True, buffered=True)
                cur.execute(query_str)
                res = True
                try:
                    res = cur.fetchall()
                except Exception:
                    pass
                con.commit()
        except Exception as e:
            # TODO: reformat to something cleaner
            raise Exception(f"{e}\nError: something is wrong with your MySQL connection. Please check and retry!")
        return res

    def get_tables(self):
        q = "SHOW TABLES;"
        result = self.run_native_query(q)
        return result

    def get_views(self):
        q = "SHOW VIEWS;"
        result = self.run_native_query(q)
        return result

    def select_query(self,
                     from_object,
                     where # ,  # assume everything here is AND clause
                     # session  # TODO: rm because of merger with datahubs
                     ):
        """ Here you can inter-operate betweens integrations. """
        # if 'PREDICT' in query:  # <- idea
        #     for column in select:
        #         predict(column, df)  # multiple outputs for multiple predictors
        #
        # self.select_query(from_object, where, session)

        self.run_native_query()

    def join(self, left_integration_instance, left_where, on=None):
        # Can join either:
        #   - another DS
        #   - an ML model
        if not on:
            on = '*'
        pass

    def select_into(self, integration_instance, stmt):
        pass

    def describe_table(self, table_name):
        """ For getting standard info about a table. e.g. data types """
        q = f"DESCRIBE {table_name};"
        result = self.run_native_query(q)
        return result
