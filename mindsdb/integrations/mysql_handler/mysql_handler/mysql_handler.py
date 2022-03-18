import mysql.connector
from contextlib import closing

from mindsdb_sql import parse_sql
from mindsdb.integrations.libs.base_handler import DatabaseHandler


class MySQLHandler(DatabaseHandler):

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.connection = None
        self.mysql_url = None
        self.parser = parse_sql
        self.dialect = 'mysql'
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.user = kwargs.get('user')
        self.database = kwargs.get('database')  # may want a method to change active DB
        self.password = kwargs.get('password')
        self.ssl = kwargs.get('ssl')
        self.ssl_ca = kwargs.get('ssl_ca')
        self.ssl_cert = kwargs.get('ssl_cert')
        self.ssl_key = kwargs.get('ssl_key')
        self.connect()

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
            self.connect()
        try:
            with closing(self.connection) as con:
                cur = con.cursor(dictionary=True, buffered=True)
                cur.execute(f"USE {self.database};")
                cur.execute(query_str)
                res = True
                try:
                    res = cur.fetchall()
                except Exception:
                    pass
                con.commit()
        except Exception as e:
            raise Exception(f"Error: {e}. Please check and retry!")
        return res

    def get_tables(self):
        q = "SHOW TABLES;"
        result = self.run_native_query(q)
        return result

    def get_views(self):
        q = f"SHOW FULL TABLES IN {self.database} WHERE TABLE_TYPE LIKE 'VIEW';"
        result = self.run_native_query(q)
        return result

    def describe_table(self, table_name):
        """ For getting standard info about a table. e.g. data types """
        q = f"DESCRIBE {table_name};"
        result = self.run_native_query(q)
        return result

    def select_query(self, stmt):
        # TODO: discuss this interface. Having original (only from and where) will be limiting for queries with more parts (e.g. limit)
        query = f"SELECT {','.join([t.__str__() for t in stmt.targets])} FROM {stmt.from_table.parts[0]}"
        if stmt.where:
            query += f" WHERE {str(stmt.where)}"

        result = self.run_native_query(query)
        return result

    def select_into(self, table_name, select_query):
        # TODO: rework this to intake a parsed query
        query = f"CREATE TABLE {self.database}.{table_name} AS ({select_query})"
        result = self.run_native_query(query)

    def join(self, left_integration_instance, left_where, on=None):
        # For now, can only join tables that live within the specified DB
        if not on:
            on = '*'
        pass


if __name__ == '__main__':
    kwargs = {
        "host": "localhost",
        "port": "3306",
        "user": "root",
        "password": "root",
        "database": "test",
        "ssl": False
    }
    handler = MySQLHandler('test_handler', **kwargs)
    assert handler.check_status()

    dbs = handler.run_native_query("SHOW DATABASES;")
    assert isinstance(dbs, list)

    tbls = handler.get_tables()
    assert isinstance(tbls, list)

    views = handler.get_views()
    assert isinstance(views, list)

    try:
        handler.run_native_query("CREATE TABLE test_mdb (test_col INT);")
    except Exception:
        pass # already exists

    described = handler.describe_table("test_mdb")
    assert isinstance(described, list)

    query = "SELECT * FROM test_mdb WHERE 'id'='a'"
    parsed = handler.parser(query, dialect=handler.dialect)
    result = handler.select_query(parsed)

    try:
        result = handler.run_native_query("DROP TABLE test_mdb2")
    except:
        pass
    result = handler.select_into('test_mdb2', "SELECT * FROM test_mdb")