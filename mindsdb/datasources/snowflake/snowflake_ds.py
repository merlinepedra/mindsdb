from typing_extensions import runtime
import ray
import logging
import argparse
import atexit
from threading import Event
import snowflake

class DS:
    def __init__(self):
        self.ctx = snowflake.connector.connect(
            user='<user_name>',
            password='<password>',
            account='<account_identifier>'
            )
        

    def query(self, query):
        cs = self.ctx.cursor()
        try:
            cs.execute("SELECT current_version()")
            one_row = cs.fetchone()
            print(one_row[0])
        except:
            return 'aaa'
            cs.close()

@ray.remote
class SnowflakeDS():

    
    def __init__(self):
        #ray.shutdown()
        pass

    

    def connect(self):
        return DS()
        ctx = snowflake.connector.connect(
            user='<user_name>',
            password='<password>',
            account='<account_identifier>'
            )
        cs = ctx.cursor()

        try:
            cs.execute("SELECT current_version()")
            one_row = cs.fetchone()
            print(one_row[0])
        except:
            return 'aaa'
            cs.close()
        ctx.close()

    @staticmethod
    def start(server, port):
        runtime_env={"pip": "requirements.txt"}
        ray.init(address='{server}:{port}'.format(server=server, port=port), namespace="snowflake", runtime_env=runtime_env)
        return SnowflakeDS.options(name="SnowflakeDS").remote()
        
        

    @staticmethod
    def stop(actor_handle):
        def killit():
            print('shutting down')
            ray.kill(actor_handle)
            ray.shutdown()
        return killit
        
    
        
        
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--action', help='start or stop')
    parser.add_argument('--server', default='127.0.0.1', help='server address (optional)')
    parser.add_argument('--port', help='the port number')
    
    args = parser.parse_args()

    if args.action == 'start':
        if not args.port:
            exit('ERROR: To start this cluster you must specify a port')
            
        snowflake_actor = SnowflakeDS.start(server = args.server, port = args.port)
        atexit.register(SnowflakeDS.stop(snowflake_actor))
        Event().wait()
    
        
    
    

