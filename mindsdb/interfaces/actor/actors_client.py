
import socketio 
import traceback
import uuid
import random
import logging

server_url = None


class ActorsClient:
    


    def __init__(self, url):
        sio = socketio.Client()
        self.instances = {}
        self._sio = sio
        #self.emit = sio.emit

        @sio.event
        def connect():
            print('connection established')

        @sio.event
        def call_method(data):
            try:
                ret = getattr(self.instances[data["id"]], data["method"])(**data["args"])
                sio.emit('on_call_response', {'call_id':data['call_id'], 'response': ret})
            except Exception:
                sio.emit( 'on_call_response', {'call_id':data['call_id'], 'error': str(traceback.format_exc()) } )

        @sio.event
        def disconnect():
            print('disconnected from server')

        @sio.event
        def new(data):
            self.instances[data["id"]] = self.class_def(**data["args"])
        

        
        logging.info('Connecting to Server')
        sio.connect(url)

        

        logging.info('Connected to Server')

    def _emit(self):
        print ('aaa')
        #self.sio.emit(endpoint, data)

    def __getattribute__(self, __name: str):

        if __name[0] == '_': 
            return
        
        print(__name)
        id = random.randint(1,100000000)
        class_name = __name
        sio = self._sio

        #self.
        
        class ActorObject:

            def __init__(selfb, **kargs):
                payload = {'name': class_name, 'id':id, 'args': kargs }
                logging.info('Registering Remote Object', payload)
                print(sio)
                sio.emit('register_client', payload)

            def __getattribute__(selfb, method_name: str):

                def caller(**kwargs):
                    payload = {'name': __name, 'id':id, 'method':method_name, 'args': kwargs }
                    logging.info('Calling Remote Method', payload)
                    #sio_client.emit('call_method', payload)

                
                return caller

        return ActorObject
    
    

        
        


    



actors = ActorsClient('http://localhost:5000')

    
a = actors.A()

a = actors.B()
# a.add()
# b = a.get()
