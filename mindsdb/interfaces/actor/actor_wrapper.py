
import socketio 
import traceback



server_url = None


class ActorWrapper:
    


    def __init__(self, url, class_def):
        sio = socketio.Client()
        self.instances = {}
        self.sio = sio
        self.emit = sio.emit
        self.class_def = class_def

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
        

        
        
        sio.connect(url)
        sio.emit('register', {'name': class_def.__name__})
        

    
    
    
        
        


    

def make_actor(class_def = None):
    channel = ActorWrapper('http://localhost:5000', class_def)
    
    
    
@make_actor
class A:
    def __init__(self):
        self.a = 0
        pass

    def add(self, num):
        self.a += num
    
    def get(self):
        return self.a
    



    
    