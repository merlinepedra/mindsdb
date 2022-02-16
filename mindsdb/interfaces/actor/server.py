from aiohttp import web
import socketio
import uuid

sio = socketio.AsyncServer()
app = web.Application()
sio.attach(app)

actors = {}

instances = {}

async def index(request):
    """Serve the client-side application."""
    with open('static/index.html') as f:
        return web.Response(text=f.read(), content_type='text/html')

@sio.event
def connect(sid, environ):
    print("connect ", sid)

@sio.event
async def register(sid, data):
    print("message ", data)
    actors[data["name"]] = sid

    await sio.emit('new', {"id":'12', "args":{}}, room=actors[data["name"]])
    await sio.emit('init2', {}, room=actors[data["name"]])
    await sio.emit('call_method', {"id":"12", "call_id": "1" , "method":"add", "args":{"num":3}}, room=actors[data["name"]])
    await sio.emit('call_method', {"id":"12", "call_id": "2" ,"method":"get", "args":{}}, room=actors[data["name"]])
    


@sio.event
async def on_call_response(sid, data):
    
    print("message ", data)
    
 

@sio.event
def disconnect(sid):
    print('disconnect ', sid)


app.router.add_static('/static', 'static')
app.router.add_get('/', index)

if __name__ == '__main__':
    web.run_app(app, port=5000)


    #test code
    #snowflake = Actor("SnowflakeDS")() 
    #snowflake.connect(args=1)