import uvicorn 
from app import app
import os

HOST = os.environ.get('HOST') 
PORT = int(os.environ.get('PORT')) 

if __name__ == "__main__": 
    # uvicorn.run(app, host=HOST, port=PORT)
    uvicorn.run('app:app', host=HOST, port=PORT, reload=True)