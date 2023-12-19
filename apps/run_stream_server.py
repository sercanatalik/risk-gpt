import asyncio
import os
import logging
import threading
import uvicorn

from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from perspective import Table, PerspectiveManager, PerspectiveStarletteHandler
import duckdb
import tornado.ioloop
import concurrent.futures


here = os.path.abspath(os.path.dirname(__file__))
file_path = os.path.join(
    here, "..", "data",  "risk.arrow"
)

IS_MULTI_THREADED = True



def readUpdates(lastUpdatedAt):
    return duckdb.execute("SELECT * FROM read_parquet('..//data//risk//*.parquet') WHERE lastUpdatedAt > ?",[lastUpdatedAt]).fetchdf()

def perspective_thread(manager):

    df = duckdb.read_parquet('..//data//risk//*.parquet').fetchdf()
    global lastUpdatedAt
    lastUpdatedAt =  df["lastUpdatedAt"].max()
    table = Table(df,index='id')
    manager.host_table("risk",table)

    def updater():
        global lastUpdatedAt

        df = readUpdates(lastUpdatedAt)
        if df.size>0:
            table.update(df)
            print('updated',df.size)
            lastUpdatedAt = df['lastUpdatedAt'].max()


    callback = tornado.ioloop.PeriodicCallback(callback=updater,  callback_time=100)

    psp_loop = tornado.ioloop.IOLoop()
    if IS_MULTI_THREADED:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            manager.set_loop_callback(psp_loop.run_in_executor, executor)
            callback.start()
            psp_loop.start()
    else:
        manager.set_loop_callback(psp_loop.add_callback)
        callback.start()
        psp_loop.start()




def make_app():
    manager = PerspectiveManager()

    thread = threading.Thread(target=perspective_thread, args=(manager,))
    thread.daemon = True
    thread.start()

    async def websocket_handler(websocket: WebSocket):
        handler = PerspectiveStarletteHandler(manager=manager, websocket=websocket)
        await handler.run()

    app = FastAPI()
    app.add_api_websocket_route("/ws", websocket_handler)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return app

if __name__ == "__main__":
    app = make_app()
    logging.critical("Listening on http://localhost:8080")
    uvicorn.run(app, host="localhost", port=8080)


