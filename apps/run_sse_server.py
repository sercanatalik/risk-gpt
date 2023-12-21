from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.encoders import jsonable_encoder
import asyncio

from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import duckdb
from pyarrow import Table
import orjson
import logging
import polars as pl

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
#

def schema_to_json(schema):
    mapping ={
        'Utf8':'string',
        'Int64':'integer',
        'Float64':'float',
        'Boolean':'boolean',
        'Date32':'date',
        'Datetime':'datetime',
        'Time64':'time',

    }
    _schema = {}
    for i in schema:
        _schema[i]= mapping[str(schema[i].base_type())]

    return _schema

@app.get("/stream")
async def stream(request: Request,userId: str = None) -> StreamingResponse:
    df = duckdb.read_parquet('..//data//risk//*.parquet').fetchdf()
    dl = pl.from_pandas(df)
    async def event_stream():
        _response = orjson.dumps([{'userId': userId,'totalCount':df.size,'schema':schema_to_json(dl.schema)}]).decode("utf8")
        yield "data: " + _response + "\n\n"
        await asyncio.sleep(1)
        for idx, frame in enumerate(dl.iter_slices(n_rows=200)):
            _response = orjson.dumps(jsonable_encoder(frame.to_dicts())).decode("utf8")
            yield "data: " + _response + "\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(event_stream(),media_type="text/event-stream")


@app.get("/")
async def index():
    # Example: Create a PyArrow table (replace with your data source)

    return {'ok': 'ok'}


if __name__ == "__main__":
    logging.critical("Listening on http://localhost:8080")
    uvicorn.run(app, host="localhost", port=8080)
