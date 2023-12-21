#!/usr/bin/env python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from langchain.prompts import ChatPromptTemplate
from langchain.chat_models import  ChatOpenAI
from langserve import add_routes
import os

os.environ["OPENAI_API_KEY"] = "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"

app = FastAPI(
  title="LangChain Server",
  version="1.0",
  description="A simple api server using Langchain's Runnable interfaces",
)


model = ChatOpenAI(openai_api_key=os.environ["OPENAI_API_KEY"])

prompt = ChatPromptTemplate.from_template("You are helpful assistant please answer questions.  { query }")

add_routes(
        app,
        prompt | model,
        path="/chat",
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="localhost", port=8001)

