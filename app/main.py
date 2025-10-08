from fastapi import FastAPI
from .routers import bytesCalculator
import os
import logging

app = FastAPI()

app.include_router(bytesCalculator.router)

@app.get("/")
async def root():
    return {"message": "Type /docs to access API documentation!"}