from fastapi import FastAPI
from .routers import bytesCalculator, queryParser

app = FastAPI()

app.include_router(bytesCalculator.router)
app.include_router(queryParser.router)

@app.get("/")
async def root():
    return {"message": "Type /docs to access API documentation!"}