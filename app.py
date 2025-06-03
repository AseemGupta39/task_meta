from fastapi import FastAPI,HTTPException
from controllers.controller import router as processor_router

app = FastAPI(title="File Processing API")

app.include_router(processor_router)