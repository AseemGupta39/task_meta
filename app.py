from fastapi import FastAPI,HTTPException
from controllers.controller import router as processor_router
from pydantic import ValidationError
from models import schemas

app = FastAPI(title="File Processing API")

app.include_router(processor_router)