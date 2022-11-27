import logging
from datetime import datetime

from fastapi import FastAPI, BackgroundTasks

from scripts.write_data_to_postgres import main as fetch_and_load_data

app = FastAPI()


logging.basicConfig(level=logging.INFO)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/fetch")
async def fetch_data(background_tasks: BackgroundTasks):
    background_tasks.add_task(fetch_and_load_data)
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")

    return f"Data refresh started {current_time}"
