import asyncio
from asyncio.locks import Event, Lock
from enum import Enum

import fastapi
from fastapi import FastAPI, responses
import databases
import logging
import settings
from traceback import format_exc

# from sqlalchemy.orm
from models import create_all, check_data, drop_all, TABLES, fetch_data
from schemas import Granularity, Exchange, Pair, Candle

logger = logging.getLogger("app")

app = FastAPI(debug=True)


@app.on_event("startup")
async def startup_event():
    logger.info("startup create db if not exist and fill if not filled")
    event = Event()
    event.clear()
    async with databases.Database(settings.DATABASE_URL) as db:
        try:
            for t in TABLES:
                await db.execute(f"SELECT * FROM {TABLES[t].name}")
            event.set()
        except Exception as e:
            logger.error(format_exc(limit=1))
            await create_all(db, event)
        finally:
            if not event.is_set():
                logger.debug("waiting for db")
                await event.wait()
            await check_data(db)


async def get_db():
    async with databases.Database(settings.DATABASE_URL) as db:
        yield db


@app.get(r"/candles/{pair:path}", response_model=list[Candle])
async def get_candles(pair: Pair, by: Granularity = Granularity.by_hour, db: databases.Database = fastapi.Depends(get_db)):
    result = await fetch_data(db, pair, by)
    return result


@app.get("/index")
async def root():
    return responses.HTMLResponse("<h>app</h>")
