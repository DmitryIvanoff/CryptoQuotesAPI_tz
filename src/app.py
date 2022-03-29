from asyncio.locks import Event
from fastapi import FastAPI, responses
from traceback import format_exc
import databases
import logging
import settings

from models import create_all, check_data, drop_all, TABLES, fetch_data, create_view
from schemas import Granularity, Pair, CandleOut

logger = logging.getLogger("app")

app = FastAPI(debug=settings.DEBUG)

db = databases.Database(settings.DATABASE_URL)


@app.on_event("startup")
async def startup_event():
    logger.info("startup create db if not exist and fill if not filled")
    event = Event()
    event.clear()
    await db.connect()
    try:
        for t in TABLES:
            await db.execute(f"SELECT * FROM {TABLES[t].name}")
            await create_view(db, TABLES[t])
        event.set()
    except Exception as e:
        logger.error(format_exc(limit=1))
        await create_all(db, event)
    finally:
        if not event.is_set():
            logger.debug("waiting for db")
            await event.wait()
        await check_data(db)


@app.on_event("shutdown")
async def shutdown_event():
    await db.disconnect()


async def get_db():
    async with databases.Database(settings.DATABASE_URL) as db:
        yield db


@app.get(r"/candles/{pair:path}", response_model=list[CandleOut])
async def get_candles(
    pair: Pair,
    by: Granularity = Granularity.by_hour,
):
    result = await fetch_data(db, pair, by)
    logger.debug(result)
    return result


@app.get("/index")
async def root():
    return responses.HTMLResponse("<h>app</h>")
