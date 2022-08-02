import asyncio
import datetime
import time
from pprint import pformat


import aiohttp.client
import databases

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    Float,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    func,
    select,
    text,

)
from sqlalchemy.sql import quoted_name
from sqlalchemy.schema import CreateIndex, CreateTable, DropIndex, DropTable, Index

from markets import load_data
from schemas import Candle, CandleOut, Exchange, Granularity, Pair
from settings import logger

metadata = MetaData()

TABLES = {
    (pair, interval): Table(
        f"{str(pair.value).replace('/', '')}_{interval.value}_candles",
        metadata,
        Column("since", DateTime),
        Column("open", Float),
        Column("high", Float),
        Column("low", Float),
        Column("close", Float),
        Column("volume", Float),
        Column("trades", Integer, nullable=True),
        Column("exchange", Enum(Exchange)),
        PrimaryKeyConstraint("since", "exchange"),
        Index(
            f"{str(pair.value).replace('/', '')}_{interval.value}_candles_since_exchange_idx",
            "since",
            "exchange",
        ),
    )
    for pair in Pair
    for interval in Granularity
}


async def drop_all(db: databases.Database):
    logger.info("dropping tables...")
    async with db.transaction():
        for table in TABLES:
            t = TABLES[table]
            query = DropTable(t, if_exists=True)
            logger.debug(query)
            await db.execute(query)
            for index in t.indexes:
                query = DropIndex(index)
                logger.debug(query)
                await db.execute(query)


async def create_view(db: databases.Database, table: Table):
    query = f"""CREATE VIEW IF NOT EXISTS "{table.name}_max_min_daily_view" AS {select(
        func.max(table.c.high).label("max_high"), func.min(table.c.low).label("min_low")
    ).group_by(text(f'date("{table.name}".since)'))}"""
    logger.debug(query)
    await db.execute(query)


async def create_all(db: databases.Database, event: asyncio.Event | None = None):
    logger.info("creating tables...")

    logger.debug(TABLES)
    coros = []
    async with db.transaction():
        for table in TABLES:
            coros.append(create_table(db, table))
        await asyncio.gather(*coros)
    if event is not None:
        logger.debug("event is set")
        event.set()


async def create_table(db: databases.Database, table: Table) -> None:
    t = TABLES[table]
    logger.debug(t)
    query = CreateTable(t, if_not_exists=True)
    logger.debug(query)
    await db.execute(query)
    await create_view(db, t)
    for index in t.indexes:
        query = CreateIndex(index)
        logger.debug(query)
        try:
            await db.execute(query)
        except Exception as e:
            logger.exception(e)


async def load_candles_to_table(
        db: databases.Database, table: Table, candles: list[Candle]
) -> None:
    if candles:
        await db.execute(table.insert().values([candle.dict() for candle in candles]))


async def fetch_data(
        db: databases.Database, pair: Pair, interval: Granularity = Granularity.by_hour
) -> list[CandleOut]:
    table: Table = TABLES[(pair, interval)]

    escaped_table = quoted_name(table.name, False)
    # raw sql version with view
    query = f"""
    SELECT t.since AS time, t.open, t.high, t.low, 
    t.close, t.volume, t.exchange, (t.low = view.min_low) AS type
    FROM {escaped_table} as t JOIN {escaped_table}_max_min_daily_view AS "view" ON 
    t.high = view.max_high OR t.low = view.min_low ORDER BY t.since DESC;
    """
    logger.debug(query)
    logger.debug(f'table name: {table.name}')
    return await db.fetch_all(query)


async def check_data(db: databases.Database) -> None:
    logger.info("checking data...")
    started_at = time.perf_counter()
    futures = []
    async with aiohttp.client.ClientSession() as session:
        for interval in Granularity:
            for pair in Pair:
                for exchange in Exchange:
                    futures.append(
                        load_from_exchange_to_db(
                            db, exchange, interval, pair, session
                        )
                    )
        await asyncio.gather(*futures)
    logger.info(f"elapsed: {time.perf_counter() - started_at:5.2}s")
    await logger.complete()


async def load_from_exchange_to_db(
        db,
        exchange,
        interval,
        pair,
        session: aiohttp.client.ClientSession,
) -> None:
    table = TABLES[(pair, interval)]
    result: Candle | None = await db.fetch_one(
        table.select()
        .where(table.c.exchange == exchange)
        .order_by(table.c.since.desc())
    )
    logger.debug(f"{exchange=}:\n{result=}")
    if not result:
        if interval == Granularity.by_hour:
            start_since = datetime.datetime.now() - datetime.timedelta(days=30)
        elif interval == Granularity.by_minute:
            start_since = datetime.datetime.now() - datetime.timedelta(days=1)
        else:
            start_since = datetime.datetime.now() - datetime.timedelta(hours=1)
    else:
        start_since = result.since

    try:
        candles = await load_data(
            exchange,
            pair,
            start_since,
            session,
            interval,
        )
    except Exception as e:
        logger.exception(e)
        return

    # filter candles
    candles = [candle for candle in candles if candle.since != start_since]
    if not candles:
        return

    logger.opt(lazy=True).debug(pformat(candles, indent=2, width=500))

    await load_candles_to_table(db, table, candles)
