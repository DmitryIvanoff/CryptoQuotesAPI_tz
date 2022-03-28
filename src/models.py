import datetime
import logging
import aiohttp.client
import databases
import asyncio
from sqlalchemy.schema import CreateTable, CreateIndex, Index, DropTable, DropIndex
from sqlalchemy import (
    Column,
    Integer,
    DateTime,
    Float,
    Enum,
    Table,
    MetaData,
    select,
    insert,
    update,
)
from schemas import Granularity, Exchange, Pair, Candle
from markets import load_data

logger = logging.getLogger("app")
logger.setLevel("DEBUG")
metadata = MetaData()

HourCandle = Table(
    "hourcandles",
    metadata,
    Column("since", DateTime, primary_key=True, index=True),
    Column("open", Float),
    Column("high", Float),
    Column("low", Float),
    Column("close", Float),
    Column("volume", Float),
    Column("trades", Integer, nullable=True),
    Column("pair", Enum(Pair)),
    Column("exchange", Enum(Exchange)),
)

MinuteCandle = Table(
    "minutecandles",
    metadata,
    Column("since", DateTime, primary_key=True, index=True),
    Column("open", Float),
    Column("high", Float),
    Column("low", Float),
    Column("close", Float),
    Column("volume", Float),
    Column("trades", Integer, nullable=True),
    Column("pair", Enum(Pair)),
    Column("exchange", Enum(Exchange)),
)

TABLES = {"hour": HourCandle, "minute": MinuteCandle}


async def drop_all(db: databases.Database):
    logger.info("dropping tables...")
    async with db.transaction():
        for table in TABLES:
            t = TABLES[table]
            query = DropTable(t, if_exists=True)
            logger.debug(query)
            await db.execute(query)
            for index in t.indexes:
                query = DropIndex(index, if_exists=True)
                logger.debug(query)
                await db.execute(query)


async def create_all(db: databases.Database, event: asyncio.Event | None = None):
    logger.info("creating tables...")

    logger.debug(TABLES)
    async with db.transaction():
        for table in TABLES:
            t = TABLES[table]
            logger.debug(t)
            query = CreateTable(t, if_not_exists=True)
            logger.debug(query)
            await db.execute(query)
            for index in t.indexes:
                query = CreateIndex(index)
                logger.debug(query)
                try:
                    await db.execute(query)
                except Exception as e:
                    logger.exception(e)
    if event is not None:
        logger.debug("event is set")
        event.set()


async def load_data_to_db(db: databases.Database, table: Table, candles: list[Candle]):
    await db.execute(table.insert().values([candle.dict() for candle in candles]))


async def fetch_data(
    db: databases.Database, pair: Pair, interval: Granularity = Granularity.by_hour
) -> list[Candle]:
    table: Table = TABLES[interval.value]
    return await db.fetch_all(table.select().where(table.c.pair == pair))


async def check_data(db: databases.Database):
    logger.info("checking data...")
    for interval in Granularity:
        table = TABLES[interval.value]
        for pair in Pair:
            for exchange in Exchange:
                logger.debug(exchange)
                if exchange == Exchange.bitfinex:
                    continue
                result = await db.fetch_one(
                    table.select()
                    .where(table.c.exchange == exchange)
                    .order_by(table.c.since.desc())
                )
                logger.info(result)
                if not result:
                    candles = await load_data(
                        exchange,
                        pair,
                        datetime.datetime.now() - datetime.timedelta(days=30),
                        interval,
                    )
                    logger.debug(candles)
                    await load_data_to_db(db, table, candles)
                else:
                    # todo: load_data
                    pass
