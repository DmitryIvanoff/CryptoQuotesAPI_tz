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
    PrimaryKeyConstraint,
    select,
    insert,
    update,
)
from schemas import Granularity, Exchange, Pair, Candle
from markets import load_data

logger = logging.getLogger("app")
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
    if candles:
        await db.execute(table.insert().values([candle.dict() for candle in candles]))


async def fetch_data(
    db: databases.Database, pair: Pair, interval: Granularity = Granularity.by_hour
) -> list[Candle]:
    table: Table = TABLES[(pair, interval)]
    return await db.fetch_all(table.select().order_by(table.c.since.desc()))


async def check_data(db: databases.Database):
    logger.info("checking data...")
    # todo: попробовать поиграться с очередностью
    #  загрузки данных с бирж в БД
    for interval in Granularity:
        for pair in Pair:
            for exchange in Exchange:
                logger.debug(exchange)
                table = TABLES[(pair, interval)]
                result: Candle | None = await db.fetch_one(
                    table.select()
                    .where(table.c.exchange == exchange)
                    .order_by(table.c.since.desc())
                )
                logger.info(result)
                if not result:
                    if interval == Granularity.by_hour:
                        start_since = datetime.datetime.now() - datetime.timedelta(
                            days=30
                        )
                    elif interval == Granularity.by_minute:
                        start_since = datetime.datetime.now() - datetime.timedelta(
                            days=1
                        )

                    candles = await load_data(
                        exchange,
                        pair,
                        start_since,
                        interval,
                    )
                    logger.debug(candles)
                    await load_data_to_db(db, table, candles)
                else:
                    start_since = result.since
                    candles = await load_data(
                        exchange,
                        pair,
                        start_since,
                        interval,
                    )
                    candles = [candle for candle in candles if candle.since != start_since]
                    if not candles:
                       continue

                    logger.debug(candles)
                    await load_data_to_db(db, table, candles)
