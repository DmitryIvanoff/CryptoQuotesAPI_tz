import datetime
import asyncio
import aiohttp
from schemas import Granularity, Exchange, Pair, Candle
import logging

logger = logging.getLogger("app")
MARKETS = {
    "KRAKEN": {
        "endpoint": "https://api.kraken.com/0/public/OHLC",
        "pairs": {
            "BTC/USD": "XXBTZUSD",
            "ETH/USD": "XETHZUSD",
            "XRP/EUR": "XXRPZEUR",
            "XRP/USD": "XXRPZUSD",
        },
        "intervals": {"hour": 60, "minute": 1},
    },
    # "BITFINEX": ""
}


async def load_data(
    exchange: Exchange,
    pair: Pair,
    since: datetime.datetime,
    interval: Granularity | None = Granularity.by_hour,
) -> list[Candle]:
    market_pair = MARKETS[exchange.value]["pairs"][pair.value]
    if interval:
        interval = MARKETS[exchange.value]["intervals"][interval.value]
    logger.info(
        f'loading data for {exchange} "{pair}" since {since} for {interval}m...'
    )
    params = {
        "pair": MARKETS[exchange.value]["pairs"][pair.value],
        "since": int(since.timestamp()),
    }
    params.update({"interval": interval}) if interval else None
    candles = []
    async with aiohttp.client.ClientSession() as session:
        async with session.get(
            MARKETS[exchange]["endpoint"], params=params
        ) as response:
            resp = await response.json()
            # logger.debug(resp)
            if e := resp["error"]:
                logger.error(e)
                return candles
            result = resp["result"]
            data = result[market_pair]
            last = result["last"]
            for since, open, high, low, close, _, volume, trades in data:
                candles.append(
                    Candle(
                        since=datetime.datetime.fromtimestamp(since),
                        open=open,
                        high=high,
                        low=low,
                        close=close,
                        volume=volume,
                        trades=trades,
                        pair=pair,
                        exchange=exchange,
                    )
                )

    return candles
