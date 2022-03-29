import datetime
import aiohttp
from schemas import Granularity, Exchange, Pair, Candle
import logging

logger = logging.getLogger("app")
MARKETS = {
    "KRAKEN": {
        "endpoint": "https://api.kraken.com/0/public/OHLC",
        "pairs": {
            "BTC/USD": ("XBTUSD", "XXBTZUSD"),
            "ETH/USD": ("ETHUSD", "XETHZUSD"),
            "XRP/EUR": ("XRPEUR", "XXRPZEUR"),
            "XRP/USD": ("XRPUSD", "XXRPZUSD"),
        },
        "intervals": {"hour": 60, "minute": 1},
    },
    "BITFINEX": {
        "endpoint": "https://api-pub.bitfinex.com/v2/candles/trade:{interval}:{pair}/hist",
        "pairs": {
            "BTC/USD": "tBTCUSD",
            "ETH/USD": "tETHUSD",
            "XRP/EUR": "tXRPEUR",
            "XRP/USD": "tXRPUSD",
        },
        "intervals": {"hour": "1h", "minute": "1m"},
    },
}


async def load_data(
    exchange: Exchange,
    pair: Pair,
    since: datetime.datetime,
    interval: Granularity | None = Granularity.by_hour,
) -> list[Candle]:
    candles = []
    if exchange == Exchange.kraken:
        market_pair_in = MARKETS[exchange.value]["pairs"][pair.value][0]
        market_pair_out = MARKETS[exchange.value]["pairs"][pair.value][1]
        if interval:
            interval = MARKETS[exchange.value]["intervals"][interval.value]
        logger.info(
            f'loading data for {exchange} "{pair}" since {since} for {interval}m...'
        )
        params = {
            "pair": market_pair_in,
            "since": int(since.timestamp()),
        }
        params.update({"interval": interval}) if interval else None
        async with aiohttp.client.ClientSession() as session:
            async with session.get(
                MARKETS[exchange.value]["endpoint"], params=params
            ) as response:
                resp = await response.json()
                if e := resp["error"]:
                    logger.error(e)
                    return candles
                result = resp["result"]
                data = result[market_pair_out]
                last = result["last"]
                for since, _open, high, low, close, _, volume, trades in data:
                    candles.append(
                        Candle(
                            since=datetime.datetime.fromtimestamp(since),
                            open=_open,
                            high=high,
                            low=low,
                            close=close,
                            volume=volume,
                            trades=trades,
                            exchange=exchange,
                        )
                    )

    elif exchange == Exchange.bitfinex:
        market_pair = MARKETS[exchange.value]["pairs"][pair.value]
        interval = MARKETS[exchange.value]["intervals"][interval.value]
        params = {"start": since.timestamp() * 1e3, "limit": 10000}
        async with aiohttp.client.ClientSession() as session:
            async with session.get(
                MARKETS[exchange.value]["endpoint"].format(
                    interval=interval, pair=market_pair
                ),
                ssl=False,
                params=params,
            ) as response:
                resp = await response.json()
                data = resp
                if not data:
                    return candles
                for since, _open, close, high, low, volume in data:
                    candles.append(
                        Candle(
                            since=datetime.datetime.fromtimestamp(since / 1000),
                            open=_open,
                            high=high,
                            low=low,
                            close=close,
                            volume=volume,
                            exchange=exchange,
                        )
                    )

    return candles
