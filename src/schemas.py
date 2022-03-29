import enum
import datetime
from pydantic import BaseModel


class Granularity(enum.Enum):
    by_hour = "hour"
    by_minute = "minute"


class Pair(enum.Enum):
    btc_usd = "BTC/USD"
    eth_usd = "ETH/USD"
    xrp_eur = "XRP/EUR"
    xrp_usd = "XRP/USD"


class Exchange(enum.Enum):
    kraken = "KRAKEN"
    bitfinex = "BITFINEX"


class Candle(BaseModel):
    since: datetime.datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int | None
    exchange: Exchange
