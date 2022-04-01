import datetime
import enum

from pydantic import BaseModel, ValidationError, validator


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


class Type(enum.Enum):
    max_type = "max"
    min_type = "min"


class Candle(BaseModel):
    since: datetime.datetime
    open: float
    close: float
    high: float
    low: float
    volume: float
    trades: int | None
    exchange: Exchange

    @validator("exchange", pre=True)
    def validate_exchange(cls, v):
        if isinstance(v, str):
            try:
                v = Exchange[v]
            except KeyError:
                raise ValidationError(f"{v} is not member of Exchange")
        assert v is not None
        return v

    class Config:
        orm_mode = True


class CandleOut(BaseModel):
    type: Type
    time: datetime.datetime
    open: float
    close: float
    high: float
    low: float
    volume: float
    exchange: Exchange

    @validator("exchange", pre=True)
    def validate_exchange(cls, v):
        if isinstance(v, str):
            try:
                v = Exchange[v]
            except KeyError:
                raise ValidationError(f"{v} is not member of Exchange")
        assert v is not None
        return v

    @validator("type", pre=True)
    def validate_maxmin_type(cls, v):
        if bool(v):
            return Type.min_type
        else:
            return Type.max_type
