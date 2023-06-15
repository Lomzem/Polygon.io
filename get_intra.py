import datetime
import polars as pl
from polygon import RESTClient


MARKET_OPEN = datetime.time(6, 30)
MARKET_CLOSE = datetime.time(13, 0)


def get_polygon_data(api_key: str, date: datetime.date, ticker: str) -> pl.LazyFrame:
    client = RESTClient(api_key)
    df = pl.LazyFrame(
        client.get_aggs(
            ticker=ticker, multiplier=1, timespan="minute", from_=date, to=date
        )
    )
    return df


def format_df(df: pl.LazyFrame) -> pl.LazyFrame:
    df = (
        df.select(pl.exclude("otc"))
        .rename({"transactions": "trades"})
        .with_columns(
            timestamp=pl.col("timestamp")
            .cast(pl.Datetime(time_unit="ms"))
            .dt.offset_by("-7h")
        )
        .with_columns(
            date=pl.col("timestamp").cast(pl.Date),
            time=pl.col("timestamp").cast(pl.Time),
            open=pl.col("open").cast(pl.Float32),
            high=pl.col("high").cast(pl.Float32),
            low=pl.col("low").cast(pl.Float32),
            close=pl.col("close").cast(pl.Float32),
            vwap=pl.col("vwap").cast(pl.Float32),
            volume=pl.col("volume").cast(pl.Int32),
            trades=pl.col("trades").cast(pl.Int32),
        )
        .select(pl.exclude("timestamp"))
    )
    return df


def get_high_price(df: pl.LazyFrame) -> float:
    return df.select(pl.col("high").max()).collect().item()


def get_high_time(df: pl.LazyFrame) -> datetime.time:
    return (
        df.sort(["high", "time"], descending=[True, False])
        .first()
        .select(pl.col("time"))
        .collect()
        .item()
    )


def get_post_high(df: pl.LazyFrame, high_time) -> pl.LazyFrame:
    return df.filter(pl.col("time").gt(high_time))


def get_low_price(df: pl.LazyFrame) -> float:
    return df.select(pl.col("low").min()).collect().item()


def get_low_time(df: pl.LazyFrame) -> datetime.time:
    return (
        df.sort(["low", "time"], descending=[False, True])
        .first()
        .select(pl.col("time"))
        .collect()
        .item()
    )


def get_volume(df: pl.LazyFrame) -> int:
    return df.select(pl.col("volume").sum()).collect().item()


def get_important(df: pl.LazyFrame, ticker: str) -> pl.LazyFrame:
    date = df.select(pl.col("date")).first().collect().item()
    market_hours = df.filter(pl.col("time").is_between(MARKET_OPEN, MARKET_CLOSE))
    pm_hours = df.filter(pl.col("time").lt(MARKET_OPEN))

    open_price = (
        df.filter(pl.col("time") >= (MARKET_OPEN))
        .select(pl.col("open"))
        .first()
        .collect()
        .item()
    )
    close_price = (
        df.filter(pl.col("time") <= (MARKET_CLOSE))
        .select(pl.col("close"))
        .last()
        .collect()
        .item()
    )

    high_price = get_high_price(market_hours)
    high_time = get_high_time(market_hours)
    post_high = get_post_high(market_hours, high_time)

    low_price = get_low_price(market_hours)
    low_time = get_low_time(market_hours)
    lowph = get_low_price(post_high)
    lowph_time = get_low_time(post_high)

    pmvol = get_volume(pm_hours)
    volume = get_volume(market_hours) + pmvol

    pmhigh = get_high_price(pm_hours)
    pmhigh_time = get_high_time(pm_hours)
    post_pmhigh = get_post_high(pm_hours, pmhigh)

    pmlow = get_low_price(post_pmhigh)
    pmlow_time = get_low_time(post_pmhigh)

    imp_df = pl.LazyFrame(
        {
            "date": date,
            "ticker": ticker,
            "pmhigh": pmhigh,
            "pmhigh_time": pmhigh_time,
            "pmlow": pmlow,
            "pmlow_time": pmlow_time,
            "pmvol": pmvol,
            "open": open_price,
            "high": high_price,
            "high_time": high_time,
            "low": low_price,
            "low_time": low_time,
            "lowph": lowph,
            "lowph_time": lowph_time,
            "close": close_price,
            "volume": volume,
        },
        schema={
            "date": pl.Date,
            "ticker": str,
            "pmhigh": pl.Float32,
            "pmhigh_time": pl.Time,
            "pmlow": pl.Float32,
            "pmlow_time": pl.Time,
            "pmvol": pl.Int32,
            "open": pl.Float32,
            "high": pl.Float32,
            "high_time": pl.Time,
            "low": pl.Float32,
            "low_time": pl.Time,
            "lowph": pl.Float32,
            "lowph_time": pl.Time,
            "close": pl.Float32,
            "volume": pl.Int32,
        },
    )
    return imp_df
