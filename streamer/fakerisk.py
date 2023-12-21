import string
import time

from faker import Faker
from datetime import datetime, timedelta
import numpy as np
import polars as pl
import duckdb
import random
from bson import ObjectId

fake = Faker()


def generate_bond_data(num_records):
    data = []
    for _ in range(num_records):
        country_code = ''.join(random.choices(string.ascii_uppercase, k=2))

        # Generate a nine-character alphanumeric code
        alphanumeric_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=9))

        # For simplicity, use a random digit as the check digit (real ISINs use a complex calculation)
        check_digit = random.choice(string.digits)

        # Combine to form the ISIN
        isinId = country_code + alphanumeric_code + check_digit

        ccy = np.random.choice(
            ["USD", "EUR", "GBP", "JPY", "AUD", "CAD", "KRW", "TRY"],
            p=[0.5, 0.2, 0.1, 0.05, 0.05, 0.05, 0.025, 0.025],
        )
        rating = random.choice(
            [
                "AAA",
                "AA+",
                "AA",
                "AA-",
                "A+",
                "A",
                "A-",
                "BBB+",
                "BBB",
                "BBB-",
                "B+",
                "B",
                "B-",
                "CCC",
            ]
        )
        sector = fake.random_element(
            elements=(
                "Technology",
                "Finance",
                "Energy",
                "Healthcare",
                "Manufacturing",
                "Telecommunications",
                "Utilities",
                "Consumer Goods",
                "Transportation",
                "Pharmaceuticals",
            )
        )
        country = fake.country()
        issuer = fake.company()
        maturityDate = datetime.now() + timedelta(days=random.randint(365, 365 * 10))
        maturity = maturityDate.strftime("%Y-%m-%d")
        midPx = round(random.uniform(80, 120), 3)
        previousPx = round(midPx + random.normalvariate(0.1, 1.2), 3)
        zSpread = round(random.uniform(70, 500), 3)
        midYield = round(0.05 + zSpread / 10000, 3)
        dtm = (maturityDate.date() - datetime.today().date()).days
        duration = round(dtm / 365, 3)
        notional = round(random.uniform(-50e6, 100e6), 0)
        exposure = round(-1 * notional * duration / (1 + midYield) / 1e4)
        coupon = round(random.uniform(0.8, 1.5)*midYield, 2)
        securityName = issuer.split('-')[0].split(',')[0].split(' ')[0] + ' ' + str(coupon) + ' ' + str(maturityDate.year)
        data.append(
            {
                "isinId": isinId,
                "rating": rating,
                "sector": sector,
                "country": country,
                "issuer": issuer,
                "maturity": maturity,
                "currency": ccy,
                "midPx": midPx,
                "previousPx": previousPx,
                "zSpread": zSpread,
                "midYield": midYield,
                "dtm": dtm,
                "duration": duration,
                "countryCode": country_code,
                "notional": notional,
                "exposure": exposure,
                "coupon": coupon,
                "securityName": securityName,
            }
        )

    df = pl.DataFrame(data)
    df = df.with_columns(pl.col("isinId").cast(pl.Categorical))
    df = df.with_columns(pl.col("rating").cast(pl.Categorical))
    df = df.with_columns(pl.col("sector").cast(pl.Categorical))
    df = df.with_columns(pl.col("country").cast(pl.Categorical))
    df = df.with_columns(pl.col("issuer").cast(pl.Categorical))
    df = df.with_columns(pl.col("maturity").cast(pl.Date))
    df = df.with_columns(pl.col("currency").cast(pl.Categorical))
    df = df.with_columns(pl.col("countryCode").cast(pl.Categorical))

    return df


def generate_random_names(num_names):
    return [fake.name() for _ in range(num_names)]


def generate_book_structure(num_traders):
    """
    Generate a DataFrame of HMS books with random data.

    Args:
        num_traders (int): The number of traders for which to generate books.

    Returns:
        pandas.DataFrame: A DataFrame containing the generated HMS books data.
    """
    fake = Faker()
    books = {
        "balanceSheet": [
            np.random.choice(["HBEU", "HBUS", "HBAP", "HBFR"], p=[0.5, 0.2, 0.2, 0.1])
            for _ in range(num_traders)
        ],
        "desk": [
            np.random.choice(
                [
                    "Flow Credit",
                    "Structured Index",
                    "Flow Rates",
                    "Private Credit",
                    "Emerging Markets",
                ],
                p=[0.4, 0.1, 0.3, 0.1, 0.1],
            )
            for _ in range(num_traders)
        ],
        "trader": generate_random_names(num_traders),
        "book": [fake.bothify(text="GDM#####") for _ in range(num_traders)],
        "business": ["Global Debt Markets" for _ in range(num_traders)],
        "numberOfBooks": [random.randint(3, 10) for _ in range(num_traders)],
    }
    df = pl.DataFrame(books)
    secondaryTraders = {desk: fake.name() for desk in df.select("desk").unique()["desk"]}
    df = df.with_columns(
        pl.col("desk")
        .map_elements(lambda x: secondaryTraders[x])
        .alias("secondaryTrader")
    )

    df = df.select(
        pl.exclude("numberOfBooks").repeat_by(pl.col("numberOfBooks")).explode()
    ).with_columns(
        [
            (
                pl.col("book")
                .map_elements(lambda _: fake.bothify(text="GDM#####"))
                .alias("book")
            )
        ]
    )

    df = df.with_columns(pl.col("business").cast(pl.Categorical))
    df = df.with_columns(pl.col("desk").cast(pl.Categorical))
    df = df.with_columns(pl.col("trader").cast(pl.Categorical))
    df = df.with_columns(pl.col("book").cast(pl.Categorical))
    df = df.with_columns(pl.col("balanceSheet").cast(pl.Categorical))
    df = df.with_columns(pl.col("secondaryTrader").cast(pl.Categorical))
    return df

def update_risk_records(books, instruments, n_records, snapId='LIVE' + datetime.today().strftime('%Y%m%d')):
    books = books.sample(n_records, shuffle=True, with_replacement=True)
    instruments = instruments.sample(n_records, shuffle=True, with_replacement=True)
    df = pl.concat([books, instruments], how="horizontal")
    df = df.with_columns(pl.concat_str(df['book'], df['isinId']).alias('positionId'))
    df = df.with_columns(snapId + ':' + pl.col('positionId')).rename({'literal': 'id'})
    _now =datetime.now()
    df = df.with_columns(lastUpdatedAt = _now)
    df = df.with_columns(pl.Series("snapId",[snapId for _ in df.iter_rows()]))
    df = df.with_columns(pl.Series("objectId", [str(ObjectId.from_datetime(_now)) for _ in df.iter_rows()]))
    return df,snapId

def streamRisk():
    try:
        books = pl.read_parquet('../data/books.parquet')
    except:
        books = generate_book_structure(65)
        books.write_parquet('../data/books.parquet')
    try:
        instruments = pl.read_parquet('../data/instruments.parquet')
    except:
        instruments = generate_bond_data(50000)
        instruments.write_parquet('../data/instruments.parquet')

    riskdf,snapId = update_risk_records(books, instruments, random.uniform(5,150))
    filePath = '..//data//risk//'+snapId + datetime.now().strftime('%H%M%S')+'.parquet'
    riskdf.write_parquet(filePath)


if __name__ == "__main__":
    pl.Config(set_fmt_float="full")
    pl.Config.set_tbl_cols(14)
    while True:
        streamRisk()
        time.sleep(5)
        print('running')
