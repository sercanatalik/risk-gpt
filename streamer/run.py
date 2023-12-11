from faker import Faker
import random
from datetime import datetime, timedelta
import faker
import numpy as np
import polars as pl
import pyarrow as pa

fake = Faker()


def generate_bond_data(num_records):
    data = []
    for _ in range(num_records):
        isin = fake.isbn10()
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
        maturity = (
            datetime.now() + timedelta(days=random.randint(365, 365 * 10))
        ).strftime("%Y-%m-%d")
        midPx = round(random.uniform(80, 120), 3)
        previosPx = round(midPx + random.normalvariate(0.1, 1.2), 3)
        zspread = round(random.uniform(70, 500), 3)
        midYield = 0.05 + zspread / 10000

        data.append(
            {
                "ISIN": isin,
                "Rating": rating,
                "Sector": sector,
                "Country": country,
                "Issuer": issuer,
                "Maturity": maturity,
                "Currency": ccy,
                "MidPx": midPx,
                "previosPx": previosPx,
                "zspread": zspread,
                "midYield": midYield,
            }
        )

    df = pl.DataFrame(data)
    df = df.with_columns(pl.col("ISIN").cast(pl.Categorical))
    df = df.with_columns(pl.col("Rating").cast(pl.Categorical))
    df = df.with_columns(pl.col("Sector").cast(pl.Categorical))
    df = df.with_columns(pl.col("Country").cast(pl.Categorical))
    df = df.with_columns(pl.col("Issuer").cast(pl.Categorical))
    df = df.with_columns(pl.col("Maturity").cast(pl.Date))
    df = df.with_columns(pl.col("Currency").cast(pl.Categorical))

    return df


def generate_random_names(num_names):
    names = []
    for _ in range(num_names):
        names.append(fake.name())
    return names


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
    desks = df.select("desk").unique().to_dict()["desk"]
    secondaryTraders = {}
    for i in desks:
        secondaryTraders[i] = fake.name()

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


def generate_risk_records(n_row):
    books = generate_book_structure(65)
    books = books.sample(n_row, shuffle=True, with_replacement=True)

    instruments = generate_bond_data(40000)
    instruments = instruments.sample(n_row, shuffle=True, with_replacement=True)

    # concat the books and instruments dataframes
    df = pl.concat([books, instruments], how="horizontal")

    # generate a column name called daysToMatuiry which calculates difference between maturity column and today's date
    df = df.with_columns(
        [
            pl.col("Maturity")
            .map_elements(lambda x: (x - datetime.today().date()).days)
            .alias("daysToMaturity")
        ]
    )

    df = df.with_columns(
        [
            pl.col("daysToMaturity").map_elements(lambda x: x / 365).alias("duration"),
            pl.col("daysToMaturity")
            .map_elements(lambda x: round(random.uniform(-100e3, 150e3) / x) * 1e6)
            .alias("notional"),
            pl.col("daysToMaturity").abs().alias("exposure"),
        ]
    )
    df = df.with_columns('exposure',df['notional']*df['duration'])
    
    



    return df


if __name__ == "__main__":
    pl.Config(set_fmt_float="full")
    df = generate_risk_records(10)
    print(df.columns)
    print(df)
