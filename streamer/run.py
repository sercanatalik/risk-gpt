from faker import Faker
import random
from datetime import datetime, timedelta
import faker
import numpy as np

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
        data.append(
            {
                "ISIN": isin,
                "Rating": rating,
                "Sector": sector,
                "Country": country,
                "Issuer": issuer,
                "Maturity": maturity,
                "Currency": ccy,
            }
        )
    return data


def generate_random_names(num_names):
    names = []
    for _ in range(num_names):
        names.append(fake.name())
    return names


def generate_book(num_books):
    # 'Flow Credit', 'Structured Credit', 'High Yield', 'Investment Grade', 'Emerging Markets'
    books = {
        "balanceSheet": [
            np.random.choice(["HBEU", "HBUS", "HBAP", "HBFR"], p=[0.5, 0.2, 0.2, 0.1])
            for _ in range(num_books)
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
            for _ in range(num_books)
        ],
        "trader": generate_random_names(num_books),
        "book": [fake.bothify(text="GDM#####") for _ in range(num_books)],
        "business": ["Global Debt Markets" for _ in range(num_books)],
    }
    return books


def generate_risk_data(num_records):
    data = []
    instruments = generate_bond_data(20000)

    for _ in range(num_records):
        balanceSheet = np.random.choice(
            ["HBEU", "HBUS", "HBAP", "HBFR"], p=[0.5, 0.2, 0.2, 0.1]
        )
        desk = np.random.choice(
            ["Flow Credit", "HBUS", "HBAP", "HBFR"], p=[0.5, 0.2, 0.2, 0.1]
        )


print(generate_book(10))
